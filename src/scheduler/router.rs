use anyhow::anyhow;
use std::{marker::PhantomData, sync::Arc};

use ahash::HashMap;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{
    common::{SchedulableTask, TaskContext},
    scheduler::JobContext,
};
// =========================================================
// 解码器策略 (The Strategy)
// =========================================================

/// 定义如何将字节数组解析为具体参数 Args
pub trait PayloadDecoder<Args>: Send + Sync + 'static + Clone {
    fn decode(&self, body: &[u8]) -> anyhow::Result<Args>;
}

/// 策略 A: JSON 解码
pub struct JsonDecoder<T>(PhantomData<T>);
impl<T> Clone for JsonDecoder<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T: DeserializeOwned + Send + Sync + 'static> PayloadDecoder<T> for JsonDecoder<T> {
    fn decode(&self, body: &[u8]) -> anyhow::Result<T> {
        serde_json::from_slice(body).map_err(|e| anyhow::anyhow!("JSON decode failed: {}", e))
    }
}
/// 策略 B: 原始字节 (透传)
#[derive(Clone)]
pub struct BytesDecoder;

impl PayloadDecoder<Vec<u8>> for BytesDecoder {
    fn decode(&self, body: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(body.to_vec())
    }
}

// =========================================================
// 通用包装器 (The Generic Wrapper)
// =========================================================
/// 一个通用的 Handler 包装器
/// 它不知道具体的参数类型，全靠 Decoder 告诉它怎么做。
struct HandlerWrapper<F, D, Args> {
    func: F,
    decoder: D,
    _phantom: PhantomData<Args>,
}

// 手动实现 Clone，避免对 Args 的 Clone 约束
impl<F: Clone, D: Clone, Args> Clone for HandlerWrapper<F, D, Args> {
    fn clone(&self) -> Self {
        Self {
            func: self.func.clone(),
            decoder: self.decoder.clone(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, D, Args> ErasedByteHandler for HandlerWrapper<F, D, Args>
where
    F: Fn(JobContext, Args) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    D: PayloadDecoder<Args>, // <--- 关键：依赖解码器策略
    Args: Send + Sync + 'static,
{
    async fn handle(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()> {
        // 1. 委托给 Decoder 进行解码
        let args = self.decoder.decode(&ctx.data.payload)?;

        // 2. 包装 Context
        let job_ctx = JobContext::new(ctx);

        // 3. 执行用户逻辑
        (self.func)(job_ctx, args).await
    }

    fn box_clone(&self) -> Box<dyn ErasedByteHandler> {
        Box::new(self.clone())
    }
}

// =========================================================
// 内部 Handler Trait (类型擦除)
// =========================================================
#[async_trait]
pub trait ErasedByteHandler: Send + Sync {
    async fn handle(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()>;
    fn box_clone(&self) -> Box<dyn ErasedByteHandler>;
}
impl Clone for Box<dyn ErasedByteHandler> {
    fn clone(&self) -> Box<dyn ErasedByteHandler> {
        self.box_clone()
    }
}

// =========================================================
// TaskRouter (API 入口)
// =========================================================

#[derive(Clone, Default)]
pub struct TaskRouter {
    routes: Arc<HashMap<String, Box<dyn ErasedByteHandler>>>,
}

impl TaskRouter {
    pub fn new() -> Self {
        Self::default()
    }
    /// 注册 JSON 处理函数
    pub fn register<F, Fut, Args>(self, name: &str, handler: F) -> Self
    where
        F: Fn(JobContext, Args) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
        Args: DeserializeOwned + Send + Sync + 'static,
    {
        // 使用 JsonDecoder
        self.insert(
            name,
            Box::new(HandlerWrapper {
                func: handler,
                decoder: JsonDecoder(PhantomData),
                _phantom: PhantomData,
            }),
        )
    }
    /// 注册原始字节处理函数
    /// 这里的 handler 参数必须是 Vec<u8>
    pub fn register_bytes<F, Fut>(self, name: &str, handler: F) -> Self
    where
        F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        // 使用 BytesDecoder
        self.insert(
            name,
            Box::new(HandlerWrapper {
                func: handler,
                decoder: BytesDecoder,
                _phantom: PhantomData,
            }),
        )
    }

    // 私有方法
    fn insert(mut self, name: &str, handler: Box<dyn ErasedByteHandler>) -> Self {
        let mut routes = Arc::try_unwrap(self.routes).unwrap_or_else(|r| (*r).clone());
        routes.insert(name.to_string(), handler);
        self.routes = Arc::new(routes);
        self
    }
}

// 核心：实现 SchedulableTask
#[async_trait]
impl SchedulableTask<Vec<u8>> for TaskRouter {
    async fn execute(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()> {
        let name = &ctx.data.name;
        if let Some(handler) = self.routes.get(name) {
            handler.handle(ctx).await
        } else {
            Err(anyhow!("Unknown task type: {}", name))
        }
    }
}
