use anyhow::anyhow;
use std::{marker::PhantomData, sync::Arc};

use ahash::HashMap;
use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{
    common::{SchedulableTask, TaskContext},
    scheduler::JobContext,
};

#[async_trait]
pub trait ErasedHandler: Send + Sync {
    async fn handle(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()>;
    fn box_clone(&self) -> Box<dyn ErasedHandler>;
}

impl Clone for Box<dyn ErasedHandler> {
    fn clone(&self) -> Box<dyn ErasedHandler> {
        self.box_clone()
    }
}

/// --- 任务路由器 ---
#[derive(Clone, Default)]
pub struct TaskRouter {
    // 存储类型擦除后的 Handler
    routes: Arc<HashMap<String, Box<dyn ErasedHandler>>>,
}
impl TaskRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册 JSON 任务
    pub fn register<F, Fut, Args>(self, name: &str, handler: F) -> Self
    where
        F: Fn(JobContext, Args) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
        Args: DeserializeOwned + Send + Sync + 'static,
    {
        self.insert(
            name,
            Box::new(JsonWrapper {
                func: handler,
                _phantom: PhantomData,
            }),
        )
    }

    pub fn register_raw<F, Fut>(self, name: &str, handler: F) -> Self
    where
        F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.insert(name, Box::new(BytesWrapper { func: handler }))
    }

    fn insert(mut self, name: &str, handler: Box<dyn ErasedHandler>) -> Self {
        let mut routes = Arc::try_unwrap(self.routes).unwrap_or_else(|r| (*r).clone());
        routes.insert(name.to_string(), handler);
        self.routes = Arc::new(routes);
        self
    }
}

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

// =========================================================
// 内部适配器 (JSON)
// =========================================================

struct JsonWrapper<F, Args> {
    func: F,
    _phantom: PhantomData<Args>,
}

impl<F, Args> Clone for JsonWrapper<F, Args>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            func: self.func.clone(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, Args> ErasedHandler for JsonWrapper<F, Args>
where
    F: Fn(JobContext, Args) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    Args: DeserializeOwned + Send + Sync + 'static,
{
    async fn handle(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()> {
        // 1. 反序列化
        let args: Args = serde_json::from_slice(&ctx.data.payload)
            .map_err(|e| anyhow::anyhow!("JSON deserialize error: {}", e))?;
        // 2. 包装 Context
        let job_ctx = JobContext::new(ctx);
        // 3. 执行
        (self.func)(job_ctx, args).await
    }

    // ✨ 核心修复：实现 box_clone
    fn box_clone(&self) -> Box<dyn ErasedHandler> {
        Box::new(self.clone())
    }
}

// --- Bytes Wrapper (原始) ---

#[derive(Clone)]
struct BytesWrapper<F> {
    func: F,
}
#[async_trait]
impl<F, Fut> ErasedHandler for BytesWrapper<F>
where
    F: Fn(JobContext, Vec<u8>) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    async fn handle(&self, ctx: TaskContext<Vec<u8>>) -> anyhow::Result<()> {
        let bytes = ctx.data.payload.clone();
        (self.func)(JobContext::new(ctx), bytes.to_vec()).await
    }
    fn box_clone(&self) -> Box<dyn ErasedHandler> {
        Box::new(self.clone())
    }
}
