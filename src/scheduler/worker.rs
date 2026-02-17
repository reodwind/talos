use std::sync::Arc;

use crate::{
    common::SchedulableTask, driver::TaskDriver, persistence::TaskStore,
    scheduler::builder::WorkerBuilder,
};

/// 消费者工作节点 (The Public Face)
///
/// 这是用户直接交互的对象。它封装了底层的 `TaskDriver`，
/// 屏蔽了 Context 创建、泛型组装等复杂细节。
///
/// # 泛型说明
/// - `P`: 持久化层实现 (例如 RedisPersistence)
/// - `H`: 具体的任务处理逻辑 (用户写的 Struct)
/// - `T`: 任务载荷数据类型 (例如 String 或 JSON)
pub struct Worker<H, T> {
    /// 内部核心驱动器 (引擎)
    pub(crate) driver: TaskDriver<H, T>,
}

impl<H, T> Worker<H, T>
where
    H: SchedulableTask<T> + Send + Sync + 'static,
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    /// 创建一个 Worker Builder
    /// - 默认泛型 T 为 Vec<u8>，适配 TaskRouter 模式
    pub fn builder() -> WorkerBuilder<Vec<u8>> {
        WorkerBuilder::<Vec<u8>>::new()
    }
    /// [入口] 启动 Worker
    ///
    /// 这是一个异步阻塞方法。它会启动 Driver 的主循环。
    /// 通常你应该在 `tokio::spawn` 中调用它。
    pub async fn start(self) {
        // 委托给核心驱动器启动
        self.driver.start().await;
    }

    /// 获取内部 Store
    pub fn store(&self) -> Arc<dyn TaskStore<T>> {
        self.driver.store()
    }
}
