use crate::{
    common::config::SchedulerConfig,
    driver::DriverMetrics,
    persistence::{TaskQueue, TaskStore},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// 驱动器上下文 (Driver Context)
///
/// **职责**:
/// 这是一个“单例”性质的结构体（在每个节点中只有一个）。
/// 它打包了 Driver 运行所需的所有全局资源，避免在函数调用时传递一长串参数。
///
/// **泛型**:
/// - `T`: 任务接口数据
pub struct DriverContext<T> {
    // --- 身份标识 (Identity) ---
    /// 当前节点的唯一 ID (Worker ID)
    ///
    /// **作用**:
    /// 1. **抢占任务**: 当调用 `acquire` 时，这个 ID 会被写入任务的 `worker_id` 字段。
    /// 2. **脑裂防御**: 只有持有此 ID 的节点才能对任务进行心跳续约。
    /// 3. **问题排查**: 在数据库中可以看到任务正运行在哪台机器上。
    pub node_id: String,

    // --- 核心组件 (Components) ---
    /// 数据存储层 (负责 Save, Load, Update, Remove)
    pub store: Arc<dyn TaskStore<T>>,

    /// 任务队列层 (负责 Acquire, Heartbeat)
    pub queue: Arc<dyn TaskQueue>,

    /// 全局配置
    /// - 包含最大并发数、轮询间隔、心跳间隔等参数。
    pub config: Arc<SchedulerConfig>,

    ///全局统计指标
    pub metrics: Arc<DriverMetrics>,

    // --- 信号与控制 (Signals & Control) ---
    /// 关机信号
    pub shutdown: CancellationToken,
}

impl<T> Clone for DriverContext<T> {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            store: self.store.clone(),
            queue: self.queue.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<T> DriverContext<T> {
    /// 检查是否收到停机信号
    ///
    /// 用于在循环中快速判断是否应该退出。
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }
}
impl<T> DriverContext<T>
where
    T: Send + Sync + Clone + 'static,
{
    /// 创建一个新的驱动器上下文
    pub fn new(
        node_id: String,
        store: Arc<dyn TaskStore<T>>,
        queue: Arc<dyn TaskQueue>,
        config: SchedulerConfig,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            node_id,
            store,
            queue,
            config: Arc::new(config),
            metrics: Arc::new(DriverMetrics::default()),
            shutdown: shutdown_token,
        }
    }
}
