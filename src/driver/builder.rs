use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    common::{SchedulableTask, SchedulerConfig, utils::get_hostname},
    driver::{DriverContext, DriverMetrics, DriverPlugin, TaskDriver, plugins::MetricsPlugin},
    persistence::{TaskQueue, TaskStore, memory::MemoryPersistence},
    policy::{WaitStrategy, expbackoff::ExponentialBackoff},
};

/// 任务驱动构造器 (Builder Pattern)
/// - `T`: 任务 Payload 类型
pub struct TaskDriverBuilder<T> {
    /// 节点ID名
    node_id: Option<String>,
    /// 全局配置
    config: Option<SchedulerConfig>,
    /// 等待策略
    wait_strategy: Option<Arc<dyn WaitStrategy>>,
    /// 插件列表
    plugins: Vec<Box<dyn DriverPlugin<T>>>,
    /// 持久化 - 存储层 (Option 用于处理默认值逻辑)
    store: Option<Arc<dyn TaskStore<T>>>,
    /// 持久化 - 队列层
    queue: Option<Arc<dyn TaskQueue>>,
    /// 全局统计指标
    metrics: Arc<DriverMetrics>,

    shutdown: Option<CancellationToken>,
}

impl<T> Default for TaskDriverBuilder<T>
where
    T: Send + Sync + Clone + 'static,
{
    /// 创建一个新的构造器
    ///
    /// **默认行为**:
    /// - Config: Default
    /// - WaitStrategy: ExponentialBackoff (指数退避)
    /// - Persistence: None (默认初始化为 MemoryPersistence)
    /// - Metrics: 0 (全新计数器)
    fn default() -> Self {
        Self {
            node_id: None,
            config: None,
            wait_strategy: None,
            plugins: Vec::new(),
            store: None,
            queue: None,
            shutdown: None,
            metrics: Arc::new(DriverMetrics::default()),
        }
    }
}

impl<T> TaskDriverBuilder<T>
where
    T: Send + Sync + Clone + 'static,
{
    pub fn new(
        node_id: Option<String>,
        config: Option<SchedulerConfig>,
        store: Option<Arc<dyn TaskStore<T>>>,
        queue: Option<Arc<dyn TaskQueue>>,
        plugins: Vec<Box<dyn DriverPlugin<T>>>,
        wait_strategy: Option<Arc<dyn WaitStrategy>>,
        shutdown: Option<CancellationToken>,
    ) -> Self {
        Self {
            node_id,
            config,
            store,
            queue,
            plugins,
            wait_strategy,
            shutdown,
            metrics: Arc::new(DriverMetrics::default()),
        }
    }
}

// ==========================================
// 2. 通用方法 (适用于任何 P)
// ==========================================
impl<T> TaskDriverBuilder<T>
where
    T: Send + Sync + Clone + 'static,
{
    /// 设置节点 ID (如果不设，自动生成)
    pub fn with_node_id(mut self, id: String) -> Self {
        self.node_id = Some(id);
        self
    }

    /// 设置调度器配置
    pub fn with_config(mut self, config: SchedulerConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// 设置等待策略
    pub fn with_wait_strategy<S>(mut self, strategy: S) -> Self
    where
        S: WaitStrategy + 'static,
    {
        self.wait_strategy = Some(Arc::new(strategy));
        self
    }

    /// 添加插件 (支持链式调用)
    ///
    /// **注意**: 插件的执行顺序与添加顺序一致 (FIFO)。
    pub fn with_plugin<PL>(mut self, plugin: PL) -> Self
    where
        PL: DriverPlugin<T> + 'static,
    {
        self.plugins.push(Box::new(plugin));
        self
    }
    /// 设置持久化层
    ///
    /// **设计说明**:
    /// - 这里接收一个实现了 `TaskQueue + TaskStore` 的具体类型 `P`。
    /// - 然后将其包装为 `Arc` 并分别强转为 `dyn TaskStore` 和 `dyn TaskQueue`。
    ///
    /// **TODO / 潜在 Bug**:
    /// - 如果传入的 `persistence` 对象内部使用了 `RefCell` 等非线程安全组件，
    /// - 这里虽然能编译通过 (因为 P: Send+Sync)，但在运行时可能会有不可预期的行为。
    /// - 不过对于标准的 Redis/Memory 实现，这是安全的。
    pub fn with_persistence<P>(mut self, persistence: P) -> Self
    where
        P: TaskQueue + TaskStore<T> + Send + Sync + 'static,
    {
        let arc = Arc::new(persistence);
        self.store = Some(arc.clone()); // 引用计数 +1，转换为 dyn Store
        self.queue = Some(arc); // 引用计数 +1，转换为 dyn Queue
        self
    }

    /// 设置任务队列
    pub fn with_queue<Q>(mut self, queue: Q) -> Self
    where
        Q: TaskQueue + 'static,
    {
        self.queue = Some(Arc::new(queue));
        self
    }

    /// 设置持久化存储
    pub fn with_store<S>(mut self, store: S) -> Self
    where
        S: TaskStore<T> + 'static,
    {
        self.store = Some(Arc::new(store));
        self
    }
    /// 构建驱动器
    ///
    /// * 返回 TaskDriver
    pub fn build<Task>(mut self, task_handler: Task) -> TaskDriver<Task, T>
    where
        Task: SchedulableTask<T> + Send + Sync + 'static,
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let node_id = self.node_id.unwrap_or_else(|| get_hostname());
        let config = self.config.unwrap_or_default();

        // 处理持久化层的默认逻辑
        let (store, queue) = if let (Some(s), Some(q)) = (self.store, self.queue) {
            (s, q)
        } else {
            debug!("Driver Build load Using default MemoryPersistence");
            let mem = Arc::new(MemoryPersistence::new(config.clone()));
            (
                mem.clone() as Arc<dyn TaskStore<T>>,
                mem as Arc<dyn TaskQueue>,
            )
        };
        // 默认等待策略：指数退避 (min=10ms, max=1s)
        let wait_strategy = self
            .wait_strategy
            .unwrap_or_else(|| Arc::new(ExponentialBackoff::new(10, 1000)));

        // 自动注入 MetricsPlugin
        let metrics_plugin = Box::new(MetricsPlugin::new(self.metrics.clone()));
        self.plugins.insert(0, metrics_plugin);

        // 初始化全局停机 Token
        let token = self.shutdown.unwrap_or_else(|| CancellationToken::new());

        // 创建上下文
        let ctx = DriverContext::new(node_id, store, queue, config, token);
        // 组装最终的 Driver
        TaskDriver::new_with_components(ctx, task_handler, self.plugins, wait_strategy)
    }
}
