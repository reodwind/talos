use std::{marker::PhantomData, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::{
    common::{Extensions, SchedulableTask, SchedulerConfig},
    driver::{DriverPlugin, TaskDriverBuilder},
    persistence::{TaskQueue, TaskStore},
    policy::WaitStrategy,
    scheduler::Worker,
};

/// Worker 构建器
///
/// 用于一步步配置并生成 Worker 实例。
pub struct WorkerBuilder<T> {
    /// 必填: 全局配置
    config: Option<SchedulerConfig>,
    /// 任务存储 (必须)
    store: Option<Arc<dyn TaskStore<T>>>,

    /// 任务队列 (必须)
    queue: Option<Arc<dyn TaskQueue>>,

    /// 选填: 自定义插件链路
    plugins: Vec<Box<dyn DriverPlugin<T>>>,
    /// 等待策略 (用于 Driver)
    wait_strategy: Option<Arc<dyn WaitStrategy>>,
    /// 选填: 节点 ID (默认自动生成)
    node_id: Option<String>,
    /// 选填: 扩展容器 (可选注入，用于 Workflow ID, Trace ID 等)
    extensions: Extensions,
    /// 全局停机信号 (可选注入，用于多组件协同)
    shutdown_token: Option<CancellationToken>,
    // 占位符，用于泛型推导
    _marker: std::marker::PhantomData<T>,
}

impl<T> WorkerBuilder<T>
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    /// 创建一个新的构建器
    ///
    /// # 参数
    /// - `config`: 全局配置
    /// - `persistence`: 已经初始化好的存储层 (如 RedisClient)
    /// - `handler`: 你的业务逻辑实现
    pub fn new() -> Self {
        Self {
            node_id: None,
            config: None,
            store: None,
            queue: None,
            plugins: Vec::new(),
            wait_strategy: None,
            extensions: Extensions::new(),
            shutdown_token: None,
            _marker: PhantomData,
        }
    }

    /// [可选] 设置节点 ID
    /// 用于分布式锁和日志追踪。如果不填，默认生成 UUID。
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }

    /// [可选] 注入自定义插件
    /// 例如：限流器、Prometheus 监控、日志记录器
    pub fn with_plugin<PL>(mut self, plugin: PL) -> Self
    where
        PL: DriverPlugin<T> + 'static,
    {
        self.plugins.push(Box::new(plugin));
        self
    }

    /// 链式添加策略：可以添加多个，如退避策略 + 维护时间窗策略
    pub fn with_strategy<S: WaitStrategy>(mut self, strategy: S) -> Self {
        self.wait_strategy = Some(Arc::new(strategy));
        self
    }

    /// [可选] 依赖注入接口扩展
    pub fn extension<E: Send + Sync + 'static>(mut self, val: E) -> Self {
        self.extensions
            .insert(std::any::TypeId::of::<E>(), Box::new(val));
        self
    }

    /// [核心] 构建 Worker
    ///
    /// 这里完成了所有组件的组装工作：
    /// Config + Persistence -> Context -> Driver -> Worker
    pub fn build<Task>(self, task: Task) -> Worker<Task, T>
    where
        Task: SchedulableTask<T> + Send + Sync + 'static,
    {
        let driver_builder = TaskDriverBuilder::new(
            self.node_id,
            self.config,
            self.store,
            self.queue,
            self.plugins,
            self.wait_strategy,
            self.shutdown_token,
        )
        .with_extensions(self.extensions);
        // 组装 Driver (核心引擎)
        let driver = driver_builder.build(task);

        // 返回包装好的 Worker
        Worker { driver }
    }
}
