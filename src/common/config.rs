use serde::{Deserialize, Serialize};

/// 执行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// 工作池模式 (默认)
    ///
    /// - 机制: Worker 线程会阻塞等待任务完成 (`await` 任务本身)。
    /// - 并发限制: 受 `workers` 数量的硬限制。
    /// - 适用场景: CPU 密集型任务，或需要严格控制并发线程数的场景。
    WorkerPool,

    /// 异步分发模式
    ///
    /// - 机制: Worker 线程只负责 `spawn` 任务，不等待完成。
    /// - 并发限制: 受 `max_concurrency` 信号量限制，不受线程数限制。
    /// - 适用场景: 高并发 IO 密集型任务 (如大量 HTTP 请求、数据库写入)。
    AsyncDispatch,
}
/// 回调通知模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackMode {
    /// 同步回调 (默认)
    /// 任务完成后立即在当前 Worker 线程执行回调。
    Sync,
    /// 异步回调
    /// 将回调作为新任务提交回队列（适合回调逻辑很重的情况）。
    Async,
    /// 无回调
    None,
}
// ==========================================
// 1. 资源配置 (WorkerConfig)
// ==========================================
/// 单机资源与并发控制配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// 工作线程数
    ///
    /// - 说明: 核心执行线程的数量。在 `WorkerPool` 模式下直接决定并发度。
    /// - 默认值: 系统逻辑核心数 (`num_cpus::get()`)
    /// - 建议: CPU 密集型设为 `核心数 + 1`；IO 密集型可适当调大 (如 `核心数 * 2`)。
    pub workers: usize,

    /// 内存任务队列容量
    ///
    /// - 说明: 内存中等待被 Worker 领取的任务数量上限。
    /// - 默认值: 1024
    /// - 影响: 设置过大会占用过多内存；设置过小可能导致 Redis 拉取频繁阻塞。
    pub queue_capacity: usize,

    /// 全局最大并发数 (仅 AsyncDispatch 模式有效)
    ///
    /// - 说明: 限制整个节点同时处于 Running 状态的任务上下文数量。
    /// - 默认值: 1000
    /// - 警告: 如果任务 Payload 很大 (e.g. 1MB)，请调低此值以防 OOM。
    pub max_concurrency: usize,

    /// 轮询间隔 (秒)
    ///
    /// - 说明: 当 Pub/Sub 通知静默（无新任务）时，Driver 主动去 Redis 检查任务的间隔。
    /// - 默认值: 5
    /// - 影响: 这是一个兜底机制。设得太短会导致空转浪费资源；设得太长会降低极端情况下的任务实时性。
    pub poll_interval_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            workers: num_cpus::get(), // [智能默认] 自动获取核数
            queue_capacity: 1024,
            max_concurrency: 1000,
            poll_interval_secs: 5,
        }
    }
}

// ==========================================
// 2. 集群配置 (ClusterConfig)
// ==========================================

/// 分布式与集群相关配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// 节点唯一标识 (Node ID)
    ///
    /// - 说明: 用于抢占任务锁、心跳注册。必须全局唯一。
    /// - 默认值: `hostname-随机串` (如 `server01-a1b2c`)
    /// - 建议: 在 K8s 环境中可注入 Pod Name。
    pub node_id: String,

    /// 心跳间隔 (毫秒)
    ///
    /// - 说明: Worker 向 Redis 汇报存活的频率。
    /// - 默认值: 5000 ms (5秒)
    /// - 建议: 必须小于 `PolicyConfig.zombie_check_interval_ms`。
    pub heartbeat_interval_ms: u64,

    /// 任务抢占批次大小
    ///
    /// - 说明: 每次去 Redis 拉取任务的数量。
    /// - 默认值: 50
    /// - 建议: 根据平均任务耗时调整。任务快则调大，任务慢则调小。
    pub acquire_batch_size: usize,

    /// Redis 连接池大小
    ///
    /// - 说明: 维持的 Redis 长连接数量。
    /// - 默认值: 核心数 * 2
    #[cfg(feature = "distributed")]
    pub redis_pool_size: usize,

    /// Pub/Sub 通知频道后缀
    ///
    /// - 说明: 监听新任务的频道名。完整频道为 `{namespace}:notify`。
    /// - 默认值: "notify"
    pub pubsub_channel: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        // [智能默认] 尝试获取主机名，失败则用 "node"
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "node".to_string());

        // 生成短随机 ID
        let random_suffix = nanoid::nanoid!(5, &nanoid::alphabet::SAFE);

        Self {
            node_id: format!("{}-{}", hostname, random_suffix),
            heartbeat_interval_ms: 5000,
            acquire_batch_size: 50,
            #[cfg(feature = "distributed")]
            redis_pool_size: num_cpus::get() * 2,
            pubsub_channel: "notify".to_string(),
        }
    }
}

// ==========================================
// 3. 策略配置 (PolicyConfig)
// ==========================================

/// 调度策略与容错配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// 执行模式
    pub execution_mode: ExecutionMode,
    /// 回调模式
    pub callback_mode: CallbackMode,

    /// 僵尸任务检测阈值 (毫秒)
    ///
    /// - 说明: Worker 心跳超过这个时间未更新，视为僵尸任务。
    /// - 默认值: 30,000 ms (30秒)
    /// - 建议: 设为 `heartbeat_interval_ms` 的 3~5 倍。
    pub zombie_check_interval_ms: u64,
    /// 僵尸任务判定阈值 (毫秒)
    ///
    /// - 说明: 任务从 Running 状态开始，如果超过这个时间还未完成，视为僵尸任务。
    /// - 默认值: 60,000 ms (1分钟)
    /// - 建议: 根据任务的平均执行时间调整，确保正常任务不会被误判为僵尸任务。
    pub zombie_threshold_ms: u64,
    /// 每次回收僵尸任务的最大数量
    ///
    /// - 默认: 50
    /// - 建议: 根据 Redis 负载调整，避免单次操作阻塞太久。
    pub rescue_batch_size: usize,

    /// 僵尸任务最大抢占次数 (熔断阈值)
    ///
    /// - 说明: 一个任务因超时被抢救(Reschedule)的最大次数。超过此值将被移入死信队列(DLQ)。
    /// - 默认值: 3 (防止毒丸任务无限搞挂节点)
    pub max_zombie_retries: u32,

    /// 提交任务超时 (毫秒)
    pub submit_timeout_ms: u64,

    /// 优雅停机超时 (秒)
    ///
    /// - 说明: 收到 SIGTERM 后等待任务完成的最大时间。
    pub shutdown_timeout_secs: u64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::WorkerPool,
            callback_mode: CallbackMode::Sync,
            zombie_check_interval_ms: 30_000,
            zombie_threshold_ms: 60_000,
            rescue_batch_size: 50,
            max_zombie_retries: 3,
            submit_timeout_ms: 2000,
            shutdown_timeout_secs: 30,
        }
    }
}

// ==========================================
// 4. 总配置入口 (SchedulerConfig)
// ==========================================

/// 调度器总配置
///
/// 使用分层结构组织配置项。支持 `serde` 序列化，可直接从 YAML/JSON 加载。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// 资源与并发
    #[serde(default)]
    pub worker: WorkerConfig,

    /// 集群与网络
    #[serde(default)]
    pub cluster: ClusterConfig,

    /// 策略与行为
    #[serde(default)]
    pub policy: PolicyConfig,

    /// 命名空间 (用于 Redis Key 前缀)
    /// 默认: "talos"
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "talos".to_string()
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            worker: WorkerConfig::default(),
            cluster: ClusterConfig::default(),
            policy: PolicyConfig::default(),
            namespace: default_namespace(),
        }
    }
}

impl SchedulerConfig {
    /// 快速创建一个开发环境配置
    pub fn new_dev() -> Self {
        let mut cfg = Self::default();
        // 开发环境下心跳快一点，方便调试
        cfg.cluster.heartbeat_interval_ms = 1000;
        cfg.policy.zombie_check_interval_ms = 5000;
        cfg
    }
}
