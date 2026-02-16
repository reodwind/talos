use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

/// 调度器统一结果类型
///
/// 使用此别名可以简化函数签名：`fn do_something() -> Result<()>`
pub type Result<T> = std::result::Result<T, SchedulerError>;

#[derive(Error, Debug)]
pub enum SchedulerError {
    // ==========================================
    // 1. 基础配置与启动错误 (Configuration & Startup)
    // ==========================================
    /// 配置错误
    ///
    /// - 触发场景: 启动时解析 YAML 失败、参数校验不通过、或者逻辑上互斥的配置项被同时启用。
    /// - 后果: 系统启动失败 (Panic)。
    /// - 处理: 检查配置文件或环境变量。
    #[error("Configuration error: {0}")]
    Config(String),

    /// 调度器已停机
    ///
    /// - 触发场景: 在调用 `shutdown()` 之后，或者系统收到 SIGTERM 信号期间，仍有客户端尝试提交新任务。
    /// - 后果: 请求被拒绝。
    /// - 处理: 客户端应停止提交，或等待系统重启。
    #[error("Scheduler is stopping or paused, rejecting new tasks.")]
    SchedulerShutdown,

    /// 线程池耗尽 (仅 WorkerPool 模式)
    ///
    /// - 触发场景: 所有 Worker 线程都在忙于执行任务，且队列已满。
    /// - 后果: 任务被积压或拒绝。
    #[error("Worker pool is exhausted (active threads: {0}).")]
    WorkerPoolExhausted(usize),

    // ==========================================
    // 2. 队列与资源错误 (Queue & Resource)
    // ==========================================
    /// 队列已满 (背压保护)
    ///
    /// - 触发场景: 内存队列 (`mpsc::channel`) 达到 `queue_capacity` 上限。
    /// - 后果: 触发背压 (Backpressure)，拒绝新任务以保护内存不被撑爆。
    /// - 处理: 客户端应减缓提交速度，或增加 `queue_capacity` 配置。
    #[error("Job queue is full (capacity: {0}). Backpressure triggered.")]
    QueueFull(usize),

    /// 内部通信通道已关闭
    ///
    /// - 触发场景: 调度器的核心接收端 (Receiver) 已被 Drop，但仍有组件尝试发送消息。
    /// - 后果: 这是一个严重的程序 Bug，通常意味着组件生命周期管理出错。
    #[error("Internal communication channel closed.")]
    ChannelClosed,

    /// 任务载荷过大
    ///
    /// - 触发场景: 提交的任务 Context JSON 序列化后体积超过限制 (如 1MB)。
    /// - 后果: 任务被直接丢弃，防止 Redis 网络阻塞。
    /// - 处理: 建议将大文件存入 S3/OSS，任务中只传 URL。
    #[error("Task payload too large: {0} bytes (limit: {1}).")]
    PayloadTooLarge(usize, usize),

    // ==========================================
    // 3. 基础设施与 IO 错误 (Infrastructure & IO)
    // ==========================================
    /// Redis 交互失败
    ///
    /// - 触发场景: 网络抖动、Redis 重启、连接池耗尽、或 Redis 处于 Loading 状态。
    /// - 处理: 此类错误通常是暂时的，系统会自动重试。
    #[cfg(feature = "distributed")]
    #[error("Redis interaction failed: {0}")]
    Redis(#[from] deadpool_redis::redis::RedisError),

    /// Redis Pool 线程池错误
    ///
    /// - 触发场景: 网络抖动、Redis 重启、连接池耗尽、或 Redis 处于 Loading 状态。
    /// - 处理: 此类错误通常是暂时的，系统会自动重试。
    #[cfg(feature = "distributed")]
    #[error("Redis interaction failed: {0}")]
    Pool(#[from] deadpool_redis::PoolError),

    /// 序列化/反序列化失败
    ///
    /// - 触发场景: 数据库里的 JSON 格式损坏，或者代码版本升级导致结构体不兼容。
    /// - 后果: 这是一个不可恢复的错误 (Poison Pill)。
    /// - 处理: 任务会被移入死信队列 (DLQ)。
    #[error("Serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    /// 通用 IO 错误
    ///
    /// - 触发场景: 读写文件失败、网络 Socket 断开等。
    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),

    // ==========================================
    // 4. 分布式一致性错误 (Consistency & Locking)
    // ==========================================
    /// 抢占分布式锁失败
    ///
    /// - 触发场景: 多个节点同时尝试获取同一个任务的锁，竞争失败。
    /// - 处理: 属于正常竞争，系统会自动寻找下一个任务。
    #[error("Failed to acquire lock for task {0} (contention or timeout).")]
    LockAcquireFailed(String),

    /// [核心] Fencing Token (Epoch) 不匹配 (脑裂防御)
    ///
    /// - 触发场景:
    ///     1. 当前 Worker (A) 执行任务过慢，导致心跳超时。
    ///     2. 调度器认为 A 已死，将任务重新分配给 Worker (B)。
    ///     3. Worker (B) 将 Redis 中的 Epoch 版本号 +1。
    ///     4. Worker (A) "复活"并尝试续约心跳或提交结果，发现版本号落后。
    /// - 后果: 操作被拒绝。
    /// - 处理: Worker (A) 必须立即停止当前任务 (自杀机制)。
    #[error("Fencing token mismatch for task {task_id}: expected epoch {expected}, got {actual}.")]
    FencingTokenMismatch {
        task_id: String,
        expected: i64,
        actual: i64,
    },

    /// 节点注册失败
    ///
    /// - 触发场景: 节点 ID 冲突，或者注册中心不可用。
    #[error("Node registration failed: {0}")]
    NodeRegistrationFailed(String),

    // ==========================================
    // 5. 调度逻辑与执行错误 (Scheduling & Execution)
    // ==========================================
    /// Cron 表达式无效
    ///
    /// - 触发场景: 用户提交的定时任务 Cron 字符串格式错误。
    #[error("Invalid cron expression: {0}")]
    InvalidCron(#[from] cron::error::Error),

    /// 任务执行超时
    ///
    /// - 触发场景: 任务执行时间超过了 `execution_timeout` 设置。
    /// - 处理: 任务会被 Cancel 信号中断。
    #[error("Task {0} execution timed out.")]
    TaskTimeout(String),

    /// 任务被取消
    ///
    /// - 触发场景: 用户手动调用 API 取消了任务。
    #[error("Task {0} was cancelled.")]
    TaskCancelled(String),

    /// 任务不存在
    ///
    /// - 触发场景: 尝试操作一个已经被删除或从未存在的任务 ID。
    #[error("Task {0} not found.")]
    TaskNotFound(String),

    // ==========================================
    // 6. 业务与运行时错误 (Business & Runtime)
    // ==========================================
    /// 任务自身逻辑错误
    ///
    /// - 触发场景: 业务代码返回 `Err`，或者 HTTP 请求返回 500。
    /// - 处理: 依据重试策略进行指数退避重试。
    #[error("Task execution failed: {0}")]
    TaskFailure(String),

    /// 毒丸任务 (Poison Pill)
    ///
    /// - 触发场景:
    ///     1. 任务导致 Worker 进程 Panic/Crash。
    ///     2. 任务连续 N 次重试均失败且无明确原因。
    ///     3. 数据格式严重错误，根本无法解析。
    /// - 后果: **不可恢复**。
    /// - 处理: 直接移入死信队列 (DLQ)，不再重试，并报警人工介入。
    #[error("Critical error (Poison Pill): {0}")]
    PoisonPill(String),

    /// 用户代码未知错误 (Anyhow 包装)
    ///
    /// - 说明: 用于包装用户闭包中抛出的各种奇奇怪怪的错误。
    #[error("User task error: {0}")]
    UserTaskError(#[source] anyhow::Error),

    #[error("User Registry task error: {0}")]
    TaskRegistryError(#[source] anyhow::Error),

    /// 持久化层通用错误
    ///
    /// - 说明: 用于包装底层存储 (MySQL/Postgres) 的驱动错误。
    #[error("Persistence layer failure: {0}")]
    Persistence(String),
}

// 自动转换 Tokio MPSC 发送错误
impl<T> From<SendError<T>> for SchedulerError {
    fn from(_: SendError<T>) -> Self {
        SchedulerError::ChannelClosed
    }
}

// --- 工业级特性：智能重试判断 ---
impl SchedulerError {
    /// 判断该错误是否值得重试 (Retryable)
    ///
    /// 该方法用于调度器的错误处理中间件。它区分了 "暂时性故障" 和 "永久性故障"。
    ///
    /// - 返回 `true`: 代表网络抖动、资源暂时不足、锁竞争等。系统应执行指数退避重试。
    /// - 返回 `false`: 代表配置错误、代码 Bug、数据损坏、脑裂等。系统应立即放弃或进入 DLQ。
    pub fn is_retryable(&self) -> bool {
        match self {
            // --- 可以重试的情况 ---

            // 1. 基础设施抖动 (Redis 网络断开、集群切主)
            #[cfg(feature = "distributed")]
            SchedulerError::Redis(e) => {
                // Redis 连接丢了、超时了、正在加载数据 -> 重试
                e.is_connection_dropped() || e.is_cluster_error() || e.is_io_error()
            }

            // 2. IO 错误 (网络超时、文件被占用) -> 重试
            SchedulerError::Io(_) => true,

            // 3. 锁竞争 (别人正在跑，或者锁刚好过期) -> 稍后重试
            SchedulerError::LockAcquireFailed(_) => true,

            // 4. 队列满了 (背压) -> 稍后重试
            SchedulerError::QueueFull(_) => true,

            // 5. 业务逻辑失败 (可能是调用第三方 API 超时) -> 由用户策略决定是否重试
            //    但在系统层面，这是"可重试"的类型
            SchedulerError::TaskFailure(_) => true,
            SchedulerError::UserTaskError(_) => true,
            SchedulerError::TaskTimeout(_) => true,

            // --- 不可重试的情况 (永久性错误) ---

            // 1. 配置和代码错误 -> 重试也没用
            SchedulerError::Config(_) => false,

            // 2. 数据坏了 (JSON 解析失败) -> 毒丸
            SchedulerError::Serialization(_) => false,
            SchedulerError::PayloadTooLarge(_, _) => false,

            // 3. 脑裂 (Fencing Token Mismatch)
            //    核心逻辑：既然令牌不匹配，说明我已经失去所有权，绝对不能重试，必须立即终止！
            SchedulerError::FencingTokenMismatch { .. } => false,

            // 4. 显式毒丸 -> 已判定为有害
            SchedulerError::PoisonPill(_) => false,

            // 5. 逻辑错误 (Cron 写错了、查无此任务)
            SchedulerError::InvalidCron(_) => false,
            SchedulerError::TaskNotFound(_) => false,

            // 6. 系统关闭 -> 没必要重试了
            SchedulerError::SchedulerShutdown => false,
            SchedulerError::ChannelClosed => false,
            SchedulerError::WorkerPoolExhausted(_) => false, // 线程池耗尽通常等待调度器下一轮循环即可，不算"错误重试"

            // 其他未覆盖的默认不重试，防止死循环
            _ => false,
        }
    }
}
