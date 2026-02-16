use crate::common::TaskData;
use crate::common::error::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Notify;

// ==========================================
// 1. 任务队列接口 (TaskQueue) - 调度的大脑
// ==========================================

/// 任务队列接口
///
/// **职责**: 负责任务的流转控制、分布式锁、节点心跳和脑裂防御。
/// **特点**:
/// - 高频调用 (High Frequency)
/// - 低延迟要求 (Low Latency)
/// - 数据量极小 (只传输 ID 和 Epoch，不传输任务 Body)
/// - 通常由 Redis (ZSET/Lua) 或内存实现
#[async_trait]
pub trait TaskQueue: Send + Sync + 'static {
    /// 抢占任务 (Acquire)
    ///
    /// 从 Pending 队列中锁定一批任务给当前 Worker。
    ///
    /// # 参数
    /// - `worker_id`: 当前节点的唯一 ID。
    /// - `batch_size`: 期望获取的任务数量。
    /// - `max_crashes`: 毒丸熔断阈值。如果任务崩溃次数超过此值，将被隔离。
    ///
    /// # 返回值
    /// 返回一个元组列表 `(TaskID, Epoch)`。
    /// - `TaskID`: 任务唯一标识。
    /// - `Epoch`: **关键**。Fencing Token (栅栏令牌)。
    ///
    /// - Worker 必须持有此令牌才能进行后续的心跳续约和数据更新。
    /// - 如果 Redis 中的 Epoch 发生变化（被抢占），手持旧 Epoch 的操作将被拒绝。
    async fn acquire(
        &self,
        worker_id: &str,
        batch_size: usize,
        max_crashes: u32,
    ) -> Result<Vec<AcquireItem>>;

    /// 心跳续约 (Heartbeat)
    ///
    /// 维持对运行中任务的锁定，防止其因超时被判定为“僵尸任务”。
    ///
    /// # 参数
    /// - `tasks`: `(TaskID, Epoch)` 的列表。
    /// - **注意**: 必须携带 Epoch！接口会校验 Redis 中的 Epoch 是否与传入的一致。
    /// - 如果不一致（说明发生了脑裂，任务被别人抢了），该任务的心跳将失败。
    /// - `worker_id`: 当前节点 ID。
    async fn heartbeat(&self, tasks: &[(String, u64)], worker_id: &str) -> Result<()>;

    /// 释放/归还任务 (Release)
    ///
    /// 当任务执行完成（无论成功失败），或者 Worker 决定放弃执行时调用。
    /// 这会从 Running 队列中移除任务锁。
    ///
    /// # 注意
    /// 这里的 Release 只是释放“锁”。任务的数据清理或状态更新由 `TaskStore` 负责。
    async fn release(&self, task_ids: &[String], worker_id: &str) -> Result<()>;

    /// 监听新任务信号 (Watch)
    ///
    /// 用于 Pub/Sub 模式。
    /// 当有新任务进入 Pending 队列时，通过 `notify` 唤醒 Driver 立即进行拉取。
    ///
    /// # 参数
    /// - `notify`: Tokio 的通知句柄。实现层应在后台监听（如 Redis Subscribe），收到消息后调用 `notify.notify_one()`。
    async fn watch(&self, notify: Arc<Notify>) -> Result<()>;

    /// 回收僵尸任务 (Rescue)
    ///
    /// 找出并重置那些“疑似已死”的任务。
    ///
    /// # 参数
    /// - `worker_id`: 当前节点的 ID (用于日志或抢占标记)。
    /// - `now`: 当前统一时间戳 (秒/毫秒，取决于实现)。
    /// - `timeout_ms`: 任务超时阈值 (配置中的 visibility_timeout_ms 或 zombie_check_interval_ms)。
    /// - `limit`: 本次回收最大数量，避免单次操作过重。
    async fn rescue(
        &self,
        worker_id: &str,
        now: f64,
        timeout_ms: u64,
        limit: usize,
    ) -> Result<Vec<String>>;
}

// ==========================================
// 2. 任务存储接口 (TaskStore) - 系统的身体
// ==========================================

/// 任务存储接口
///
/// **职责**: 负责任务数据的持久化存储、查询和归档。
/// **特点**:
/// - 数据量大 (包含完整的 JSON 载荷)
/// - 允许较高的延迟 (相比 Queue 而言)
/// - 实现灵活 (可以是 Redis KV，也可以是 MySQL, Postgres, S3, Mongo)
#[async_trait]
pub trait TaskStore<T>: Send + Sync + 'static {
    /// 保存任务 (Save)
    ///
    /// 将任务的完整上下文写入存储。
    /// 如果 ID 已存在，则覆盖更新。
    async fn save(&self, ctx: &TaskData<T>) -> Result<()>;

    /// 加载任务 (Load)
    ///
    /// 根据 ID 获取任务的完整上下文。
    async fn load(&self, id: &str) -> Result<Option<TaskData<T>>>;

    /// 批量加载任务 (Load Batch)
    ///
    /// **性能关键点**: 在 `acquire` 拿到一批 ID 后，通过此接口一次性加载数据。
    /// 避免 N+1 次网络调用。
    async fn load_batch(&self, ids: &[String]) -> Result<Vec<Option<TaskData<T>>>>;

    /// 移除任务 (Remove)
    ///
    /// 物理删除任务数据。通常在任务 `Completed` 或 `Cancelled` 后调用。
    async fn remove(&self, id: &str) -> Result<()>;

    /// 重新入队
    ///
    /// 用于 Cron 任务循环执行，或失败任务的退避重试。
    /// 逻辑：更新任务数据 -> 从 Running 移除 -> 加入 Pending (未来时间)。
    async fn requeue(&self, ctx: TaskData<T>) -> Result<()>;

    /// 移入死信队列 (Move to DLQ)
    ///
    /// - 当任务被判定为“毒丸”(Poison Pill) 或重试次数耗尽时调用。
    /// - 实现层应将任务数据移动到专门的归档区域（如 Redis 的 `dlq` Hash 或 数据库的 `archived_tasks` 表）。
    ///
    /// # 参数
    /// - `ctx`: 任务上下文。
    /// - `err_msg`: 失败原因描述。
    async fn move_to_dlq(&self, ctx: &TaskData<T>, err_msg: String) -> Result<()>;
}

/// acquire 拉取结果
#[derive(Debug, Clone)]
pub struct AcquireItem {
    pub id: String,
    /// ZSet 中的 Score，通常作为 Epoch 或 执行时间戳
    pub score: u64,
}
