use serde::{Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

use crate::common::{TimeUtils, new_task_id};

// 定义扩展容器类型
pub type Extensions = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

// ==========================================
// 1. 任务状态枚举 (TaskState)
// ==========================================

/// 任务生命周期状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    /// 等待中
    /// - 任务已提交，正在队列中等待被 Driver 获取。
    Pending,

    /// 运行中
    /// - 任务已被 Worker 锁定并开始执行。
    /// - 处于此状态的任务必须定期发送心跳，否则会被视为僵尸任务。
    Running,

    /// 已完成
    /// - 任务执行成功。
    Completed,

    /// 已失败
    /// - 任务执行抛错，且已达到最大重试次数，或者遇到了不可恢复的错误（毒丸）。
    Failed,

    /// 已取消
    /// - 用户手动取消，或因超时被系统强制取消。
    Cancelled,

    /// 调度中 (仅用于 Cron 任务的模板)
    /// - 代表这不是一个具体的执行实例，而是一个定时任务模板。
    Scheduled,
}

impl TaskState {
    /// 状态是否是终态（不可流转）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        )
    }
}

// ==========================================
// 2. 调度类型 (ScheduleType)
// ==========================================

/// 任务调度类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ScheduleType {
    /// 一次性任务 (立即执行)
    Once,

    /// 延时任务 (Run At)
    /// 在指定的时间戳之后执行。
    Delay(u64),

    /// 定时任务 (Cron)
    /// 遵循 Cron 表达式周期性执行。
    /// 字段: Cron 表达式字符串 (e.g., "0 */5 * * * *")
    Cron(String),
}

// ==========================================
// 3. 核心任务数据 (TaskData)
// ==========================================

/// 任务数据
///
/// - 这是在 Scheduler, Queue, Driver 和 Worker 之间流转的核心数据包。
/// - 它包含了任务的所有信息：数据载荷、状态、以及分布式控制所需的元数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskData<T> {
    // --- 基础标识 ---
    /// 全局唯一的任务 ID
    ///
    /// 通常使用 UUID 或 NanoID 生成。
    pub id: String,

    /// 任务名称/分类，用于日志和监控聚合
    pub name: String,

    /// 任务分组/队列名 (可选，用于多租户或多通道)
    pub queue: String,

    /// 任务数据载荷
    /// 泛型 T 通常是具体的业务参数结构体，或者 `serde_json::Value`。
    pub payload: Arc<T>,

    // --- 调度控制 ---
    /// 当前调度类型
    pub schedule_type: ScheduleType,

    // --- 状态与归属 ---
    /// 当前状态
    pub state: TaskState,

    /// 当前持有该任务的 Worker ID
    /// - 用于心跳续约和问题追踪
    #[serde(default)]
    pub worker_id: Option<String>,

    /// 优先级 (可选)
    /// 数字越大优先级越高，用于 VIP 队列抢占。
    #[serde(default)]
    pub priority: u8,

    // --- 分布式一致性 (核心) ---
    /// [Fencing Token] 逻辑时钟 / 纪元
    ///
    /// - 乐观锁版本号 (CAS 核心)
    /// - 每次状态变更或重新入队，epoch + 1
    ///
    /// - 作用: 防止脑裂 (Split-Brain)。
    /// - 机制:
    ///   1. 每次任务被 Worker 抢占（从 Pending -> Running）时，Redis 中的 Epoch 会自增。
    ///   2. 这里的 `epoch` 字段存储 Worker 抢占那一刻拿到的快照。
    ///   3. Worker 在心跳续约或写库时，必须校验此 epoch 是否等于 Redis 中的最新值。
    ///   4. 如果不相等，说明任务已被其他 Worker 抢走，当前 Worker 必须立即停止。
    #[serde(default)]
    pub epoch: u64,

    // --- 统计与重试 ---
    /// 已重试次数
    #[serde(default)]
    pub retry_count: u32,

    /// 最大重试次数
    #[serde(default)]
    pub max_retries: u32,

    /// 当前已重试次数
    #[serde(default)]
    pub attempt: u32,

    /// 也就是最后一次失败的错误信息 (用于调试)
    #[serde(default)]
    pub last_error: Option<String>,

    /// 下次可被拉取的时间戳 (秒)
    /// 用于退避重试或延时任务。
    pub next_poll_at: f64,

    // ================= 调度控制 =================
    /// Cron 表达式 (持久化存储)
    /// None = 一次性任务; Some("*/5 * * * * *") = 循环任务
    pub cron_expression: Option<String>,

    /// 时区字符串
    pub timezone: Option<String>,

    /// 创建时间 (Unix Timestamp Secs)
    pub created_at: f64,

    /// 最后更新时间 (Unix Timestamp Secs)
    pub updated_at: f64,

    // 专门给内存版用的索引辅助字段
    pub(crate) running_expiry: Option<u64>,
    // --- 扩展元数据 ---
    /// 扩展字段
    /// 用于存储 Workflow ID, Trace ID, 用户 IP 等非业务逻辑强相关的元数据。
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl<T> TaskData<T> {
    /// 创建一个新的任务上下文
    pub fn new(id: String, payload: T, schedule_type: ScheduleType) -> Self {
        let now = TimeUtils::now_f64();
        // // 初始可拉取时间
        let next_poll_at = TimeUtils::next_initial_time(&schedule_type, None, now);

        Self {
            id,
            name: new_task_id(),
            worker_id: None,
            payload: Arc::new(payload),
            queue: "default".into(),
            schedule_type,
            state: TaskState::Pending,
            priority: 0,
            epoch: 0, // 初始 Epoch 为 0
            retry_count: 0,
            max_retries: 3,
            attempt: 0,
            last_error: None,
            next_poll_at,
            cron_expression: None,
            timezone: None,
            created_at: now,
            updated_at: now,
            running_expiry: None,
            metadata: HashMap::new(),
        }
    }

    /// 更新最后活动时间
    pub fn touch(&mut self) {
        self.updated_at = TimeUtils::now_f64();
    }

    /// 获取元数据
    pub fn get_meta(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// 设置元数据
    pub fn set_meta(mut self, meta: HashMap<String, String>) -> Self {
        self.metadata = meta;
        self
    }
    /// 添加元数据 (例如 TraceID)
    pub fn add_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// 设置最大重试次数
    pub fn set_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// 设置优先级
    pub fn set_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
    /// 检查是否还能重试
    pub fn can_retry(&self) -> bool {
        self.attempt <= self.max_retries
    }

    /// 调用设置 Cron
    /// - `expr`: cron 表达式
    /// - `tz`: 时区
    pub fn with_cron(mut self, expr: &str, tz: &str) -> Self {
        self.cron_expression = Some(expr.to_string());
        self.timezone = Some(tz.to_string());
        self
    }
}

impl<T> TaskData<T> {
    /// 标记为运行中
    ///
    /// Driver 在抢占任务成功后，会调用此方法来更新内存中 Context 的状态，
    /// 以便后续的 execute 逻辑能拿到正确的 worker_id 和 epoch。
    pub fn mark_running(&mut self, worker_id: String, new_epoch: u64) {
        // 1. 状态变更为 Running
        self.state = TaskState::Running;

        // 2. 记录是谁抢到了 (当前节点 ID)
        self.worker_id = Some(worker_id);

        // 3. 更新 Fencing Token (防止脑裂)
        self.epoch = new_epoch;

        // 4. 刷新最后更新时间
        self.touch();

        // 5. 尝试次数 +1 (表示这又是一次新的尝试)
        self.attempt += 1;
    }
}

// ==========================================
// 4. 统计指标 (SchedulerStats)
// ==========================================

/// 调度器运行时统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerStats {
    /// 待处理任务数
    pub pending_tasks: usize,
    /// 运行中任务数
    pub running_tasks: usize,
    /// 成功任务计数 (累计)
    pub completed_count: u64,
    /// 失败任务计数 (累计)
    pub failed_count: u64,
    /// 活跃 Worker 线程数
    pub active_workers: usize,
    /// 错误率 (0.0 - 1.0)
    pub error_rate: f64,
}

/// 运行时上下文，包含数据和控制工具
#[derive(Clone)]
pub struct TaskContext<T> {
    // 将数据包裹在里面
    pub data: TaskData<T>,
    // 运行时工具 (不可序列化)
    pub token: CancellationToken,
    // 全局扩展容器 (可选注入，用于 Workflow ID, Trace ID 等)
    pub extensions: Arc<Extensions>,
}
impl<T> TaskContext<T> {
    // 构造函数：把数据和 Token 组装起来
    pub fn new(data: TaskData<T>, token: CancellationToken, extensions: Arc<Extensions>) -> Self {
        Self {
            data,
            token,
            extensions,
        }
    }
    // 代理访问 payload，方便用户
    pub fn payload(&self) -> &T {
        &self.data.payload
    }
    /// 判断是否取消
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
    /// 取消任务
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}
