use crate::common::TaskData;

/// 加载状态枚举
#[derive(Debug)]
pub enum LoadStatus<T> {
    /// 成功加载：数据存在且完整
    Found(TaskData<T>),
    /// 数据不存在：没有找到对应的任务数据
    NotFound,
    /// 数据损坏：数据存在但无法解析（如 JSON 反序列化失败）
    DataCorrupted { reason: String, raw_content: String },
}
/// acquire 拉取结果
#[derive(Debug, Clone)]
pub struct AcquireItem {
    pub id: String,
    /// ZSet 中的 Score，通常作为 Epoch 或 执行时间戳
    pub score: u64,
}
