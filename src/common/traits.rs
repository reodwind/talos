use std::sync::Arc;

use async_trait::async_trait;

use crate::common::TaskContext;

// ==========================================
// 2. 核心任务接口 (SchedulableTask)
// ==========================================

/// 可调度任务接口
///
/// # 泛型说明
/// - `T`: **Input** (任务输入参数)。必须是 `Send + Sync + Clone`。
///
/// # 关联类型
/// - `Output`: **Output** (任务执行结果)。用户可以自定义返回类型。
///
/// # 设计哲学
/// 作为一个库，我们不限制 Output 的类型。
/// - 如果你想存入 Redis，可以在 Output 定义为 `serde_json::Value`。
/// - 如果你只是打印日志不返回数据，可以在 Output 定义为 `()`。
/// - 如果你是流式计算，可以在 Output 定义为 `Vec<f64>`。
#[async_trait]
pub trait SchedulableTask<T>: Send + Sync + 'static
where
    T: Send + Sync + Clone + 'static,
{
    /// 定义任务执行成功的返回类型
    // type Output: Send + Sync + 'static;

    /// 执行具体的业务逻辑
    ///
    /// 返回 `anyhow::Result<Self::Output>`。
    /// - `Ok(val)`: 任务成功，返回结果。
    /// - `Err(e)`: 任务失败，调度器将捕获错误并进行重试。
    async fn execute(&self, ctx: TaskContext<T>) -> anyhow::Result<()>;
}

/// 为 Arc<Task> 自动实现 SchedulableTask<T>
#[async_trait]
impl<T, Task> SchedulableTask<T> for Arc<Task>
where
    T: Send + Sync + Clone + 'static,
    Task: SchedulableTask<T>,
{
    async fn execute(&self, ctx: TaskContext<T>) -> anyhow::Result<()> {
        (**self).execute(ctx).await
    }
}
