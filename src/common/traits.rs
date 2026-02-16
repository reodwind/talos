use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use crate::common::model::TaskContext;
use std::sync::Arc;

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
#[async_trait]
pub trait TaskHandler: Send + Sync + 'static {
    /// 定义参数类型 (输入)
    type Args: DeserializeOwned + Send + Sync + Clone + 'static;

    /// 定义返回类型 (输出)
    type Output: Send + Sync + 'static;

    async fn handle(&self, args: Self::Args) -> anyhow::Result<Self::Output>;

    async fn on_result(
        &self,
        _args: Self::Args,
        _res: &anyhow::Result<Self::Output>,
    ) -> anyhow::Result<bool>;
}
#[async_trait]
pub trait ErasedHandler: Send + Sync {
    async fn handle_erased(&self, args: Box<RawValue>) -> anyhow::Result<()>;
}
#[async_trait]
impl<H> ErasedHandler for H
where
    H: TaskHandler + Clone,
{
    async fn handle_erased(&self, args: Box<RawValue>) -> anyhow::Result<()> {
        let this = self.clone();
        let typed_args: H::Args = match serde_json::from_str(args.get()) {
            Ok(v) => v,
            Err(e) => return Err(anyhow::anyhow!("Args parse failed: {}", e)),
        };
        let result = this.handle(typed_args.clone()).await;

        // 触发回调
        let _ = this.on_result(typed_args, &result).await;

        // 抹除返回值类型，只返回 Result<()>
        result.map(|_| ())
    }
}
// 让 Arc<Task> 自动实现 SchedulableTask
#[async_trait]
impl<T, Task> SchedulableTask<T> for Arc<Task>
where
    T: Send + Sync + Clone + 'static,
    Task: SchedulableTask<T>, // 内部必须实现了 SchedulableTask
{
    // type Output = Task::Output; // 转发内部的 Output 类型

    async fn execute(&self, ctx: TaskContext<T>) -> anyhow::Result<()> {
        (**self).execute(ctx).await
    }
}
