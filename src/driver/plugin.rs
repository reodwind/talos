use crate::{common::TaskContext, driver::context::DriverContext};
use async_trait::async_trait;

/// 驱动器插件/钩子接口
///
/// 用户可以通过实现此 Trait 来介入 Driver 的生命周期。
/// 常见用途：
/// - 自定义日志/监控 (Metrics)
/// - 错误报警 (Alerting)
/// - 动态流控 (Rate Limiting)
/// - 任务过滤 (Filtering)
#[async_trait]
pub trait DriverPlugin<T>: Send + Sync + 'static
where
    T: Send + Sync + Clone + 'static,
{
    /// [生命周期] Driver 启动时调用
    async fn on_start(&self, _ctx: &DriverContext<T>) {}

    /// [生命周期] Driver 关闭时调用
    async fn on_shutdown(&self, _ctx: &DriverContext<T>) {}

    /// 每次拉取任务前调用
    ///
    /// # 返回值
    /// - `true`: 允许拉取。
    /// - `false`: 暂停拉取（例如检测到磁盘满了，或者系统负载过高，想暂时“踩刹车”）。
    async fn before_fetch(&self, _ctx: &DriverContext<T>) -> bool {
        true
    }

    /// [任务] 任务开始执行前调用
    async fn before_execute(&self, _task: &TaskContext<T>) {}

    /// [执行后] 任务执行结束 (无论成功失败都会调用)
    /// 用于记录总耗时
    async fn after_execute(&self, _ctx: &TaskContext<T>, _duration: f64) {}

    /// [执行成功] 任务逻辑执行成功 (Ok)
    async fn on_success(&self, _ctx: &TaskContext<T>) {}

    /// [执行失败] 任务逻辑执行失败 (Err 或 Panic) [新增]
    async fn on_failure(&self, _ctx: &TaskContext<T>, _error: &str) {}
}

// ==========================================
// 默认的空插件 (No-Op)
// ==========================================

pub struct NoOpPlugin;

#[async_trait]
impl<T> DriverPlugin<T> for NoOpPlugin where T: Send + Sync + Clone + 'static {}
