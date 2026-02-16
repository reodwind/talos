use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    common::TaskContext,
    driver::{DriverMetrics, DriverPlugin},
};

pub struct MetricsPlugin<T> {
    metrics: Arc<DriverMetrics>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> MetricsPlugin<T> {
    pub fn new(metrics: Arc<DriverMetrics>) -> Self {
        Self {
            metrics,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> DriverPlugin<T> for MetricsPlugin<T>
where
    T: Send + Sync + Clone + 'static,
{
    // 记录总请求数
    async fn before_execute(&self, _ctx: &TaskContext<T>) {
        self.metrics.inc_active();
    }
    // 记录执行时长
    async fn after_execute(&self, _ctx: &TaskContext<T>, _duration: f64) {}
    // 任务成功 -> -1, success+1
    async fn on_success(&self, _task: &TaskContext<T>) {
        self.metrics.dec_active();
        self.metrics.inc_success();
    }
    // 任务失败 -> -1, failure+1
    async fn on_failure(&self, _ctx: &TaskContext<T>, _error: &str) {
        self.metrics.dec_active();
        self.metrics.inc_failure();
    }
}
