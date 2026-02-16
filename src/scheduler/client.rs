use std::{sync::Arc, time::Duration};

use anyhow::bail;
use serde::Serialize;

use crate::{
    common::{ScheduleType, TaskData, TimeUtils, model::DynamicJob, new_task_id},
    persistence::TaskStore,
};

pub struct SchedulerClient<T> {
    store: Arc<dyn TaskStore<T>>,
}

impl<T> SchedulerClient<T>
where
    T: Serialize + Send + Sync + 'static,
{
    pub fn new(store: Arc<dyn TaskStore<T>>) -> Self {
        Self { store }
    }
    /// 提交原始 TaskData
    pub async fn submit_raw(&self, mut task: TaskData<T>) -> anyhow::Result<String> {
        // === 1. 数据完整性验证 ===
        if task.id.trim().is_empty() {
            bail!("Task ID cannot be empty");
        }
        if task.name.trim().is_empty() {
            task.name = "default".to_string()
        }
        TimeUtils::validate_schedule(&task.schedule_type)?;

        let now = TimeUtils::now();
        task.next_poll_at =
            TimeUtils::next_initial_time(&task.schedule_type, task.timezone.as_deref(), now);
        task.created_at = now;
        task.updated_at = now;

        let task_id = task.id.clone();

        self.store.save(&task).await?;
        Ok(task_id)
    }

    // 提交通用任务
    pub async fn submit(&self, payload: T, schedule: ScheduleType) -> anyhow::Result<String> {
        let task_id = new_task_id();
        let task = TaskData::new(task_id.clone(), payload, schedule);
        self.submit_raw(task).await
    }
    // 提交实时任务
    pub async fn submit_now(&self, payload: T) -> anyhow::Result<String> {
        let task_id = new_task_id();
        let task = TaskData::new(task_id.clone(), payload, ScheduleType::Once);
        self.submit_raw(task).await
    }
    /// 提交延时任务
    pub async fn submit_delay(&self, payload: T, delay: Duration) -> anyhow::Result<String> {
        self.submit(payload, ScheduleType::Delay(delay.as_millis() as u64))
            .await
    }
    /// 提交Cron任务
    pub async fn submit_cron(&self, payload: T, cron_expr: &str) -> anyhow::Result<String> {
        self.submit(payload, ScheduleType::Cron(cron_expr.to_string()))
            .await
    }
}
/// 针对的是注册机制
impl SchedulerClient<DynamicJob> {
    pub async fn submit_dynamic(&self, name: &str, args: impl Serialize) -> anyhow::Result<String> {
        let job = DynamicJob::new(name, serde_json::value::to_raw_value(&args)?);
        self.submit(job, ScheduleType::Once).await
    }
}
