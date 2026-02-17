use std::{sync::Arc, time::Duration};

use anyhow::bail;
use serde::Serialize;

use crate::{
    common::{ScheduleType, TaskData, TimeUtils, new_task_id},
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

impl SchedulerClient<Vec<u8>> {
    /// 提交一个 JSON 任务
    ///
    /// # 参数
    /// - `task_type`: 任务类型 (路由键)，例如 "send_email"
    /// - `payload`: 参数结构体，会被自动序列化为 JSON Bytes
    pub async fn submit_json<P>(&self, task_type: &str, payload: P) -> anyhow::Result<String>
    where
        P: Serialize + Send + Sync,
    {
        // 1. 序列化为 JSON 字节
        let bytes = serde_json::to_vec(&payload)?;
        // 2. 提交
        self.submit_bytes(task_type, bytes, ScheduleType::Once).await
    }
    /// 提交一个 JSON 延时任务
    pub async fn submit_json_delay<P>(&self, task_type: &str, payload: P, delay: Duration) -> anyhow::Result<String>
    where
        P: Serialize + Send + Sync,
    {
        let bytes = serde_json::to_vec(&payload)?;
        self.submit_bytes(task_type, bytes, ScheduleType::Delay(delay.as_millis() as u64)).await
    }

    /// 提交一个 JSON Cron 任务
    pub async fn submit_json_cron<P>(&self, task_type: &str, payload: P, cron: &str) -> anyhow::Result<String>
    where
        P: Serialize + Send + Sync,
    {
        let bytes = serde_json::to_vec(&payload)?;
        self.submit_bytes(task_type, bytes, ScheduleType::Cron(cron.to_string())).await
    }

    /// 提交原始字节任务 (通用入口)
    pub async fn submit_bytes(&self, task_type: &str, payload: Vec<u8>, schedule: ScheduleType) -> anyhow::Result<String> {
        let task_id = new_task_id();
        let mut task = TaskData::new(task_id, payload, schedule);
        
        // 关键：设置 Task Name，Router 靠这个分发
        task.name = task_type.to_string(); 
        
        self.submit_raw(task).await
    }
}