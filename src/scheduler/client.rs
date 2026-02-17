use std::{sync::Arc, time::Duration};

use anyhow::bail;
use serde::Serialize;

#[cfg(feature = "redis-store")]
use crate::persistence::RedisPersistence;
use crate::{
    common::{ScheduleType, TaskData, TimeUtils, new_task_id},
    persistence::TaskStore,
};

/// JSON 数据包装器
///
/// 使用此包装器来告诉 Client 将数据序列化为 JSON。
///
/// # Example
/// ```rust
/// client.submit("email", Json(my_struct), ...).await?;
/// ```
pub struct Json<T>(pub T);

/// 原始字节包装器
///
/// 使用此包装器来告诉 Client 直接使用原始字节（Zero-Copy）。
///
/// # Example
/// ```rust
/// client.submit("video", Bytes(vec![0x01, 0x02]), ...).await?;
/// ```
pub struct Bytes(pub Vec<u8>);

// ==========================================
// Payload 转换 Trait
// ==========================================
/// 定义如何将不同类型的数据转换为任务 Payload (Vec<u8>)
pub trait IntoPayload {
    fn into_payload(self) -> anyhow::Result<Vec<u8>>;
}
impl<T> IntoPayload for Json<T>
where
    T: Serialize,
{
    fn into_payload(self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(&self.0).map_err(|e| anyhow::anyhow!("JSON serialization failed: {}", e))
    }
}

impl IntoPayload for Bytes {
    fn into_payload(self) -> anyhow::Result<Vec<u8>> {
        Ok(self.0)
    }
}

// ==========================================
// SchedulerClient 实现
// ==========================================

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
    #[cfg(feature = "redis-store")]
    pub fn new_redis(url: &str) -> anyhow::Result<Self>
    where
        T: serde::de::DeserializeOwned + Clone,
    {
        // 使用默认配置
        let config = crate::common::SchedulerConfig::default();
        let store = Arc::new(RedisPersistence::new(&config, url)?);
        Ok(Self::new(store))
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
        if task.created_at == 0.0 {
            task.created_at = now;
            task.updated_at = now;
            task.next_poll_at =
                TimeUtils::next_initial_time(&task.schedule_type, task.timezone.as_deref(), now);
        }

        let task_id = task.id.clone();

        self.store.save(&task).await?;
        Ok(task_id)
    }
}

impl SchedulerClient<Vec<u8>> {
    /// 通用提交入口 (一次性任务)
    ///
    /// # 参数
    /// - `task_type`: 任务类型 (路由键)
    /// - `payload`: 数据包装器，支持 `Json(...)` 或 `Bytes(...)`
    ///
    /// # 示例
    /// ```rust
    /// client.submit("email", Json(args)).await?;
    /// client.submit("video", Bytes(data)).await?;
    /// ```
    pub async fn submit<P>(&self, task_type: &str, payload: P) -> anyhow::Result<String>
    where
        P: IntoPayload,
    {
        self.submit_with_schedule(task_type, payload, ScheduleType::Once)
            .await
    }
    /// 提交延时任务
    pub async fn submit_delay<P>(
        &self,
        task_type: &str,
        payload: P,
        delay: Duration,
    ) -> anyhow::Result<String>
    where
        P: IntoPayload,
    {
        self.submit_with_schedule(
            task_type,
            payload,
            ScheduleType::Delay(delay.as_millis() as u64),
        )
        .await
    }

    /// 提交 Cron 定时任务
    pub async fn submit_cron<P>(
        &self,
        task_type: &str,
        payload: P,
        cron: &str,
    ) -> anyhow::Result<String>
    where
        P: IntoPayload,
    {
        self.submit_with_schedule(task_type, payload, ScheduleType::Cron(cron.to_string()))
            .await
    }
    /// [全能入口] 指定任意调度类型
    pub async fn submit_with_schedule<P>(
        &self,
        task_type: &str,
        payload: P,
        schedule: ScheduleType,
    ) -> anyhow::Result<String>
    where
        P: IntoPayload,
    {
        // 1. 利用 Trait 统一处理序列化
        let bytes = payload.into_payload()?;

        // 2. 构建任务
        let mut task = TaskData::new(new_task_id(), bytes, schedule);
        task.name = task_type.to_string();

        // 3. 提交
        self.submit_raw(task).await
    }
}
