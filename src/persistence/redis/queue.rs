use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep; // 用于 PubSub stream

use super::core::RedisPersistence;
use crate::common::TimeUtils;
use crate::common::error::{Result, SchedulerError};
use crate::persistence::AcquireItem;
use crate::persistence::traits::TaskQueue;
use deadpool_redis::redis::{self};

#[async_trait]
impl TaskQueue for RedisPersistence {
    async fn acquire(
        &self,
        worker_id: &str,
        batch_size: usize,
        _max_crashes: u32,
    ) -> Result<Vec<AcquireItem>> {
        let mut conn = self.pool.get().await?;
        let now = TimeUtils::now_f64();

        // 调用 Lua 脚本 "acquire"
        // KEYS[1]: pending_zset
        // KEYS[2]: running_zset
        // ARGV[1]: meta_prefix
        // ARGV[2]: worker_id
        // ARGV[3]: now_score
        // ARGV[4]: batch_size
        let result: Vec<String> = self
            .scripts
            .acquire
            .key(self.key_pending())
            .key(self.key_running())
            .arg(self.key_meta_prefix()) // 注意：脚本里会拼接 id
            .arg(worker_id)
            .arg(now)
            .arg(batch_size)
            .invoke_async(&mut conn)
            .await?;

        // Lua 返回的是 [id1, epoch1, id2, epoch2, ...] 的扁平数组
        // 需要将其转换为 Vec<(String, u64)>
        let mut rwa_pairs = Vec::with_capacity(result.len() / 2);
        let mut iter = result.into_iter();

        while let Some(id) = iter.next() {
            if let Some(epoch_str) = iter.next() {
                let epoch = epoch_str.parse::<u64>().unwrap_or(0);
                rwa_pairs.push((id, epoch));
            }
        }
        let pairs = rwa_pairs
            .into_iter()
            .map(|(id, score)| AcquireItem { id, score })
            .collect();
        Ok(pairs)
    }

    async fn heartbeat(&self, tasks: &[(String, u64)], worker_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let now = TimeUtils::now_f64();

        // 这里需要对每个任务单独调用 Lua (或者重写 Lua 支持批量)
        // 为了性能，通常建议用 Pipeline 批量调用脚本，但 redis crate 对 Pipeline Script 支持稍繁琐。
        // 这里使用 Pipeline 包装多次脚本调用。
        let mut pipe = redis::pipe();

        for (id, epoch) in tasks {
            pipe.invoke_script(
                self.scripts
                    .heartbeat
                    .clone()
                    .key(self.key_running())
                    .key(self.key_meta(id))
                    .arg(id)
                    .arg(worker_id)
                    .arg(*epoch) // Client 持有的 Epoch
                    .arg(now),
            );
        }

        // 执行 Pipeline
        let results: Vec<i32> = pipe.query_async(&mut conn).await?;

        // 检查结果
        for (i, code) in results.iter().enumerate() {
            if *code == -2 {
                // 错误码 -2: Fencing Token Mismatch
                let (id, expected) = &tasks[i];
                return Err(SchedulerError::FencingTokenMismatch {
                    task_id: id.clone(),
                    expected: *expected as i64, // 这里为了简化，没从 Redis 拿最新的，但足以报错
                    actual: *expected as i64,   // 这里的语义是 "我以为是 expected，但被拒绝了"
                });
            }
            // 代码 -1 (Task Not Found) 通常可以忽略，或者记录日志
        }

        Ok(())
    }

    async fn release(&self, task_ids: &[String], _worker_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();

        for id in task_ids {
            pipe.invoke_script(self.scripts.release.clone().key(self.key_running()).arg(id));
        }

        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn watch(&self, notify: Arc<Notify>) -> Result<()> {
        let channel_name = self.key_notify();

        let client = self.client.clone();

        // 开启一个后台任务监听 Pub/Sub
        tokio::spawn(async move {
            loop {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let config = redis::AsyncConnectionConfig::new().set_push_sender(tx);
                let conn_result = client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await;
                match conn_result {
                    Ok(mut conn) => {
                        if let Err(e) = conn.psubscribe(&channel_name).await {
                            eprintln!("[Watch] Subscribe error: {}. Retry in 3s...", e);
                            sleep(Duration::from_secs(3)).await;
                            continue;
                        }
                        while let Some(push_msg) = rx.recv().await {
                            match push_msg.kind {
                                redis::PushKind::Message => {
                                    notify.notify_one();
                                }
                                _ => {}
                            }
                        }
                        eprintln!("[Watch] Connection closed. Reconnecting...");
                    }
                    Err(e) => {
                        eprintln!("[Watch] Connect failed: {}. Retry in 3s...", e);
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });

        Ok(())
    }
}
