use async_trait::async_trait;

use super::core::RedisPersistence;
use crate::common::TimeUtils;
use crate::common::error::Result;
use crate::common::model::{TaskData, TaskState};
use crate::persistence::traits::TaskStore;
use deadpool_redis::redis::{self, AsyncCommands};

#[async_trait]
impl<T> TaskStore<T> for RedisPersistence
where
    T: Send + Sync + Clone + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    async fn save(&self, ctx: &TaskData<T>) -> Result<()> {
        let mut conn = self.pool.get().await?;

        let data_key = self.key_data(&ctx.id);
        let pending_key = self.key_pending();
        let notify_key = self.key_notify();

        // 1. 序列化任务数据
        let json = serde_json::to_string(ctx)?;

        // 2. 使用 Pipeline 提高性能
        let mut pipe = redis::pipe();

        // (A) 保存数据主体 (KV)
        pipe.set(&data_key, json);

        // (B) 如果是 Pending 状态，加入 Pending 队列 (ZSET)
        // Score = next_poll_at (调度时间)
        if ctx.state == TaskState::Pending {
            pipe.zadd(&pending_key, ctx.id.clone(), ctx.next_poll_at);

            // (C) 发送通知 (Pub/Sub) 唤醒订阅者
            pipe.publish(&notify_key, &ctx.id);
        }

        // 执行 Pipeline
        pipe.query_async::<()>(&mut conn).await?;

        Ok(())
    }

    async fn load(&self, id: &str) -> Result<Option<TaskData<T>>> {
        let mut conn = self.pool.get().await?;
        let key = self.key_data(id);

        // GET string
        let json: Option<String> = conn.get(key).await?;

        match json {
            Some(s) => {
                let ctx = serde_json::from_str(&s)?;
                Ok(Some(ctx))
            }
            None => Ok(None),
        }
    }

    async fn load_batch(&self, ids: &[String]) -> Result<Vec<Option<TaskData<T>>>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.pool.get().await?;

        // 构造 MGET 批量查询
        let keys: Vec<String> = ids.iter().map(|id| self.key_data(id)).collect();
        let json_list: Vec<Option<String>> = conn.get(keys).await?;

        let mut results = Vec::with_capacity(ids.len());
        for json_opt in json_list {
            match json_opt {
                Some(s) => {
                    let ctx = serde_json::from_str(&s)?;
                    results.push(Some(ctx));
                }
                None => results.push(None),
            }
        }
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();

        // 彻底清理：数据、元数据、队列索引
        pipe.del(self.key_data(id))
            .del(self.key_meta(id))
            .zrem(self.key_pending(), id)
            .zrem(self.key_running(), id);

        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn requeue(&self, ctx: TaskData<T>) -> Result<()> {
        let mut conn = self.pool.get().await?;

        // 1. 序列化新的任务数据 (包含了新的 next_poll_at, attempt 等)
        let json = serde_json::to_string(&ctx)?;
        let id = ctx.id.clone();
        let next_score = ctx.next_poll_at; // f64

        // 2. 使用 Lua 脚本保证原子性: 更新数据 + 移出 Running + 移入 Pending
        // 这里可以直接写脚本，也可以放到 scripts/redis_requeue.lua
        let script = redis::Script::new(
            r#"
            -- ARGV[1]: json
            -- ARGV[2]: next_score
            -- ARGV[3]: task_id
            -- KEYS[1]: data_key
            -- KEYS[2]: running_zset
            -- KEYS[3]: pending_zset
            
            redis.call('SET', KEYS[1], ARGV[1])
            redis.call('ZREM', KEYS[2], ARGV[3])
            redis.call('ZADD', KEYS[3], ARGV[2], ARGV[3])
            return 1
        "#,
        );

        script
            .key(self.key_data(&id))
            .key(self.key_running())
            .key(self.key_pending())
            .arg(json)
            .arg(next_score)
            .arg(id)
            .invoke_async::<()>(&mut conn)
            .await?;

        Ok(())
    }

    async fn move_to_dlq(&self, ctx: &TaskData<T>, err_msg: String) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();

        // 1. 序列化 (包含错误信息)
        // 实际场景可能需要一个新的 DLQ 结构体，这里简化处理
        let dlq_entry = serde_json::json!({
            "task": ctx,
            "error": err_msg,
            "failed_at": TimeUtils::now_f64()
        })
        .to_string();

        // 2. 存入 DLQ (使用 List 结构)
        pipe.lpush(self.key_dlq(), dlq_entry);

        // 3. 清理原数据
        pipe.del(self.key_data(&ctx.id))
            .del(self.key_meta(&ctx.id))
            .zrem(self.key_pending(), &ctx.id)
            .zrem(self.key_running(), &ctx.id);

        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }
}
