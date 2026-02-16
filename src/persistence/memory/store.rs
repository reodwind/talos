use super::MemoryPersistence;
use crate::common::error::Result;
use crate::common::model::{TaskData, TaskState};
use crate::persistence::traits::TaskStore;
use async_trait::async_trait;

#[async_trait]
impl<T> TaskStore<T> for MemoryPersistence<T>
where
    T: Send + Sync + Clone + 'static,
{
    async fn save(&self, ctx: &TaskData<T>) -> Result<()> {
        let id = ctx.id.clone();
        let next = ctx.next_poll_at as u64;

        // 插入或更新
        self.data.insert(ctx.id.clone(), ctx.clone());

        // 存入等待索引 (按时间排序)
        let mut pending = self.pending_queue.lock();
        pending.insert((next, id), ());

        // 如果是 Pending 任务，触发通知（唤醒可能在休眠的 Worker）
        if ctx.state == TaskState::Pending {
            self.notify.notify_waiters();
        }

        Ok(())
    }

    async fn load(&self, id: &str) -> Result<Option<TaskData<T>>> {
        match self.data.get(id) {
            Some(v) => Ok(Some(v.clone())),
            None => Ok(None),
        }
    }

    async fn load_batch(&self, ids: &[String]) -> Result<Vec<Option<TaskData<T>>>> {
        let mut results = Vec::with_capacity(ids.len());
        // 内存操作极快，直接循环获取即可
        for id in ids {
            match self.data.get(id) {
                Some(task) => results.push(Some(task.clone())),
                None => {}
            }
        }
        Ok(results)
    }

    async fn remove(&self, id: &str) -> Result<()> {
        if let Some((_, ctx)) = self.data.remove(id) {
            //从运行索引删除
            if let Some(expiry) = ctx.running_expiry {
                let shard_index = self.get_shard_index(id);
                let mut running = self.running_queues[shard_index].lock();
                running.remove(&(expiry, id.to_string()));
            }
            if ctx.state == TaskState::Pending {
                // 删除排队的旧任务
                let mut pending = self.pending_queue.lock();
                pending.remove(&(ctx.next_poll_at as u64, id.to_string()));
            }
        }
        Ok(())
    }

    /// [核心] 重新入队
    /// Cron 任务执行完后，或者任务重试时调用
    async fn requeue(&self, task: TaskData<T>) -> Result<()> {
        let id = task.id.clone();
        let next_time = task.next_poll_at as u64;

        // 1. 更新数据层 & 获取旧的 Running Expiry
        let old_expiry_opt = if let Some(mut entry) = self.data.get_mut(&id) {
            let old = entry.running_expiry;

            // 全量替换 TaskData (包含了新的 next_poll_at, state=Pending)
            let mut new_task = task.clone();
            new_task.running_expiry = None; // 既然回到了 Pending，就没有 Running Expiry 了
            new_task.state = TaskState::Pending;

            *entry.value_mut() = new_task;
            old
        } else {
            None // 任务可能被外部删了
        };

        // 2. 清理 Running 索引 (从运行状态移除)
        if let Some(expiry) = old_expiry_opt {
            let shard_index = self.get_shard_index(&id);
            let mut running = self.running_queues[shard_index].lock();
            running.remove(&(expiry, id.clone()));
        }

        // 3. 插入 Pending 索引 (放入未来时间轴)
        {
            let mut pending = self.pending_queue.lock();
            pending.insert((next_time, id), ());
        }

        // 4. 唤醒 Driver
        self.notify.notify_waiters();

        Ok(())
    }

    async fn move_to_dlq(&self, ctx: &TaskData<T>, err_msg: String) -> Result<()> {
        let mut dlq = self.dlq.write().await;

        // 原子操作：从主表移除 -> 移入 DLQ
        self.data.remove(&ctx.id);
        dlq.push((ctx.id.clone(), err_msg));

        Ok(())
    }
}
