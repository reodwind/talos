use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

use super::MemoryPersistence;
use crate::common::TimeUtils;
use crate::common::error::{Result, SchedulerError};
use crate::common::model::TaskState;
use crate::persistence::AcquireItem;
use crate::persistence::traits::TaskQueue;

#[async_trait]
impl<T> TaskQueue for MemoryPersistence<T>
where
    T: Send + Sync + Clone + 'static,
{
    async fn acquire(
        &self,
        _worker_id: &str,
        batch_size: usize,
        _max_crashes: u32,
    ) -> Result<Vec<AcquireItem>> {
        let now = TimeUtils::now_f64() as u64;
        let mut items = Vec::with_capacity(batch_size);
        // 锁住 Pending 队列，快速切分出到期任务
        let keys_to_process: Vec<(u64, String)> = {
            let mut pending = self.pending_queue.lock();
            let keys: Vec<_> = pending
                .range(..=(now, String::from("\u{10FFFF}")))
                .take(batch_size)
                .map(|(k, _)| k.clone())
                .collect();
            // 必须在锁内移除，防止被别人重复取
            for k in &keys {
                pending.remove(k);
            }
            keys
        };

        if keys_to_process.is_empty() {
            return Ok(items);
        }
        // 移入 Running 队列
        let ttl_secs = self.config.policy.zombie_check_interval_ms / 1000;

        let running_expiry = now + ttl_secs;

        for (_score, id) in keys_to_process {
            // 计算分片索引
            let shard_idx = self.get_shard_index(&id);
            let shard = &self.running_queues[shard_idx];

            {
                let mut running = shard.lock();
                running.insert((running_expiry, id.clone()), ());
            } // 立即释放锁

            // 更新 DashMap 里的 running_expiry 字段
            if let Some(mut entry) = self.data.get_mut(&id) {
                entry.running_expiry = Some(running_expiry);
                entry.state = TaskState::Running;
                // 顺便增加 Epoch 防止脑裂
                entry.epoch += 1;

                items.push(AcquireItem {
                    id: id.clone(),
                    score: entry.epoch,
                });
            }
        }
        Ok(items)
    }

    async fn heartbeat(&self, tasks: &[(String, u64)], _worker_id: &str) -> Result<()> {
        let now = TimeUtils::now_f64() as u64;
        let ttl_secs = self.config.policy.zombie_check_interval_ms / 1000;
        let new_expiry = now + ttl_secs;

        // 任务按分片分组 (Reduce Lock Contention) ---
        // key: 分片索引, value: 属于该分片的任务列表引用
        let mut groups: HashMap<usize, Vec<&(String, u64)>> = HashMap::new();

        for task in tasks {
            let (id, _) = task;
            let shard_idx = self.get_shard_index(id);
            groups.entry(shard_idx).or_default().push(task);
        }

        //针对每个分片，批量处理
        for (shard_idx, group_tasks) in groups {
            let shard_lock = &self.running_queues[shard_idx];

            let mut running = shard_lock.lock();

            for (id, incoming_epoch) in group_tasks {
                match self.data.get_mut(id) {
                    Some(mut task) => {
                        // 如果任务已结束，忽略心跳
                        if task.state != TaskState::Running {
                            continue;
                        }

                        // [Fencing Token Check] 防脑裂核心检查
                        if *incoming_epoch < task.epoch {
                            // 客户端持有的令牌过时，说明任务已被抢占
                            return Err(SchedulerError::FencingTokenMismatch {
                                task_id: id.clone(),
                                expected: task.epoch as i64,
                                actual: *incoming_epoch as i64,
                            });
                        }

                        // 移除旧索引，插入新索引
                        if let Some(old_expiry) = task.running_expiry {
                            running.remove(&(old_expiry, id.clone()));
                        }
                        running.insert((new_expiry, id.clone()), ());

                        // 更新数据
                        task.running_expiry = Some(new_expiry);
                        // 续约成功
                        task.touch();
                    }
                    None => {
                        //任务已经丢失了
                        continue;
                    }
                }
            }
        }
        Ok(())
    }
    /// 内存版的僵尸任务回收实现
    async fn rescue(
        &self,
        _worker_id: &str,
        now: f64,
        _timeout_ms: u64, // Memory 模式下不需要重新计算 timeout，因为 expiry 已经包含了它
        limit: usize,
    ) -> Result<Vec<String>> {
        let now_u64 = now as u64;
        let mut rescued_ids = Vec::new();

        // 内存模式通常分片了，我们需要遍历所有分片
        // 警告：如果分片很多，这里串行锁可能会慢。但 Rescue 是低频操作。
        for shard in &self.running_queues {
            // 1. 快速检查：获取锁并查找过期的 ID
            let ids_in_shard: Vec<(u64, String)> = {
                let mut running = shard.lock();

                // BTreeMap/BTreeSet 的 range API 非常高效
                // 查找 range(..now) 即所有过期任务
                let keys: Vec<_> = running
                    .range(..=(now_u64, String::from("\u{10FFFF}")))
                    .take(limit - rescued_ids.len()) // 控制总量
                    .map(|(k, _)| k.clone()) // k 是 (expiry, id)
                    .collect();

                // 2. 立即移除 (在锁内)
                for k in &keys {
                    running.remove(k);
                }
                keys
            };

            // 3. 处理移除的任务
            if !ids_in_shard.is_empty() {
                let mut pending = self.pending_queue.lock();
                for (_, id) in ids_in_shard {
                    // 加回 Pending 队列
                    // 使用 now_u64 作为 score，排在队列最前面/合适位置
                    pending.insert((now_u64, id.clone()), ());

                    // 同时需要更新 TaskData 里的状态 (TaskState::Pending)
                    if let Some(mut task_entry) = self.data.get_mut(&id) {
                        task_entry.state = TaskState::Pending;
                        task_entry.running_expiry = None;
                        task_entry.worker_id = None;
                        task_entry.epoch = 0; // 重置 epoch
                        rescued_ids.push(id);
                    }
                }
            }

            if rescued_ids.len() >= limit {
                break;
            }
        }

        Ok(rescued_ids)
    }
    async fn release(&self, task_ids: &[String], _worker_id: &str) -> Result<()> {
        for id in task_ids {
            if let Some(mut task) = self.data.get_mut(id) {
                // 仅当任务还在运行时更新时间，具体状态变更依赖 TaskStore::save
                if task.state == TaskState::Running {
                    task.touch();
                }
            }
        }
        Ok(())
    }

    async fn watch(&self, notify: Arc<Notify>) -> Result<()> {
        // 桥接模式: 监听内部 notify -> 触发外部 notify
        let internal = self.notify.clone();
        tokio::spawn(async move {
            internal.notified().await;
            notify.notify_one();
        });
        Ok(())
    }
}
