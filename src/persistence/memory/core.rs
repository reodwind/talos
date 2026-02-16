use dashmap::DashMap;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};
use tokio::sync::{Notify, RwLock};

use crate::common::{SchedulerConfig, TaskData};

/// 内存持久化实现 (In-Memory Persistence)
///
/// 同时实现了 TaskQueue 和 TaskStore。
/// 使用 RwLock 保证线程安全。
#[derive(Debug)]
pub struct MemoryPersistence<T> {
    /// 注入全局配置 (新增)
    pub(super) config: Arc<SchedulerConfig>,

    /// 【数据仓库】全量数据
    /// - 核心数据存储: ID -> TaskContext
    /// - 使用 pub(super) 让 queue.rs 和 store.rs 能访问，但对外部保密
    /// - DashMap: 分片锁，高并发读写不排队
    pub(super) data: Arc<DashMap<String, TaskData<T>>>,

    /// 【等待索引】(Time, ID) -> ()
    /// - 用于快速找出 <= now 的任务
    pub(super) pending_queue: Arc<Mutex<BTreeMap<(u64, String), ()>>>,

    // 记录分片数量的掩码 (用于快速取模)
    pub(super) shard_mask: usize,

    /// 【运行索引】(ExpiryTime, ID) -> ()
    /// - 用于 Watchdog 快速找出超时的任务
    pub(super) running_queues: Vec<Arc<Mutex<BTreeMap<(u64, String), ()>>>>,

    /// 死信队列 (DLQ)
    pub(super) dlq: Arc<RwLock<Vec<(String, String)>>>,

    /// 通知信号
    pub(super) notify: Arc<Notify>,
}

impl<T> MemoryPersistence<T> {
    /// 创建一个新的内存持久化实例
    pub fn new(config: SchedulerConfig) -> Self {
        let concurrency = config.worker.max_concurrency;
        //计算最佳分片数 (取大于等于并发数的 2 的幂次)
        let shard_count = if concurrency < 64 {
            64 // 只有几十个并发时，默认 64 足够
        } else {
            concurrency.next_power_of_two()
        };
        // 初始化分片槽位
        let mut queues = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            queues.push(Arc::new(Mutex::new(BTreeMap::new())));
        }

        Self {
            config: Arc::new(config),
            data: Arc::new(DashMap::new()),
            pending_queue: Arc::new(Mutex::new(BTreeMap::new())),
            running_queues: queues,
            shard_mask: shard_count - 1, // 2^N - 1，用于位运算取模
            dlq: Arc::new(RwLock::new(Vec::new())),
            notify: Arc::new(Notify::new()),
        }
    }
    /// 辅助：计算 ID 对应的分片索引
    /// 复制这个方法到你的 memory/core.rs 或在这里直接使用
    pub(super) fn get_shard_index(&self, id: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        // 使用位运算代替取模 (前提: shard_count 是 2 的幂)
        // 这里的 shard_mask 在 core.rs 初始化时计算好了
        hash & self.shard_mask
    }
}

// Clone 实现：因为内部都是 Arc，所以 Clone 是廉价的
impl<T> Clone for MemoryPersistence<T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            data: self.data.clone(),
            running_queues: self.running_queues.clone(),
            pending_queue: self.pending_queue.clone(),
            shard_mask: self.shard_mask.clone(),
            dlq: self.dlq.clone(),
            notify: self.notify.clone(),
        }
    }
}
