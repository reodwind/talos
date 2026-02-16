use super::scripts::Scripts;
use crate::common::config::SchedulerConfig;
use deadpool_redis::{Config, Pool, Runtime, redis};
use redis as redis_driver;
use std::sync::Arc;

/// Redis 持久化实现
///
/// 包含 Redis 客户端连接池和预编译的 Lua 脚本。
#[derive(Debug, Clone)]
pub struct RedisPersistence {
    /// Redis 客户端
    pub(super) client: redis_driver::Client,
    /// Redis 客户端 连接池
    pub(super) pool: Pool,

    /// Lua 脚本库
    pub(super) scripts: Arc<Scripts>,

    /// Key 前缀 (命名空间)
    /// e.g. "talos" -> "talos:pending", "talos:data:xyz"
    pub(super) namespace: String,
}

impl RedisPersistence {
    /// 创建新实例
    pub fn new(config: &SchedulerConfig, url: &str) -> anyhow::Result<Self> {
        let mut cfg = Config::from_url(url);
        #[cfg(feature = "distributed")]
        {
            cfg.pool = Some(deadpool_redis::PoolConfig::new(
                config.cluster.redis_pool_size,
            ));
        }
        let client = redis_driver::Client::open(url)?;
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

        Ok(Self {
            pool,
            client,
            scripts: Arc::new(Scripts::new()),
            namespace: config.namespace.clone(),
        })
    }

    // --- Key 生成辅助函数 ---

    /// 获取存储任务数据的 Key (String JSON)
    pub(super) fn key_data(&self, id: &str) -> String {
        format!("{}:data:{}", self.namespace, id)
    }

    /// 获取任务元数据的 Key (Hash)
    pub(super) fn key_meta_prefix(&self) -> String {
        format!("{}:meta:", self.namespace) // 注意末尾的冒号
    }

    /// 获取单个任务元数据的 Key
    pub(super) fn key_meta(&self, id: &str) -> String {
        format!("{}:meta:{}", self.namespace, id)
    }

    /// 获取 Pending 队列 Key (ZSET)
    pub(super) fn key_pending(&self) -> String {
        format!("{}:pending", self.namespace)
    }

    /// 获取 Running 队列 Key (ZSET)
    pub(super) fn key_running(&self) -> String {
        format!("{}:running", self.namespace)
    }

    /// 获取 Pub/Sub 通知频道 Key
    pub(super) fn key_notify(&self) -> String {
        format!("{}:notify", self.namespace)
    }

    /// 获取死信队列 Key (List/Hash)
    pub(super) fn key_dlq(&self) -> String {
        format!("{}:dlq", self.namespace)
    }
}
