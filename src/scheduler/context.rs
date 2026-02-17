use tokio_util::sync::CancellationToken;

use crate::common::TaskContext;

/// 面向用户的任务上下文
pub struct JobContext {
    inner: TaskContext<Vec<u8>>,
}

impl JobContext {
    pub(crate) fn new(inner: TaskContext<Vec<u8>>) -> Self {
        Self { inner }
    }

    pub fn id(&self) -> &str {
        &self.inner.data.id
    }

    pub fn name(&self) -> &str {
        &self.inner.data.name
    }

    pub fn attempt(&self) -> u32 {
        self.inner.data.attempt
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    pub fn token(&self) -> &CancellationToken {
        &self.inner.token
    }

    /// 后门：如果用户需要直接处理二进制
    pub fn raw_payload(&self) -> &[u8] {
        &self.inner.data.payload
    }
}
