use std::{collections::HashMap, sync::Arc};

use ahash::RandomState;
use async_trait::async_trait;

use crate::common::{ErasedHandler, SchedulableTask, TaskContext, TaskHandler, model::DynamicJob};

#[derive(Clone)]
pub struct TaskRegistry {
    handlers: Arc<HashMap<String, Box<dyn ErasedHandler>, RandomState>>,
}
impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(HashMap::with_hasher(RandomState::new())),
        }
    }
    /// 注册任务 (Builder 模式)
    pub fn register<H>(mut self, name: &str, handler: H) -> Self
    where
        H: TaskHandler + Clone + 'static,
    {
        if let Some(map) = Arc::get_mut(&mut self.handlers) {
            map.insert(name.to_string(), Box::new(handler));
        }
        self
    }
}
#[async_trait]
impl SchedulableTask<DynamicJob> for TaskRegistry {
    async fn execute(&self, ctx: TaskContext<DynamicJob>) -> anyhow::Result<()> {
        let handlers = self.handlers.clone();
        let job = ctx.data;
        // O(1) 极速查找
        if let Some(handler) = handlers.get(&job.name) {
            // 调用类型擦除后的逻辑
            handler
                .handle_erased(job.payload.args.clone())
                .await
                .map_err(|e| anyhow::Error::msg(e))
        } else {
            let msg = format!("No handler for task: {}", job.name);
            Err(anyhow::Error::msg(msg))
        }
    }
}
