pub mod config;
pub mod error;
pub mod model;
pub mod traits;
pub mod time;
pub(crate) mod utils;

// 导出配置
pub use config::SchedulerConfig;

// 导出错误类型
pub use error::{Result, SchedulerError};

// 导出核心模型
pub use model::{ScheduleType, ScheduleType::*, TaskContext, TaskData, TaskState};

// 导出用户需实现的 Trait
pub use traits::{ErasedHandler, SchedulableTask, TaskHandler};

pub use time::TimeUtils;
// 内部工具的快捷访问
pub(crate) use utils::{calculate_backoff, new_task_id};
