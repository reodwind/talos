use std::time::Duration;

use crate::policy::{WaitContext, WaitStrategy, wait::WaitDecision};

/// 固定间隔策略
///
/// - 无论系统忙闲，都保持固定的节奏。
/// - 支持配置为“监听通知”或“纯轮询”。
#[derive(Debug, Clone)]
pub struct FixedWait {
    interval: Duration,
    listen: bool, // 是否监听 Pub/Sub
}

impl FixedWait {
    /// 创建默认策略 (混合模式)
    /// - 会监听通知，超时时间为 `millis`
    pub fn new(millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(millis),
            listen: true,
        }
    }

    /// 创建纯轮询策略 (忽略通知)
    /// - 适用于不信任 Pub/Sub 或不需要实时的场景
    pub fn new_pure_polling(millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(millis),
            listen: false,
        }
    }
}

impl WaitStrategy for FixedWait {
    fn make_decision(&self, _ctx: &WaitContext) -> WaitDecision {
        if self.listen {
            WaitDecision::WaitForNotification(self.interval)
        } else {
            WaitDecision::Sleep(self.interval)
        }
    }
}
