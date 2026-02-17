use std::time::Duration;

use rand::RngExt;

use crate::policy::{WaitContext, WaitStrategy, wait::WaitDecision};

/// 固定间隔策略
///
/// 特性：
/// - 基础间隔固定，但支持随机抖动 (Jitter) 以防止惊群效应。
/// - 使用绝对时间 (Instant) 计算，消除执行耗时带来的漂移。
#[derive(Debug, Clone)]
pub struct FixedWait {
    interval: Duration,
    jitter_factor: f64, // 0.0 ~ 1.0
    listen: bool,
}

impl FixedWait {
    /// 创建标准策略 (混合模式)
    /// - 默认开启 10% 的随机抖动
    pub fn new(millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(millis),
            jitter_factor: 0.1,
            listen: true,
        }
    }

    /// 创建纯轮询策略
    pub fn new_pure_polling(millis: u64) -> Self {
        Self {
            interval: Duration::from_millis(millis),
            jitter_factor: 0.1, // 轮询通常也建议带一点抖动，可视情况调整
            listen: false,
        }
    }
    /// 设置抖动因子
    /// - factor: 0.1 表示在 [0.9, 1.1] 倍之间波动
    pub fn with_jitter(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }
}

impl WaitStrategy for FixedWait {
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision {
        let mut interval = self.interval;
        // 随机抖动 (Jitter)
        if self.jitter_factor > 0.0 {
            let mut rng = rand::rng();
            // 生成一个在 [1.0 - jitter, 1.0 + jitter] 之间的系数
            let factor = rng.random_range((1.0 - self.jitter_factor)..=(1.0 + self.jitter_factor));
            // 使用 f64 乘法调整 Duration
            interval = interval.mul_f64(factor);
        }
        // 计算绝对截止时间
        let deadline = ctx.now_instant + interval;
        if self.listen {
            // 软等待：睡到 deadline，但允许被叫醒
            WaitDecision::WaitForNotification(deadline)
        } else {
            // 硬等待：必须睡到 deadline (对应旧的 Sleep)
            WaitDecision::WaitUntil(deadline)
        }
    }
}
