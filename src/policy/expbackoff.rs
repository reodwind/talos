use std::time::Duration;

use rand::RngExt;

use crate::policy::{WaitContext, WaitStrategy, wait::WaitDecision};

/// 指数退避策略 (AWS Full Jitter 模式)
///
/// - 算法: `sleep = random(0, min(cap, base * 2 ** attempt))`
/// - 这种模式在分布式系统中能最大程度地分散 Worker 的请求压力。
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    min: Duration,
    max: Duration,
    factor: f64,
    jitter: bool,
}

impl ExponentialBackoff {
    /// 创建退避策略
    ///
    /// # 参数
    /// - `min_ms`: 最小等待时间 (忙时/初始值)
    /// - `max_ms`: 最大等待时间 (封顶值)
    pub fn new(min_ms: u64, max_ms: u64) -> Self {
        Self {
            min: Duration::from_millis(min_ms),
            max: Duration::from_millis(max_ms),
            factor: 2.0,  // 默认每次翻倍
            jitter: true, // 默认开启 Full Jitter
        }
    }
    /// 设置增长因子
    /// - factor: 1.0 表示线性增长，2.0 表示指数增长
    pub fn with_factor(mut self, factor: f64) -> Self {
        self.factor = factor.max(1.0); // 最小为 1.0
        self
    }
    /// 设置是否启用抖动
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }
}

impl WaitStrategy for ExponentialBackoff {
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision {
        // 1. 忙碌状态 (刚刚处理完任务) -> 立即执行
        if ctx.idle_count == 0 {
            return WaitDecision::Immediate;
        }
        // 2. 计算指数基准值
        // 限制指数最大为 30，防止 u64 溢出
        let exponent = (ctx.idle_count - 1).min(30) as i32;
        let mut duration_micros =
            (self.min.as_micros() as f64 * self.factor.powi(exponent)) as u128;

        // 3. 封顶限制 (Cap)
        let max_micros = self.max.as_micros();
        if duration_micros > max_micros {
            duration_micros = max_micros;
        }
        // 4. 应用 Full Jitter
        if self.jitter {
            let mut rng = rand::rng();
            // 生成一个在 [0, duration_micros] 之间的随机值
            duration_micros = rng.random_range(0..=duration_micros);
            // 兜底：防止睡 0ms 导致 CPU 空转，至少睡 1ms
            if duration_micros < 1000 {
                duration_micros = 1000;
            }
        }
        let duration = Duration::from_micros(duration_micros as u64);
        // 5. 计算绝对截止时间
        let deadline = ctx.now_instant + duration;
        // 指数退避期间，如果有新任务通知，允许被叫醒
        WaitDecision::WaitForNotification(deadline)
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new(10, 5000) // 默认: 10ms ~ 5s
    }
}
