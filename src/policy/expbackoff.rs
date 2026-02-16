use std::time::Duration;

use crate::policy::{WaitContext, WaitStrategy, wait::WaitDecision};

/// 指数退避策略
///
/// - 随着连续空闲次数增加，等待时间呈指数级增长。
/// - 用于在系统空闲时大幅降低 Redis QPS 消耗。
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    min: Duration,
    max: Duration,
    factor: f64,
    jitter: bool, // 是否开启随机抖动
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
            factor: 2.0, // 默认每次翻倍
            jitter: false,
        }
    }

    /// 开启随机抖动 (Jitter)
    ///
    /// 建议在集群模式下开启，防止所有 Worker 在同一时刻醒来（惊群效应）。
    /// 注意：这需要依赖 `rand` crate。如果未引入 rand，这里仅做标记。
    pub fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    /// 内部计算逻辑
    fn calculate_duration(&self, idle_count: u32) -> Duration {
        if idle_count == 0 {
            return self.min;
        }

        // 1. 指数计算: min * factor^(idle_count - 1)
        // 限制指数最大为 30，防止 overflow
        let exponent = (idle_count - 1).min(30) as i32;
        let mut secs = self.min.as_secs_f64() * self.factor.powi(exponent);

        // 2. 随机抖动 (Jitter)
        // 逻辑: 在 [0.8 * secs, 1.2 * secs] 之间波动
        // 为了代码可编译性，这里使用伪代码注释。
        // 实际项目中请引入 `rand` 并取消注释。
        if self.jitter {
            /*
            let mut rng = rand::thread_rng();
            let random_factor = rng.gen_range(0.8..1.2);
            secs *= random_factor;
            */

            // 简易替代方案: 利用 idle_count 做个伪随机，避免引入 rand 依赖
            // (仅作演示，生产环境建议用 rand)
            let pseudo_random = 1.0 + ((idle_count % 5) as f64 * 0.05); // 1.0 ~ 1.2
            secs *= pseudo_random;
        }

        // 3. 封顶限制
        let duration = Duration::from_secs_f64(secs);
        if duration > self.max {
            self.max
        } else {
            duration
        }
    }
}

impl WaitStrategy for ExponentialBackoff {
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision {
        let duration = self.calculate_duration(ctx.idle_count);

        // 指数退避通常配合“监听通知”使用
        // 意思就是：“虽然我打算睡 30秒，但如果这期间有新任务，请立刻叫醒我”
        WaitDecision::WaitForNotification(duration)
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new(1, 5)
    }
}
