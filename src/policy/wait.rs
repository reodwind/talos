use chrono::{DateTime, Utc};
use tokio::time::Instant;

/// 等待决策 (The Decision)
///
/// - 策略层返回给 Driver 的具体行动指令。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitDecision {
    /// [立即执行] (高优)
    ///
    /// 含义: "我现在不想睡，直接干活！"
    ///
    /// - 行为: 直接返回，Driver 立刻尝试拉取任务。
    /// - 优势: 最低延迟，适合追赶进度或抢到任务后趁热打铁。
    /// - 风险: 如果连续多次返回 Immediate，可能会导致 CPU 占用过高（饿死其他任务）。
    /// - 适用场景: 1. 刚处理完一个任务，想继续处理下一个；2. 系统负载较低，想快速响应。
    Immediate,

    /// [协作让权] (低优)
    ///
    /// 含义: "我现在不想睡，但也不想抢占 CPU，先让出给其他任务吧。"
    /// - 行为: `yield_now().await`，让出当前任务的执行权，允许其他任务先运行。
    /// - 优势: 可以降低 CPU 占用，减少饿死风险，同时又不完全放弃对 CPU 的竞争。
    /// - 风险: 如果频繁返回 Yield，可能会导致整体吞吐下降（因为每次都要切换任务）。
    /// - 适用场景: 1. 刚处理完一个任务，想继续处理下一个，但系统负载较高；2. 想在等待期间让其他任务有机会运行，保持系统的整体响应性。
    Yield,

    /// [绝对休眠]
    ///
    /// 含义: "我打算睡到某个具体的时间点，期间不想被打扰。"
    /// - 行为: `sleep_until(instant).await`，完全挂起当前任务，直到指定的时间点。
    /// - 优势: 可以精确控制唤醒时间，适合需要同步外部事件或周期性任务的场景。
    /// - 风险: 如果时间点过远，可能会导致响应变慢；如果系统时间发生变化（如 NTP 调整），可能会影响唤醒行为。
    /// - 适用场景: 1. 想在特定的时间点执行某个操作（如每小时整点）；2. 想同步外部事件（如某个资源的可用时间）；3. 想实现基于时间的轮询（Polling）策略。
    WaitUntil(Instant),

    /// [响应式等待]
    ///
    /// 含义: "我打算睡 duration 这么久，但如果在这期间收到了通知，我就提前醒来。"
    ///
    /// - 行为: `select!` 同时等待 `sleep(duration)` 和 `notified()`，哪个先到就先响应哪个。
    /// - 优势: 兼顾了定时等待和事件驱动的灵活性，适合不确定何时会有新任务到达的场景。
    /// - 风险: 如果 Redis 通知丢包，可能会导致 Worker 无法及时响应；如果 duration 过长，可能会导致响应变慢。
    /// - 适用场景: 1. 想在空闲时等待一段时间，但又不想错过新任务的到来；2. 想实现基于事件的轮询（Event-Driven Polling）策略；3. 想在等待期间保持一定的响应性。
    WaitForNotification(Instant),

    /// [死等]
    ///
    /// 含义: "我现在不想睡，但也不想抢占 CPU，干脆挂起自己，等被外部通知唤醒吧。"
    ///
    /// - 行为: `notified().await`，完全挂起当前任务，直到收到通知。
    /// - 优势: 最节省资源的等待方式，适合完全依赖事件驱动的场景。
    /// - 风险: 如果通知丢包，可能会导致 Worker 无法及时响应；如果系统负载突然增加，可能会错过抢占机会。
    /// - 适用场景: 1. 想完全依赖事件驱动，不想设置任何超时；2. 系统负载较高，想完全避竞争；3. 想实现基于事件的轮询（Event-Driven Polling）策略。
    WaitIndefinitely,
}

/// 等待策略上下文
///
/// - 包含了 Driver 当前的运行状态信息，供策略层决策时参考。
/// - 目前仅包含 `idle_count`，表示 Driver 已经连续多少次去拉取任务却返回了空。
#[derive(Debug, Clone)]
pub struct WaitContext {
    /// 连续空闲次数
    ///
    /// 表示 Driver 已经连续多少次去拉取任务却返回了空。
    /// - 0: 表示刚刚处理完一个任务（忙碌状态）。
    /// - >0: 表示系统处于空闲状态。
    pub idle_count: u32,
    /// [单调时钟] 用于计算相对 Duration 和构建 Tokio Timer
    ///
    /// - 特性：永不倒流，不受 NTP/手动改时间影响
    pub now_instant: Instant,
    /// [现实时钟] 用于日历计算、Cron 解析、时区判断
    ///
    /// - 特性：带日期信息，但可能会发生跳变 (NTP)
    pub now_wall: DateTime<Utc>,
}

impl WaitContext {
    pub fn new(idle_count: u32, now_instant: Instant, now_wall: DateTime<Utc>) -> Self {
        Self {
            idle_count,
            now_instant,
            now_wall,
        }
    }
}

/// 等待策略接口 (The Interface)
///
/// - 决定了 Driver 在什么情况下该等待多久。
pub trait WaitStrategy: Send + Sync + 'static {
    /// 核心决策方法
    ///
    /// # 参数
    /// - `ctx`: 包含当前运行状态的上下文
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision;
}

/// 组合策略链 (Arbitrator)
///
/// 核心职责：解决多个策略之间的冲突。
/// - 例如：一个策略说 Yield，另一个说 WaitUntil(T)，链条必须决定听谁的。
pub struct WaitStrategyChain {
    strategies: Vec<Box<dyn WaitStrategy>>,
}
impl WaitStrategyChain {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    /// 向链中添加一个新策略
    pub fn add<S: WaitStrategy>(mut self, strategy: S) -> Self {
        self.strategies.push(Box::new(strategy));
        self
    }
    /// 检查链中是否有策略
    pub fn is_empty(&self) -> bool {
        self.strategies.is_empty()
    }
}

impl WaitStrategyChain {
    /// 核心逻辑：合并两个决策，返回更“保守”的那一个
    ///
    /// 优先级顺序 (由高到低):
    /// 1. WaitIndefinitely (死等)
    /// 2. WaitUntil (硬等待，取最晚时间)
    /// 3. WaitForNotification (软等待，取最晚时间)
    /// 4. Yield (让权)
    /// 5. Immediate (立即)
    fn merge(a: WaitDecision, b: WaitDecision) -> WaitDecision {
        use WaitDecision::*;

        // 1. [最高优先级] 死等
        // 只要有一个策略要求死等，就必须死等
        if matches!(a, WaitIndefinitely) || matches!(b, WaitIndefinitely) {
            return WaitIndefinitely;
        }

        // 辅助函数：将硬等待转换为绝对时间戳 (Deadline)
        // 辅助：提取 Deadline
        let get_deadline = |d: WaitDecision| -> Option<Instant> {
            match d {
                WaitUntil(t) | WaitForNotification(t) => Some(t),
                _ => None,
            }
        };

        let dead_a = get_deadline(a);
        let dead_b = get_deadline(b);

        // 2. [次高优先级] 都有 Deadline -> 取最晚 (更保守)
        if let (Some(ta), Some(tb)) = (dead_a, dead_b) {
            let max_t = if ta > tb { ta } else { tb };
            // 冲突解决：如果有一方是 "硬等待" (WaitUntil)，结果升级为 "硬等待"
            // (例如：限流器强制要求等到 T，即使空闲策略只要求软等待，也必须硬等)
            let is_hard = matches!(a, WaitUntil(_)) || matches!(b, WaitUntil(_));
            return if is_hard {
                WaitUntil(max_t)
            } else {
                WaitForNotification(max_t)
            };
        }
        // 3. [中优先级] 有 Deadline 胜过 无 Deadline
        if dead_a.is_some() {
            return a;
        }
        if dead_b.is_some() {
            return b;
        }

        // [低优先级] Yield
        // 只要有一个要求 Yield (而另一个是 Immediate)，就 Yield
        if matches!(a, Yield) || matches!(b, Yield) {
            return Yield;
        }

        // 5. [默认] Immediate
        Immediate
    }
}

impl WaitStrategy for WaitStrategyChain {
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision {
        // 如果没有策略，默认立即执行
        if self.strategies.is_empty() {
            return WaitDecision::Immediate;
        }
        let mut final_decision = WaitDecision::Immediate;
        // 遍历所有策略，两两合并决策，最终得到一个综合的决策
        for strategy in &self.strategies {
            let current = strategy.make_decision(ctx);
            final_decision = Self::merge(final_decision, current);
        }
        final_decision
    }
}
