use std::time::Duration;

/// 等待决策 (The Decision)
///
/// - 策略层返回给 Driver 的具体行动指令。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitDecision {
    /// [纯轮询 / 屏蔽信号]
    ///
    /// 含义: "不管外面门铃怎么响，我就睡够 duration 这么久再醒。"
    ///
    /// 适用场景:
    /// 1. 防止惊群 (Redis/内存模式都适用)。
    /// 2. 后端完全不支持通知 (如 MySQL)。
    Sleep(Duration),

    /// [监听信号 / 响应式]
    ///
    /// 含义: "我打算睡 duration 这么久，但如果有人按门铃，请立刻叫醒我。"
    ///
    /// 适用场景:
    /// 1. 内存模式 (Memory): 任务一来，立马处理，延迟极低。
    /// 2. Redis 模式: Pub/Sub 通知到来，立马处理。
    WaitForNotification(Duration),

    /// [纯订阅模式] 死等通知
    ///
    /// - 行为: `await notify` (无超时)
    /// - 警告: 如果 Redis 通知丢包，Worker 可能会永远睡死。
    /// - 场景: 极度节省资源，且外部有额外的看门狗（Watchdog）机制。
    WaitIndefinitely,

    /// [急行模式] 立即重试
    ///
    /// - 行为: 不等待，直接进入下一轮循环。
    /// - 场景: 策略层判断当前处于“追赶进度”状态，或者刚刚抢到了任务需要趁热打铁。
    Immediate,
}

/// 等待策略上下文
///
/// - 这是一个“参数包”，包含了所有策略做决策可能需要的运行时信息。
/// - 使用结构体而非直接传参，是为了未来的扩展性。
#[derive(Debug, Clone)]
pub struct WaitContext {
    /// 连续空闲次数
    /// 表示 Driver 已经连续多少次去拉取任务却返回了空。
    /// - 0: 表示刚刚处理完一个任务（忙碌状态）。
    /// - >0: 表示系统处于空闲状态。
    pub idle_count: u32,
}

impl WaitContext {
    pub fn new(idle_count: u32) -> Self {
        Self { idle_count }
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

/// 组合策略：聚合多个策略并取其最保守的决策。
///
/// 原则：
/// 1. 如果有任何策略要求 WaitIndefinitely，则结果为 WaitIndefinitely。
/// 2. 如果多个策略要求 Sleep 或 WaitForNotification，取 Duration 最长的一个。
/// 3. Sleep 的优先级高于 WaitForNotification (因为 Sleep 是强制性的)。
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

impl WaitStrategy for WaitStrategyChain {
    fn make_decision(&self, ctx: &WaitContext) -> WaitDecision {
        // 如果没有策略，默认立即执行
        if self.strategies.is_empty() {
            return WaitDecision::Immediate;
        }
        let mut final_decision = WaitDecision::Immediate;
        for strategy in &self.strategies {
            let current = strategy.make_decision(ctx);
            match (final_decision, current) {
                // 只要有一个策略说“死等”，就进入最高等级的挂起
                (_, WaitDecision::WaitIndefinitely) | (WaitDecision::WaitIndefinitely, _) => {
                    final_decision = WaitDecision::WaitIndefinitely;
                }
                // 比较两个具体的等待时长，取最大值
                (WaitDecision::Sleep(d1), WaitDecision::Sleep(d2)) => {
                    final_decision = WaitDecision::Sleep(d1.max(d2));
                }
                // 如果一个是强制 Sleep，一个是 WaitForNotification，
                // 在时长相近的情况下，优先选择 Sleep (更保守)
                (WaitDecision::Sleep(d1), WaitDecision::WaitForNotification(d2)) => {
                    final_decision = WaitDecision::Sleep(d1.max(d2));
                }
                (WaitDecision::WaitForNotification(d1), WaitDecision::Sleep(d2)) => {
                    final_decision = WaitDecision::Sleep(d1.max(d2));
                }

                (WaitDecision::WaitForNotification(d1), WaitDecision::WaitForNotification(d2)) => {
                    final_decision = WaitDecision::WaitForNotification(d1.max(d2));
                }
                // 只要有一个策略需要等待，就覆盖 Immediate
                (WaitDecision::Immediate, other) => {
                    final_decision = other;
                }
                _ => {}
            }
        }
        final_decision
    }
}
