use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::policy::{WaitContext, WaitStrategy, wait::WaitDecision};

/// 任务起搏器
///
/// 核心职责：负责驱动 Driver 的执行节奏。
/// 它根据 WaitStrategy 的决策，决定是立刻拉取、监听信号还是强制休眠。
pub struct TaskPacemaker<'a> {
    /// 暂停原子
    paused: &'a AtomicBool,
    /// 通知
    notify: &'a Notify,

    /// 关机信号
    shutdown: &'a CancellationToken,

    /// 等待策略
    wait_strategy: Arc<dyn WaitStrategy>,

    /// 连续空闲计数器
    idle_count: u32,

    is_busy: bool,
}

impl<'a> TaskPacemaker<'a> {
    pub fn new(
        paused: &'a AtomicBool,
        notify: &'a Notify,
        shutdown: &'a CancellationToken,
        wait_strategy: Arc<dyn WaitStrategy>,
    ) -> Self {
        Self {
            paused,
            notify,
            shutdown,
            wait_strategy,
            idle_count: 0,
            is_busy: false,
        }
    }

    /// 当成功拉取到任务时调用
    pub fn mark_busy(&mut self) {
        self.is_busy = true;
        self.idle_count = 0;
    }

    /// 当没有拉取到任务时调用
    pub fn mark_idle(&mut self) {
        self.is_busy = false;
        self.idle_count += 1;
    }

    /// 等待下一次动作触发
    pub async fn wait_next(&mut self) -> PacemakerEvent {
        loop {
            // 1. 检查 Shutdown (非阻塞)
            if self.shutdown.is_cancelled() {
                return PacemakerEvent::Shutdown;
            }
            // 2. 检查 Pause
            if self.paused.load(Ordering::Relaxed) {
                tokio::select! {
                    // 监听 Token 取消
                    _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                    _ = self.notify.notified() => continue,
                }
            }
            // 3. Wait Strategy
            let decision = if self.is_busy {
                WaitDecision::Immediate
            } else {
                let ctx = WaitContext::new(self.idle_count);
                self.wait_strategy.make_decision(&ctx)
            };

            match decision {
                WaitDecision::Immediate => return PacemakerEvent::Trigger,

                WaitDecision::Sleep(duration) => {
                    tokio::select! {
                        // 监听 Token 取消
                        _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                        _ = self.notify.notified() => {
                            self.mark_busy();
                            return PacemakerEvent::Trigger;
                        },
                        _ = tokio::time::sleep(duration) => return PacemakerEvent::Trigger,
                    }
                }

                WaitDecision::WaitIndefinitely => {
                    tokio::select! {
                        // 监听 Token 取消
                        _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                        _ = self.notify.notified() => {
                            self.mark_busy();
                            return PacemakerEvent::Trigger;
                        }
                    }
                }
                _ => return PacemakerEvent::Trigger,
            }
        }
    }
}

/// 起搏器产生的事件
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacemakerEvent {
    /// 触发拉取动作
    Trigger,
    /// 系统关闭
    Shutdown,
}
