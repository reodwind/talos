use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use tokio::{sync::Notify, time::Instant};
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
        self.idle_count = self.idle_count.saturating_add(1);
    }
    /// 暴露状态给 Metrics
    pub fn idle_count(&self) -> u32 {
        self.idle_count
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
                    _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
                }
            }
            // 捕获时间快照 (仅用于传给策略)
            let now_instant = Instant::now();
            let now_wall = Utc::now();

            // 3. 获取策略决策
            let ctx = WaitContext::new(self.idle_count, now_instant, now_wall);
            let decision = self.wait_strategy.make_decision(&ctx);

            // 执行决策
            match decision {
                WaitDecision::Immediate => return PacemakerEvent::Trigger,
                WaitDecision::Yield => {
                    tokio::task::yield_now().await;
                    return PacemakerEvent::Trigger;
                }
                // 硬等待：不管有没有信号，必须睡到这个点
                WaitDecision::WaitUntil(deadline) => {
                    tokio::select! {
                        _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                        _ = tokio::time::sleep_until(deadline) => return PacemakerEvent::Trigger,
                    }
                }
                // 软等待：允许被 Notify 唤醒
                WaitDecision::WaitForNotification(deadline) => {
                    tokio::select! {
                        _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                        _ = self.notify.notified() => return PacemakerEvent::Trigger,
                        _ = tokio::time::sleep_until(deadline) => return PacemakerEvent::Trigger,
                    }
                }
                WaitDecision::WaitIndefinitely => {
                    tokio::select! {
                        _ = self.shutdown.cancelled() => return PacemakerEvent::Shutdown,
                        _ = self.notify.notified() => return PacemakerEvent::Trigger,
                    }
                }
            }
        }
    }
}

/// 起搏器产生的事件
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacemakerEvent {
    /// [触发] 有任务或到了轮询时间，请立即去拉取
    Trigger,
    /// [空闲] 虽然没任务，但 Pacemaker 醒来告诉你一声，继续保持等待状态
    Idle,
    /// [重载] 配置已变更，请 Driver 重新加载配置
    Reload,
    /// [关闭] 系统停机
    Shutdown,
}
