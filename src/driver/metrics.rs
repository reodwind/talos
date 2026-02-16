use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use serde::Serialize;

/// 驱动器核心指标
// 使用 Atomic 保证高并发下的计数性能
#[derive(Debug, Default, Serialize)]
pub struct DriverMetrics {
    // --- 瞬时状态 (Gauges) ---
    /// - 当前正在执行的任务数 (活跃并发数)
    /// - 对应: Active Concurrency / Running Tasks
    pub active_tasks: AtomicUsize,

    // --- 累积计数 (Counters) ---
    /// 历史总成功任务数
    pub total_success: AtomicU64,

    /// 历史总失败任务数
    pub total_failure: AtomicU64,

    /// 历史总重试次数
    pub total_retries: AtomicU64,
}

impl DriverMetrics {
    /// 增加活跃数 (开始做任务)
    pub fn inc_active(&self) {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// 减少活跃数 (任务结束)
    pub fn dec_active(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// 记录成功
    pub fn inc_success(&self) {
        self.total_success.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录失败
    pub fn inc_failure(&self) {
        self.total_failure.fetch_add(1, Ordering::Relaxed);
    }
}
