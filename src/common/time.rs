use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use cron::Schedule;

use crate::common::ScheduleType;

/// 全局统一的时间与调度计算器
pub struct TimeUtils;

impl TimeUtils {
    pub fn now() -> f64 {
        Self::now_f64()
    }

    /// [标准] 获取当前 Unix 时间戳 (秒, 双精度)
    /// 全系统统一使用这个方法获取“现在”，方便未来 Mock 或做时钟偏移
    pub fn now_f64() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs_f64()
    }
    /// 验证调度策略是否合法
    /// 主要用于检查 Cron 表达式格式，防止提交了任务却永远无法执行
    pub fn validate_schedule(schedule: &ScheduleType) -> anyhow::Result<()> {
        match schedule {
            ScheduleType::Once => Ok(()),

            ScheduleType::Delay(_ms) => Ok(()),

            ScheduleType::Cron(expr) => {
                // 尝试解析 Cron，如果失败直接报错
                Schedule::from_str(expr)
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!("Invalid cron expression '{}': {}", expr, e))
            }
        }
    }
    /// 计算循环/下一次执行时间
    pub fn next_recurrence_time(
        schedule: &ScheduleType,
        timezone: Option<&str>,
        base_time: f64,
    ) -> Option<f64> {
        match schedule {
            // Once / Delay: 执行完就结束了，没有“下一次”
            // (除非我们要支持 LoopDelay，但那是另一种 ScheduleType)
            ScheduleType::Once => None,
            ScheduleType::Delay(_) => None,

            // Cron: 继续找下一次
            ScheduleType::Cron(expr) => Self::calculate_cron_next(expr, timezone, base_time),
        }
    }

    /// 计算初始执行时间
    pub fn next_initial_time(
        schedule: &ScheduleType,
        timezone: Option<&str>,
        base_time: f64, // 通常传 now
    ) -> f64 {
        match schedule {
            // Once: 立即执行
            ScheduleType::Once => base_time,

            // Delay: 基准时间 + 延时
            ScheduleType::Delay(ms) => base_time + (*ms as f64 / 1000.0),

            // Cron: 计算基于当前时间的下一次匹配
            ScheduleType::Cron(expr) => {
                Self::calculate_cron_next(expr, timezone, base_time).unwrap_or(base_time)
            }
        }
    }

    /// 内部通用 Cron 计算逻辑
    fn calculate_cron_next(expr: &str, tz_str: Option<&str>, base_time: f64) -> Option<f64> {
        let schedule = Schedule::from_str(expr).ok()?;
        let tz: chrono_tz::Tz = tz_str
            .and_then(|s| s.parse().ok())
            .unwrap_or(chrono_tz::UTC);

        let base_dt = Self::f64_to_datetime(base_time, &tz);

        schedule
            .after(&base_dt)
            .next()
            .map(|dt| dt.with_timezone(&Utc).timestamp_millis() as f64 / 1000.0)
    }

    /// 辅助：f64 -> DateTime
    fn f64_to_datetime<T: chrono::TimeZone>(ts: f64, tz: &T) -> DateTime<T> {
        let secs = ts as i64;
        let nsecs = ((ts - secs as f64) * 1_000_000_000.0) as u32;
        tz.timestamp_opt(secs, nsecs).unwrap()
    }
}
