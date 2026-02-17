use nanoid::nanoid;
use rand::RngExt;
use std::time::Duration;

// ==========================================
// 2. ID 生成工具 (Identity Utilities)
// ==========================================

/// 生成全局唯一的任务 ID (NanoID)
///
/// 使用 NanoID 替换 UUID。
/// - 默认长度: 21 字符
/// - 字符集: A-Za-z0-9_-
/// - 优势: 比 UUID 更短，URL 友好，生成速度更快。
#[inline]
pub fn new_task_id() -> String {
    // 使用默认配置生成 21 位 ID
    // 碰撞概率极低 (需要运行数百年才会发生碰撞)
    // nanoid!()

    // 如果你想要自定义字符集 (例如不包含 - 和 _ 以便双击选中)，可以使用:
    const ALPHABET: [char; 62] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
    nanoid!(5, &ALPHABET)
}

/// 获取当前机器的主机名
///
/// 用于生成默认的 `NodeId`。
pub fn get_hostname() -> String {
    hostname::get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|_| format!("Node_{}", nanoid!(5)))
}

// ==========================================
// 3. 算法工具 (Algorithmic Utilities)
// ==========================================

/// 计算指数退避时间 (Exponential Backoff with Jitter)
///
/// - attempt: 当前重试次数 (1, 2, 3...)
/// - base_delay: 基础延迟秒数 (例如 1.0s)
/// - max_delay: 最大延迟秒数 (例如 3600.0s)
pub fn calculate_backoff(attempt: u32, base_delay: f64, max_delay: f64) -> Duration {
    let mut rng = rand::rng(); // rand 0.8 标准写法

    // 1. 计算指数部分: base * 2^(attempt-1)
    let exponent = 2u32.saturating_pow(attempt.saturating_sub(1));
    let mut backoff = base_delay * (exponent as f64);

    // 2. 限制最大值 (Cap)
    if backoff > max_delay {
        backoff = max_delay;
    }

    // 3. 添加抖动 (Full Jitter)
    // 随机取 [0, backoff] 之间的值
    // 这比 Equal Jitter (backoff/2 + rand(0, backoff/2)) 更能平滑负载
    let jittered_backoff = rng.random_range(0.0..=backoff);

    // 4. 保证最小延迟 (防止 0ms)
    let final_secs = jittered_backoff.max(0.1); // 至少 100ms

    Duration::from_secs_f64(final_secs)
}
