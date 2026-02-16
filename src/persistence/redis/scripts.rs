use deadpool_redis::redis::Script;

/// Redis Lua 脚本库
///
/// 预加载所有脚本，避免每次调用时重新编译。
#[derive(Debug, Clone)]
pub(super) struct Scripts {
    pub acquire: Script,
    pub heartbeat: Script,
    pub release: Script,
    pub rescue: Script,
}

impl Scripts {
    pub fn new() -> Self {
        Self {
            acquire: Script::new(include_str!("../../scripts/redis_acquire.lua")),
            heartbeat: Script::new(include_str!("../../scripts/redis_heartbeat.lua")),
            release: Script::new(include_str!("../../scripts/redis_release.lua")),
            rescue: Script::new(include_str!("../../scripts/redis_rescue.lua")),
        }
    }
}
