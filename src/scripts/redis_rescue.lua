-- KEYS[1]: pending_zset
-- KEYS[2]: running_zset
-- ARGV[1]: timeout_threshold (score 界限)
-- ARGV[2]: now_ts (重置后的 score)
-- ARGV[3]: limit

local pending = KEYS[1]
local running = KEYS[2]
local threshold = ARGV[1]
local now = ARGV[2]
local limit = ARGV[3]

-- 找出所有 "最后心跳 < 阈值" 的任务
local zombies = redis.call('ZRANGEBYSCORE', running, '-inf', threshold, 'LIMIT', 0, limit)

if #zombies > 0 then
    for _, id in ipairs(zombies) do
        -- 移出运行集
        redis.call('ZREM', running, id)
        -- 移回等待集 (重置时间为当前，使其能立即被消费)
        redis.call('ZADD', pending, now, id)
    end
end

return zombies