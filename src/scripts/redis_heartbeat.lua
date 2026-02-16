-- KEYS[1]: running_zset (运行队列)
-- KEYS[2]: meta_key     (任务元数据Key "talos:meta:xxx")
-- ARGV[1]: task_id
-- ARGV[2]: worker_id
-- ARGV[3]: client_epoch (客户端持有的令牌)
-- ARGV[4]: now_score
local running_zset = KEYS[1]
local meta_key = KEYS[2]
local task_id = ARGV[1]
local worker_id = ARGV[2]
local client_epoch = tonumber(ARGV[3])
local now_score = ARGV[4]

-- 1. 检查任务是否存活 (还在 Running 集合里吗？)
local score = redis.call('ZSCORE', running_zset, task_id)
if not score then
    return -1 -- 错误码: 任务不在运行中 (可能已完成或被杀死)
end

-- 2. [Fencing Token Check] 防脑裂检查
local server_epoch = tonumber(redis.call('HGET', meta_key, 'epoch'))

-- 如果 Redis 里的版本比客户端大，说明发生了脑裂/抢占
if server_epoch and server_epoch > client_epoch then
    return -2 -- 错误码: 令牌不匹配 (你已经死了，请立即停止)
end

-- 3. 续约 (更新时间戳)
redis.call('ZADD', running_zset, now_score, task_id)
redis.call('HSET', meta_key, 'updated_at', now_score)

return 0 -- 成功
