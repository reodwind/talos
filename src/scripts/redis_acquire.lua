-- KEYS[1]: pending_zset (等待队列)
-- KEYS[2]: running_zset (运行队列)
-- ARGV[1]: meta_prefix  (元数据前缀 "talos:meta:")
-- ARGV[2]: worker_id    (当前节点ID)
-- ARGV[3]: now_score    (当前时间戳)
-- ARGV[4]: batch_size   (抢占数量)
local pending_zset = KEYS[1]
local running_zset = KEYS[2]
local meta_prefix = ARGV[1]
local worker_id = ARGV[2]
local now_score = ARGV[3]
local batch_size = ARGV[4]

-- 1. 获取到期的任务 (score <= now)
local tasks = redis.call('ZRANGEBYSCORE', pending_zset, '-inf', now_score,
                         'LIMIT', 0, batch_size)
local result = {}

for i, task_id in ipairs(tasks) do
    local meta_key = meta_prefix .. task_id

    -- 2. 原子更新 Epoch (Fencing Token)
    -- 这是防脑裂的核心：每次抢占，版本号必须 +1
    local new_epoch = redis.call('HINCRBY', meta_key, 'epoch', 1)

    -- 3. 更新元数据 (归属权转移)
    redis.call('HMSET', meta_key, 'state', 'Running', 'worker_id', worker_id,
               'updated_at', now_score)

    -- 4. 移动集合: Pending -> Running
    -- 原子性保证任务不会丢失
    redis.call('ZREM', pending_zset, task_id)
    redis.call('ZADD', running_zset, now_score, task_id)

    -- 5. 收集结果 [id1, epoch1, id2, epoch2, ...]
    table.insert(result, task_id)
    table.insert(result, tostring(new_epoch))
end

return result
