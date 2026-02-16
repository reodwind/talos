-- KEYS[1]: running_zset
-- ARGV[1]: task_id
local running_zset = KEYS[1]
local task_id = ARGV[1]

redis.call('ZREM', running_zset, task_id)
return 0
