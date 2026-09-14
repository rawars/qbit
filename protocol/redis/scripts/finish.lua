local token = redis.call('GET', KEYS[1])
if not token or token ~= ARGV[2] then return -1 end
local group = redis.call('HGET', KEYS[2], 'group')
if not group then return -2 end

redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[4], ARGV[7])
if redis.call('HGET', KEYS[3], group) == ARGV[7] then
  redis.call('HDEL', KEYS[3], group)
end
local startedAt = tonumber(redis.call('HGET', KEYS[2], 'started_at')) or tonumber(ARGV[4])
local processingMs = math.max(0, tonumber(ARGV[4]) - startedAt)
local retries = tonumber(redis.call('HGET', KEYS[2], 'retries')) or 0
redis.call('HSET', KEYS[2], 'state', ARGV[3], 'finished_at', ARGV[4])
if ARGV[5] ~= '' then redis.call('HSET', KEYS[2], 'error', ARGV[5]) end
redis.call('PEXPIRE', KEYS[2], ARGV[8])

local groupKey = ARGV[1] .. ':group:' .. group .. ':wait'
if redis.call('LLEN', groupKey) > 0 and redis.call('SADD', KEYS[6], group) == 1 then
  redis.call('RPUSH', KEYS[5], group)
  redis.call('ZADD', KEYS[8], 0, 'ready')
end
redis.call('XADD', KEYS[7], 'MAXLEN', '~', ARGV[6], '*',
  'event', ARGV[3], 'job_id', ARGV[7], 'group', group,
  'processing_ms', processingMs)
redis.call('HINCRBY', KEYS[9], ARGV[3], 1)
if ARGV[3] == 'completed' and retries > 0 then
  redis.call('XADD', KEYS[7], 'MAXLEN', '~', ARGV[6], '*',
    'event', 'recovered', 'job_id', ARGV[7], 'group', group)
  redis.call('HINCRBY', KEYS[9], 'recovered', 1)
end
redis.call('HSET', KEYS[9], 'updated_at', ARGV[4])
return 1
