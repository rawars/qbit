local token = redis.call('GET', KEYS[1])
if not token or token ~= ARGV[2] then return -1 end
local group = redis.call('HGET', KEYS[2], 'group')
if not group then return -2 end

redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[4], ARGV[6])
if redis.call('HGET', KEYS[3], group) == ARGV[6] then
  redis.call('HDEL', KEYS[3], group)
end

local startedAt = tonumber(redis.call('HGET', KEYS[2], 'started_at')) or tonumber(ARGV[3])
local processingMs = math.max(0, tonumber(ARGV[3]) - startedAt)
redis.call('HSET', KEYS[2], 'state', 'waiting', 'failed_at', ARGV[3],
  'retried_at', ARGV[3], 'error', ARGV[4])
redis.call('HINCRBY', KEYS[2], 'retries', 1)

local groupKey = ARGV[1] .. ':group:' .. group .. ':wait'
redis.call('LPUSH', groupKey, ARGV[6])
if redis.call('SADD', KEYS[6], group) == 1 then
  redis.call('RPUSH', KEYS[5], group)
  redis.call('ZADD', KEYS[8], 0, 'ready')
end

redis.call('XADD', KEYS[7], 'MAXLEN', '~', ARGV[5], '*',
  'event', 'failed', 'job_id', ARGV[6], 'group', group,
  'processing_ms', processingMs)
redis.call('XADD', KEYS[7], 'MAXLEN', '~', ARGV[5], '*',
  'event', 'retried', 'job_id', ARGV[6], 'group', group)
redis.call('HINCRBY', KEYS[9], 'failed', 1)
redis.call('HINCRBY', KEYS[9], 'retried', 1)
redis.call('HINCRBY', KEYS[10], 'failed', 1)
redis.call('HINCRBY', KEYS[10], 'retried', 1)
redis.call('HINCRBY', KEYS[10], 'processing_total_ms', processingMs)
redis.call('HINCRBY', KEYS[10], 'processing_samples', 1)
redis.call('PEXPIRE', KEYS[10], ARGV[7])
redis.call('HSETNX', KEYS[9], 'aggregates_initialized_at', ARGV[3])
redis.call('HSET', KEYS[9], 'updated_at', ARGV[3])
return 1
