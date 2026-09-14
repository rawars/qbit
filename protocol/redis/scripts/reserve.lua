local base = ARGV[1]
local now = tonumber(ARGV[4])
if redis.call('EXISTS', KEYS[8]) == 1 then
  return {'__qbit_paused__'}
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', now,
  'LIMIT', 0, tonumber(ARGV[6]))

for _, id in ipairs(expired) do
  local jobKey = base .. ':job:' .. id
  local lockKey = jobKey .. ':lock'
  local group = redis.call('HGET', jobKey, 'group')
  if redis.call('EXISTS', lockKey) == 0 then
    redis.call('ZREM', KEYS[5], id)
    if group and redis.call('HGET', KEYS[3], group) == id then
      redis.call('HDEL', KEYS[3], group)
      redis.call('HSET', jobKey, 'state', 'waiting', 'recovered_at', ARGV[4])
      local groupKey = base .. ':group:' .. group .. ':wait'
      redis.call('LPUSH', groupKey, id)
      if redis.call('SADD', KEYS[2], group) == 1 then
        redis.call('RPUSH', KEYS[1], group)
      end
      redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[5], '*',
        'event', 'stalled', 'job_id', id, 'group', group)
      redis.call('HINCRBY', KEYS[7], 'stalled', 1)
      redis.call('HSET', KEYS[7], 'updated_at', ARGV[4])
    end
  else
    local ttl = redis.call('PTTL', lockKey)
    if ttl > 0 then redis.call('ZADD', KEYS[5], now + ttl, id) end
  end
end

local attempts = redis.call('LLEN', KEYS[1])
for _ = 1, attempts do
  local group = redis.call('LPOP', KEYS[1])
  if not group then return nil end
  redis.call('SREM', KEYS[2], group)
  if redis.call('HEXISTS', KEYS[3], group) == 0 then
    local groupKey = base .. ':group:' .. group .. ':wait'
    local id = redis.call('LPOP', groupKey)
    if id then
      local jobKey = base .. ':job:' .. id
      local expiresAt = now + tonumber(ARGV[3])
      redis.call('HSET', KEYS[3], group, id)
      redis.call('ZADD', KEYS[5], expiresAt, id)
      redis.call('SET', jobKey .. ':lock', ARGV[2], 'PX', ARGV[3])
      local attempt = redis.call('HINCRBY', jobKey, 'attempts', 1)
      redis.call('HSET', jobKey, 'state', 'active', 'started_at', ARGV[4])
      local createdAt = tonumber(redis.call('HGET', jobKey, 'created_at')) or now
      local waitMs = math.max(0, now - createdAt)
      redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[5], '*',
        'event', 'active', 'job_id', id, 'group', group, 'wait_ms', waitMs)
      redis.call('HINCRBY', KEYS[7], 'reserved', 1)
      redis.call('HSET', KEYS[7], 'updated_at', ARGV[4])
      if redis.call('LLEN', KEYS[1]) > 0 then
        redis.call('ZADD', KEYS[6], 0, 'ready')
      else
        redis.call('ZREM', KEYS[6], 'ready')
      end
      local values = redis.call('HMGET', jobKey, 'name', 'payload')
      return {id, values[1], group, values[2], attempt}
    end
  end
end
redis.call('ZREM', KEYS[6], 'ready')
return nil
