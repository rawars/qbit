local id = ARGV[3]
if id == '' then
  id = 'auto-' .. tostring(redis.call('INCR', KEYS[1]))
end

local jobKey = ARGV[1] .. ':job:' .. id
if redis.call('EXISTS', jobKey) == 1 then
  local existing = redis.call('HMGET', jobKey, 'group', 'name', 'payload')
  return {id, existing[1], 1, existing[2], existing[3]}
end

local group = ARGV[4]
if group == '' then group = '__job__:' .. id end
local groupKey = ARGV[1] .. ':group:' .. group .. ':wait'
redis.call('HSET', jobKey, 'id', id, 'name', ARGV[2], 'group', group,
  'payload', ARGV[5], 'state', 'waiting', 'created_at', ARGV[6])
redis.call('RPUSH', groupKey, id)
if redis.call('HEXISTS', KEYS[4], group) == 0 then
  if redis.call('SADD', KEYS[3], group) == 1 then
    redis.call('RPUSH', KEYS[2], group)
    redis.call('ZADD', KEYS[6], 0, 'ready')
  end
end
redis.call('XADD', KEYS[5], 'MAXLEN', '~', ARGV[7], '*',
  'event', 'waiting', 'job_id', id, 'group', group)
redis.call('HINCRBY', KEYS[7], 'published', 1)
redis.call('HSETNX', KEYS[7], 'initialized_at', ARGV[6])
redis.call('HSET', KEYS[7], 'updated_at', ARGV[6])
return {id, group, 0, ARGV[2], ARGV[5]}
