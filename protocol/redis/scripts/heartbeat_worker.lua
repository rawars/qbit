if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
redis.call('HSET', KEYS[2], 'heartbeat_at', ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]), ARGV[1])
return 1
