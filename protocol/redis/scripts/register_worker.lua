redis.call('HSET', KEYS[2],
  'id', ARGV[1],
  'instance', ARGV[2],
  'concurrency', ARGV[3],
  'started_at', ARGV[4],
  'heartbeat_at', ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[5])
redis.call('ZADD', KEYS[1], tonumber(ARGV[4]) + tonumber(ARGV[5]), ARGV[1])
return 1
