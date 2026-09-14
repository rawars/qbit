local changed = redis.call('DEL', KEYS[1])
if changed == 1 then
  if redis.call('LLEN', KEYS[2]) > 0 then
    redis.call('ZADD', KEYS[3], 0, 'ready')
  end
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[1], '*',
    'event', 'resumed', 'job_id', '', 'group', '')
end
return changed
