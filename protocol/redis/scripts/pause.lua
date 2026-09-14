local changed = redis.call('SETNX', KEYS[1], ARGV[1])
if changed == 1 then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*',
    'event', 'paused', 'job_id', '', 'group', '')
end
return changed
