package qbit

import (
	protocol "github.com/rawars/qbit/protocol/redis/scripts"
	"github.com/redis/go-redis/v9"
)

var (
	addScript              = redis.NewScript(protocol.Add)
	reserveScript          = redis.NewScript(protocol.Reserve)
	finishScript           = redis.NewScript(protocol.Finish)
	retryScript            = redis.NewScript(protocol.Retry)
	renewScript            = redis.NewScript(protocol.Renew)
	registerWorkerScript   = redis.NewScript(protocol.RegisterWorker)
	heartbeatWorkerScript  = redis.NewScript(protocol.HeartbeatWorker)
	unregisterWorkerScript = redis.NewScript(protocol.UnregisterWorker)
	pauseScript            = redis.NewScript(protocol.Pause)
	resumeScript           = redis.NewScript(protocol.Resume)
)
