package volume

import (
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
)

type job struct {
	name   string
	fn     func() error
	result chan error // make sure the capacity is one which won't block
	expire time.Time
}

var globalBackupQueue = make(chan job, 100)
var once = sync.Once{}

func initJob() {
	once.Do(func() {
		go func() {
			for job := range globalBackupQueue {
				logger.Info("start run job: %s", job.name)
				if time.Now().After(job.expire) {
					logger.Info("job %s was expired, skipped", job.name)
					continue
				}
				err := job.fn()
				job.result <- err
				logger.Info("finish job %s: %v", job.name, err)
			}
		}()
	})
}
