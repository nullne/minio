package cmd

import (
	"bytes"
	"context"
	"encoding/gob"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	healRoutineTaskLength = 10000

	minioHealListPrefix = "heal-list"

	defaultObjectListCheckInterval = 10 * time.Second
)

// healTask represents what to heal along with options
//   path: '/' =>  Heal disk formats along with metadata
//   path: 'bucket/' or '/bucket/' => Heal bucket
//   path: 'bucket/object' => Heal object
type healTask struct {
	path string
	opts madmin.HealOpts
	// Healing response will be sent here
	responseCh chan healResult
}

// healResult represents a healing result with a possible error
type healResult struct {
	result madmin.HealResultItem
	err    error
}

// task buffer pool to write into bucket
// healRoutine receives heal tasks, to heal buckets, objects and format.json
type healRoutine struct {
	endpoint string
	tasks    chan healTask
	object   chan string
	objects  chan []string
	doneCh   chan struct{}
	stock    chan struct{} // indicate that there are object list in bucket to be heald
	wg       sync.WaitGroup
}

func (h *healRoutine) closed() bool {
	select {
	case <-h.doneCh:
		return true
	default:
		return false
	}
}

func (h *healRoutine) close() {
	close(h.doneCh)
	h.wg.Wait()
}

func (h *healRoutine) queueHealObject(bucket, object string) {
	if isMinioMetaBucketName(bucket) {
		return
	}
	h.queueHealTask(healTask{
		path:       pathJoin(bucket, object),
		opts:       madmin.HealOpts{ScanMode: madmin.HealDeepScan, Remove: true},
		responseCh: nil, // log only rather than push into responseCh
	})
	backgroundHealTimes.With(prometheus.Labels{"state": "queued", "status": "finished"}).Inc()
}

func (h *healRoutine) queueHealTask(task healTask) {
	if h.closed() {
		logger.Info("failed to queue object %s into the heal queue: heal routine has been closed", task.path)
		return
	}

	select {
	case h.tasks <- task:
		return
	default:
	}

	select {
	case h.tasks <- task:
	case h.object <- task.path:
	default:
		// do nothing except logging
		backgroundHealTimes.With(prometheus.Labels{"state": "queued", "status": "discarded"}).Inc()
		logger.Info("failed to push object %s into the heal queue", task.path)
	}
}

func (h *healRoutine) readFromBucket() {
	defer h.wg.Done()
	list, more, err := bgReadObjectList(h.endpoint)
	if err != nil {
		logger.Info("failed to read object list from bucket", err.Error())
		return
	}
	for _, obj := range list {
		h.queueHealTask(healTask{
			path:       obj,
			opts:       madmin.HealOpts{ScanMode: madmin.HealDeepScan},
			responseCh: nil, // log only rather than push into responseCh
		})
	}

	if !more {
		return
	}

	select {
	case h.stock <- struct{}{}:
	default:
	}
}

func (h *healRoutine) storeObjectsWorker() {
	defer h.wg.Done()
	for list := range h.objects {
		if err := bgStoreObjectList(h.endpoint, list); err != nil {
			logger.Info("failed to store the healing list %v: %s", list, err.Error())
		}
		backgroundHealTimes.With(prometheus.Labels{"state": "queued", "status": "recorded"}).Add(float64(len(list)))
		select {
		case h.stock <- struct{}{}:
		default:
		}
	}
}

func (h *healRoutine) storeObjectWorker() {
	defer h.wg.Done()
	defer close(h.objects)
	list := make([]string, healRoutineTaskLength)
	i := 0
Outer:
	for {
		if i > 0 {
			// there are unhandled objects
			select {
			case obj := <-h.object:
				list[i] = obj
				i += 1
			case h.tasks <- healTask{
				path:       list[i-1],
				opts:       madmin.HealOpts{ScanMode: madmin.HealDeepScan},
				responseCh: nil, // log only rather than push into responseCh
			}:
				i -= 1
			case <-h.doneCh:
				break Outer
			}
		} else {
			select {
			case obj := <-h.object:
				list[i] = obj
				i += 1
			case <-h.doneCh:
				break Outer
			}
		}
		if i >= healRoutineTaskLength {
			h.objects <- list
			list = make([]string, healRoutineTaskLength)
			i = 0
		}
	}

	list = list[:i]

	close(h.tasks)
	for task := range h.tasks {
		list = append(list, task.path)
	}

	close(h.object)
	for obj := range h.object {
		list = append(list, obj)
	}
	h.objects <- list
}

// Wait for heal requests and process them
func (h *healRoutine) run() {
	defer logger.Info("heal routine exited")
	defer h.wg.Done()
	allOnline := false
	ctx := context.Background()
	ticker := time.NewTicker(defaultObjectListCheckInterval)
	defer ticker.Stop()
	for {
		if !globalNodeMonitor.AllOnline() {
			allOnline = false
			select {
			case <-time.After(defaultMonitorConnectEndpointInterval):
			case <-h.doneCh:
				return
			}
			continue
		}
		// wait for three more defaultMonitorConnectEndpointInterval when comming back online just now
		if !allOnline {
			select {
			case <-time.After(3 * defaultMonitorConnectEndpointInterval):
			case <-h.doneCh:
				return
			}
		}
		allOnline = true
		select {
		case task, ok := <-h.tasks:
			if !ok {
				return
			}
			if !globalNodeMonitor.AllOnline() {
				h.queueHealTask(task)
				break
			}
			if globalHTTPServer != nil {
				// Wait at max 1 minute for an inprogress request
				// before proceeding to heal
				waitCount := 60
				// Any requests in progress, delay the heal.
				for globalHTTPServer.GetRequestCount() > 2 && waitCount > 0 {
					waitCount--
					time.Sleep(1 * time.Second)
				}
			}

			var res madmin.HealResultItem
			var err error
			bucket, object := urlPath2BucketObjectName(task.path)
			switch {
			case bucket == "" && object == "":
				res, err = bgHealDiskFormat(ctx, task.opts)
			case bucket != "" && object == "":
				res, err = bgHealBucket(ctx, bucket, task.opts)
			case bucket != "" && object != "":
				res, err = bgHealObject(ctx, bucket, object, task.opts)
			}
			if err == nil {
				backgroundHealTimes.With(prometheus.Labels{"state": "healed", "status": "succeeded"}).Inc()
			} else {
				backgroundHealTimes.With(prometheus.Labels{"state": "healed", "status": "failed"}).Inc()
			}
			if task.responseCh != nil {
				task.responseCh <- healResult{result: res, err: err}
			} else {
				if before, after := res.GetOfflineCounts(); before != 0 || after != 0 {
					h.queueHealTask(task)
					allOnline = false
					break
				}
				// log here
				if err != nil {
					logger.Info("[healRoutine]failed to heal %s/%s: %v", bucket, object, err)
				}
			}
		case <-ticker.C:
			if len(h.tasks) != 0 {
				continue
			}
			select {
			case <-h.stock:
				h.wg.Add(1)
				go h.readFromBucket()
			default:
			}
		case <-h.doneCh:
			return
		}
	}
}

func initHealRoutine(endpoint string) *healRoutine {
	h := healRoutine{
		endpoint: endpoint,
		tasks:    make(chan healTask, healRoutineTaskLength),
		doneCh:   make(chan struct{}),
		objects:  make(chan []string, 2),
		object:   make(chan string, healRoutineTaskLength),
		stock:    make(chan struct{}, 1),
	}
	h.stock <- struct{}{}
	return &h
}

func initBackgroundHealing(endpoints EndpointList) {
	var endpoint string
	for _, e := range endpoints {
		if e.IsLocal {
			endpoint = e.Host + e.Path
			break
		}
	}
	healBg := initHealRoutine(endpoint)

	healBg.wg.Add(1)
	go healBg.run()

	healBg.wg.Add(1)
	go healBg.storeObjectsWorker()

	healBg.wg.Add(1)
	go healBg.storeObjectWorker()

	globalBackgroundHealing = healBg
}

// bgHealDiskFormat - heals format.json, return value indicates if a
// failure error occurred.
func bgHealDiskFormat(ctx context.Context, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return madmin.HealResultItem{}, errServerNotInitialized
	}

	res, err := objectAPI.HealFormat(ctx, opts.DryRun)

	// return any error, ignore error returned when disks have
	// already healed.
	if err != nil && err != errNoHealRequired {
		return madmin.HealResultItem{}, err
	}

	// Healing succeeded notify the peers to reload format and re-initialize disks.
	// We will not notify peers if healing is not required.
	if err == nil {
		for _, nerr := range globalNotificationSys.ReloadFormat(opts.DryRun) {
			if nerr.Err != nil {
				logger.GetReqInfo(ctx).SetTags("peerAddress", nerr.Host.String())
				logger.LogIf(ctx, nerr.Err)
			}
		}
	}

	return res, nil
}

// bghealBucket - traverses and heals given bucket
func bgHealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return madmin.HealResultItem{}, errServerNotInitialized
	}

	return objectAPI.HealBucket(ctx, bucket, opts.DryRun, opts.Remove)
}

// bgHealObject - heal the given object and record result
func bgHealObject(ctx context.Context, bucket, object string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return madmin.HealResultItem{}, errServerNotInitialized
	}
	return objectAPI.HealObject(ctx, bucket, object, opts.DryRun, opts.Remove, opts.ScanMode, nil)
}

func bgReadObjectList(endpoint string) ([]string, bool, error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, false, errServerNotInitialized
	}
	list, err := objectAPI.ListObjects(context.Background(), minioMetaBucket, path.Join(minioHealListPrefix, endpoint), "", "", 2)
	if err != nil {
		return nil, false, err
	}
	if len(list.Objects) == 0 {
		return nil, false, nil
	}

	buf := bytes.Buffer{}
	if err := objectAPI.GetObject(
		context.Background(),
		list.Objects[0].Bucket,
		list.Objects[0].Name,
		0,
		list.Objects[0].Size,
		&buf,
		list.Objects[0].ETag,
		ObjectOptions{},
	); err != nil {
		return nil, false, err
	}

	var objects []string
	if err := gob.NewDecoder(&buf).Decode(&objects); err != nil {
		return nil, false, err
	}

	if err := objectAPI.DeleteObject(
		context.Background(),
		list.Objects[0].Bucket,
		list.Objects[0].Name,
	); err != nil {
		return nil, false, err
	}
	return objects, len(list.Objects) == 2, nil
}

func bgStoreObjectList(endpoint string, list []string) error {
	if len(list) == 0 {
		return nil
	}
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(list); err != nil {
		return err
	}
	data := buf.Bytes()

	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data), int64(len(data)))
	if err != nil {
		return err
	}
	object := path.Join(minioHealListPrefix, endpoint, mustGetUUID())
	_, err = objectAPI.PutObject(context.Background(), minioMetaBucket, object, NewPutObjReader(hashReader, nil, nil), ObjectOptions{})
	return err
}
