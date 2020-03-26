package cmd

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	fv "github.com/minio/minio/cmd/volume"
)

var (
	errUnderMaintenance = errors.New("has already been under maintenance")
	errNoMaintenance    = errors.New("no maintenance")
)

// init status
func init() {
	globalFileVolumeMaintenance.detail.Status = MaintenanceStatusIdle
}

type fileVolumeMaintenance struct {
	detail fileVolumeMaintenanceDetail
	lock   sync.RWMutex
	wg     sync.WaitGroup

	// errCh  chan error
	cancel context.CancelFunc
}

type fileVolumeMaintenanceDetail struct {
	StartTime time.Time                               `json:"startTime"`
	Volumes   map[string]*fileVolumeMaintenanceStatus `json:"volumes"`
	Status    string                                  `json:"status"`
	Error     string                                  `json:"error"`
}

type fileVolumeMaintenanceStatus struct {
	Status string `json:"status"`
	Log    string `json:"log"`
}

func (v *fileVolumeMaintenanceStatus) updateStatus(s string) {
	v.Status = s

}
func (v *fileVolumeMaintenanceStatus) updateLog(s string) {
	v.Log = s
}

const (
	MaintenanceStatusIdle      = "idle"
	MaintenanceStatusRunning   = "running"
	MaintenanceStatusSucceeded = "succeeded"
	MaintenanceStatusFailed    = "failed"
)

func (m *fileVolumeMaintenance) updateVolumeStatus(key string, ch chan string) {
	defer m.wg.Done()
	for status := range ch {
		m.lock.Lock()
		m.detail.Volumes[key].updateLog(status)
		m.lock.Unlock()
	}
}

func (m *fileVolumeMaintenance) updateError(err error) {
	if err == nil {
		return
	}
	m.lock.Lock()
	m.detail.Error = err.Error()
	m.lock.Unlock()
}

const (
	timeLayout string = "15:04:05"
)

func (m *fileVolumeMaintenance) Start(rate float64, timeRange string, drives, buckets []string) error {
	logger.Info("start volume maintenance with params rate: %v, time range: %s, drives: %v buckets: %v", rate, timeRange, drives, buckets)
	var tr []time.Time
	if ss := strings.Split(timeRange, "-"); len(ss) == 2 {
		start, err := time.Parse(timeLayout, ss[0])
		if err != nil {
			return err
		}
		end, err := time.Parse(timeLayout, ss[1])
		if err != nil {
			return err
		}
		tr = append(tr, start, end)
	}
	for i, d := range drives {
		if strings.HasSuffix(d, "/") {
			continue
		}
		drives[i] += "/"
	}
	for i, b := range buckets {
		buckets[i] = strings.Trim(b, "/")
	}

	filter := func(s string) bool {
		bb, dd := false, false
		if s != "/" {
			s = strings.TrimRight(s, "/")
		}
		if len(buckets) != 0 {
			for _, b := range buckets {
				if strings.HasSuffix(s, b) {
					bb = true
					break
				}
			}
		} else {
			bb = true
		}

		if len(drives) != 0 {
			for _, d := range drives {
				if strings.HasPrefix(s, d) {
					dd = true
					break
				}
			}
		} else {
			dd = true
		}

		return bb && dd
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	if m.detail.Status == MaintenanceStatusRunning || m.detail.Status == MaintenanceStatusSucceeded {
		return errUnderMaintenance
	}

	m.detail.Volumes = make(map[string]*fileVolumeMaintenanceStatus)
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		if !filter(key.(string)) {
			return true
		}
		m.detail.Volumes[key.(string)] = &fileVolumeMaintenanceStatus{Status: "waiting"}
		return true
	})

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.detail.Status = MaintenanceStatusRunning
	m.detail.StartTime = time.Now()
	m.detail.Error = ""

	m.wg.Add(1)
	go m.run(ctx, rate, tr, filter)
	return nil
}

func waitBetween(tr []time.Time) time.Duration {
	if len(tr) != 2 {
		return 0
	}
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), tr[0].Hour(), tr[0].Minute(), tr[0].Second(), tr[0].Nanosecond(), now.Location())
	end := time.Date(now.Year(), now.Month(), now.Day(), tr[1].Hour(), tr[1].Minute(), tr[1].Second(), tr[1].Nanosecond(), now.Location())

	if now.Before(start) {
		return start.Sub(now)
	} else if now.After(end) {
		return start.Add(time.Hour * 24).Sub(now)
	} else {
		return 0
	}
}

func (m *fileVolumeMaintenance) run(ctx context.Context, rate float64, timeRange []time.Time, filter func(string) bool) {
	defer m.wg.Done()
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		if !filter(key.(string)) {
			return true
		}
		wait := waitBetween(timeRange)
		timer := time.NewTimer(wait)
		defer timer.Stop()
		logger.Info("sleeping for %s", wait.String())
		select {
		case <-ctx.Done():
			m.updateError(ctx.Err())
			return false
		case <-timer.C:
		}

		m.detail.Volumes[key.(string)].updateStatus("running")
		// dump object list
		ch := make(chan string, 5)
		m.wg.Add(1)
		go m.updateVolumeStatus(key.(string), ch)
		err := value.(*fv.Volume).Maintain(ctx, rate, ch)
		m.updateError(err)
		m.lock.Lock()
		defer m.lock.Unlock()
		if err != nil {
			logger.Info("failed to maintaining  %s: %s", key.(string), err.Error())
			m.detail.Volumes[key.(string)].updateStatus("failed")
			return false
		} else {
			logger.Info("finish maintaining %s", key.(string))
			m.detail.Volumes[key.(string)].updateStatus("completed")
			return true
		}
	})

	m.lock.Lock()
	if m.detail.Error == "" {
		m.detail.Status = MaintenanceStatusSucceeded
	} else {
		m.detail.Status = MaintenanceStatusFailed
	}
	m.lock.Unlock()
}

// make sure pull the last status before invoking Finish
func (m *fileVolumeMaintenance) Finish() error {
	logger.Info("finishing volume maintenance, related files and directory will be removed")
	m.lock.RLock()
	cancel := m.cancel
	m.lock.RUnlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	m.lock.Lock()
	m.detail.StartTime = time.Time{}
	m.detail.Status = MaintenanceStatusIdle
	m.detail.Error = ""
	m.detail.Volumes = make(map[string]*fileVolumeMaintenanceStatus)
	m.lock.Unlock()

	return nil
}

func (m *fileVolumeMaintenance) Status() (detail fileVolumeMaintenanceDetail) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	detail.Status = m.detail.Status

	detail.StartTime = m.detail.StartTime
	if m.detail.Volumes != nil {
		detail.Volumes = make(map[string]*fileVolumeMaintenanceStatus, len(m.detail.Volumes))
		for k, v := range m.detail.Volumes {
			status := fileVolumeMaintenanceStatus{
				Status: v.Status,
				Log:    v.Log,
			}
			detail.Volumes[k] = &status
		}
	}
	detail.Error = m.detail.Error
	return detail
}

func (m *fileVolumeMaintenance) Close() {
	m.lock.RLock()
	cancel := m.cancel
	m.lock.RUnlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}
