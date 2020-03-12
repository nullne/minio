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
	StartTime time.Time                              `json:"startTime"`
	Volumes   map[string]fileVolumeMaintenanceStatus `json:"volumes"`
	Status    string                                 `json:"status"`
	Error     string                                 `json:"error"`
}

const (
	MaintenanceStatusIdle      = "idle"
	MaintenanceStatusRunning   = "running"
	MaintenanceStatusSucceeded = "succeeded"
	MaintenanceStatusFailed    = "failed"
)

const (
	VolumeMaintenanceStatusWaiting    = "waiting"
	VolumeMaintenanceStatusDumping    = "dumping"
	VolumeMaintenanceStatusCompacting = "compacting"
	VolumeMaintenanceStatusCompleted  = "completed"
	VolumeMaintenanceStatusFailed     = "failed"
)

// @TODO finish first then enrich the detail
type fileVolumeMaintenanceStatus struct {
	Status string `json:"status"`
}

func (m *fileVolumeMaintenance) updateVolumes(key, status string) {
	var s fileVolumeMaintenanceStatus
	m.lock.RLock()
	s = m.detail.Volumes[key]
	m.lock.RUnlock()
	s.Status = status
	m.lock.Lock()
	m.detail.Volumes[key] = s
	m.lock.Unlock()
}

func (m *fileVolumeMaintenance) updateError(err error) {
	if err == nil {
		return
	}
	m.lock.Lock()
	m.detail.Error = err.Error()
	m.lock.Unlock()
}

// func (m *fileVolumeMaintenance) reset() {
//     m.StartTime = time.Now()
//     m.cancel = nil
//     m.status = MaintenanceStatusIdle
//     m.Volumes = make(map[string]fileVolumeMaintenanceStatus)
//     close(m.errCh)
//     m.errCh = make(chan error, 1)
// }

const (
	timeLayout string = "15:04:05"
)

func (m *fileVolumeMaintenance) Start(rate float64, timeRange string) error {
	logger.Info("start volume maintenance with params rate: %v, time range: %s", rate, timeRange)
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

	m.lock.Lock()
	defer m.lock.Unlock()
	if m.detail.Status == MaintenanceStatusRunning || m.detail.Status == MaintenanceStatusSucceeded {
		return errUnderMaintenance
	}

	ctx, cancel := context.WithCancel(context.Background())

	// if ok, err := m.checkUnderMaintenance(ctx); err != nil {
	//     return err
	// } else if ok {
	//     return errUnderMaintenance
	// }

	m.detail.Volumes = make(map[string]fileVolumeMaintenanceStatus)
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		m.detail.Volumes[key.(string)] = fileVolumeMaintenanceStatus{
			Status: VolumeMaintenanceStatusWaiting,
		}
		return true
	})

	m.cancel = cancel

	m.wg.Add(1)
	go m.run(ctx, rate, tr)

	m.detail.Status = MaintenanceStatusRunning
	m.detail.StartTime = time.Now()
	m.detail.Error = ""
	return nil
}

// func (m *fileVolumeMaintenance) checkUnderMaintenance(ctx context.Context) (ok bool, err error) {
//     globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
//         select {
//         case <-ctx.Done():
//             err = ctx.Err()
//             return false
//         default:
//         }
//         ok, err = value.(*fv.Volume).UnderMaintenance()
//         if err != nil {
//             return false
//         }
//         return true
//     })
//     return
// }

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

func (m *fileVolumeMaintenance) run(ctx context.Context, rate float64, timeRange []time.Time) {
	defer m.wg.Done()
	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
		wait := waitBetween(timeRange)
		timer := time.NewTimer(wait)
		logger.Info("sleeping for %s", wait.String())
		select {
		case <-ctx.Done():
			m.updateError(ctx.Err())
			return false
		case <-timer.C:
		}
		timer.Stop()

		// dump object list
		// logger.Info("dumping objects list of %s", key.(string))
		// m.updateVolumes(key.(string), VolumeMaintenanceStatusDumping)
		// if err := value.(*fv.Volume).DumpListToMaintain(ctx, rate); err != nil {
		// 	logger.Info("failed to dump objects list of %s: %s", key.(string), err.Error())
		// 	m.updateVolumes(key.(string), VolumeMaintenanceStatusFailed)
		// 	m.updateError(err)
		// 	return false
		// }
		// compact
		logger.Info("compacting  %s", key.(string))
		m.updateVolumes(key.(string), VolumeMaintenanceStatusCompacting)
		if err := value.(*fv.Volume).Maintain(ctx, rate); err != nil {
			logger.Info("failed to compact  %s: %s", key.(string), err.Error())
			m.updateVolumes(key.(string), VolumeMaintenanceStatusFailed)
			m.updateError(err)
			return false
		}
		logger.Info("finish maintaining %s", key.(string))
		// completed
		m.updateVolumes(key.(string), VolumeMaintenanceStatusCompleted)
		return true
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
	logger.Info("finish volume maintenance, related files and directory will be removed")
	m.lock.RLock()
	status := m.detail.Status
	m.lock.RUnlock()

	// cancel the job if it is not finished
	if status == MaintenanceStatusRunning {
		cancel := m.cancel
		cancel()
	}

	// var err error
	// globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
	// 	if e := value.(*fv.Volume).CleanMaintain(); e != nil {
	// 		err = e
	// 		return false
	// 	}
	// 	return true
	// })
	// if err != nil {
	// 	return err
	// }

	m.lock.Lock()
	m.detail.StartTime = time.Time{}
	m.detail.Status = MaintenanceStatusIdle
	m.detail.Error = ""
	m.detail.Volumes = make(map[string]fileVolumeMaintenanceStatus)
	m.lock.Unlock()

	return nil
}

// func (m *fileVolumeMaintenance) Suspend() {
// }

func (m *fileVolumeMaintenance) Status() (detail fileVolumeMaintenanceDetail) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	detail.Status = m.detail.Status

	detail.StartTime = m.detail.StartTime
	if m.detail.Volumes != nil {
		detail.Volumes = make(map[string]fileVolumeMaintenanceStatus, len(m.detail.Volumes))
		for k, v := range m.detail.Volumes {
			detail.Volumes[k] = v
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
