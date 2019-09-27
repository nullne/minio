package volume

//
// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"sync"
//
// 	"github.com/minio/minio/pkg/volume/interfaces"
// )
//
// // It is safe for concurrent use by multiple goroutines.
// type Volumes struct {
// 	volumes sync.Map
// 	lock    sync.Map
// }
//
// var (
// 	ErrVolumeIniting = errors.New("file volume is initing now")
// )
//
// func (vs *Volumes) Add(ctx context.Context, p string) error {
// 	// check existence
// 	_, err := vs.Get(p)
// 	if err == nil {
// 		return nil
// 	}
//
// 	_, initing := vs.lock.LoadOrStore(path, struct{}{})
// 	if initing {
// 		return ErrVolumeIniting
// 	}
// 	defer globalFileVolumes.lock.Delete(path)
//
// 	vol, err := NewVolume(ctx, p)
// 	if err != nil {
// 		return err
// 	}
// 	globalFileVolumes.volumes.Store(path, vol)
// 	return nil
// }
//
// func (f *fileVolumes) get(path string) (interfaces.Volume, error) {
// 	vol, ok := globalFileVolumes.volumes.Load(path)
// 	if !ok {
// 		return nil, fmt.Errorf("file volume %s not found", path)
// 	}
// 	return vol.(interfaces.Volume), nil
// }
//
// func (f *fileVolumes) remove(path string) interfaces.Volume {
// 	vol, ok := f.volumes.Load(path)
// 	if !ok {
// 		return nil
// 	}
// 	f.volumes.Delete(path)
// 	return vol.(interfaces.Volume)
// }
//
// func (f *fileVolumes) close() (err error) {
// 	globalFileVolumes.volumes.Range(func(key, value interface{}) bool {
// 		if err = value.(interfaces.Volume).Close(); err != nil {
// 			return false
// 		}
// 		globalFileVolumes.volumes.Delete(key)
// 		return true
// 	})
// 	return
// }
