package volume

import (
	"context"
	"errors"
	"sync"

	"github.com/minio/minio/pkg/volume/interfaces"
)

// It is safe for concurrent use by multiple goroutines.
type Volumes struct {
	volumes sync.Map
	lock    sync.Map

	newVolume func(context.Context, string) (interfaces.Volume, error)
}

var (
	ErrVolumeIniting  = errors.New("file volume is initing now")
	ErrVolumeNotFound = errors.New("file volume not found")
)

func NewVolumes(newVolume func(context.Context, string) (interfaces.Volume, error)) *Volumes {
	return &Volumes{newVolume: newVolume}
}

func (vs *Volumes) Add(ctx context.Context, p string) error {
	_, err := vs.Get(p)
	if err == nil {
		return nil
	}

	_, initing := vs.lock.LoadOrStore(p, struct{}{})
	if initing {
		return ErrVolumeIniting
	}
	defer vs.lock.Delete(p)

	vol, err := vs.newVolume(ctx, p)
	if err != nil {
		return err
	}
	vs.volumes.Store(p, vol)
	return nil
}

func (vs *Volumes) Get(p string) (interfaces.Volume, error) {
	vol, ok := vs.volumes.Load(p)
	if !ok {
		return nil, ErrVolumeNotFound
	}
	return vol.(*Volume), nil
}

func (vs *Volumes) Remove(p string) error {
	vol, ok := vs.volumes.Load(p)
	if !ok {
		return interfaces.ErrNotExisted
	}
	vs.volumes.Delete(p)
	return vol.(*Volume).Remove()
}

func (vs *Volumes) Close() (err error) {
	vs.volumes.Range(func(key, value interface{}) bool {
		if err = value.(*Volume).Close(); err != nil {
			return false
		}
		vs.volumes.Delete(key)
		return true
	})
	return
}
