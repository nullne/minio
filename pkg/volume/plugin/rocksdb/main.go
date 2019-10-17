package main

import (
	"context"
	"os"
	"strings"

	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/index/rocksdb"
	"github.com/minio/minio/pkg/volume/interfaces"
)

func NewVolume(ctx context.Context, dir string) (interfaces.Volume, error) {
	var err error
	if dir, err = volume.GetValidPath(dir); err != nil {
		return nil, err
	}

	if err := volume.MkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	options := volume.IndexOptions{}
	if os.Getenv("MINIO_FILE_VOLUME_INDEX_ROOT"); s != "" {
		options.Root = s
	}
	idx, err := rocksdb.NewIndex(dir, options)
	if err != nil {
		return nil, err
	}
	v, err := volume.NewVolume(ctx, dir, idx)
	if err != nil {
		idx.Close()
		return nil, err
	}
	v.SetDirectIndexSaving(func(key string) bool {
		return strings.HasSuffix(key, "xl.json")
	})
	return v, nil
}

func NewVolumes() interfaces.Volumes {
	return volume.NewVolumes(NewVolume)
}
