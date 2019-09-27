package main

import (
	"context"

	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/interfaces"
)

func NewVolume(ctx context.Context, dir string) (interfaces.Volume, error) {
	return volume.NewVolume(ctx, dir)
}
