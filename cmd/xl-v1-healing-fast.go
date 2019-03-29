package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio/pkg/madmin"
)

func (xl xlObjects) healObjectFast(ctx context.Context, bucket string, object string, result madmin.HealResultItem, latestMeta xlMetaV1, latestDisks, outDatedDisks []StorageAPI, partsMetadata []xlMetaV1, disksToHealCount int) (madmin.HealResultItem, error) {
	// We write at temporary location and then rename to final location.
	// tmpID := mustGetUUID()

	// Heal each part. erasureHealFile() will write the healed
	// part to .minio/tmp/uuid/ which needs to be renamed later to
	// the final location.
	erasure, err := NewErasure(ctx, latestMeta.Erasure.DataBlocks,
		latestMeta.Erasure.ParityBlocks, latestMeta.Erasure.BlockSize)
	if err != nil {
		return result, toObjectErr(err, bucket, object)
	}

	erasureInfo := latestMeta.Erasure
	for partIndex := 0; partIndex < len(latestMeta.Parts); partIndex++ {
		partName := latestMeta.Parts[partIndex].Name
		partSize := latestMeta.Parts[partIndex].Size
		partActualSize := latestMeta.Parts[partIndex].ActualSize
		partNumber := latestMeta.Parts[partIndex].Number
		tillOffset := erasure.ShardFileTillOffset(0, partSize, partSize)
		readers := make([]io.ReaderAt, len(latestDisks))
		checksumAlgo := erasureInfo.GetChecksumInfo(partName).Algorithm
		for i, disk := range latestDisks {
			if disk == OfflineDisk {
				continue
			}
			checksumInfo := partsMetadata[i].Erasure.GetChecksumInfo(partName)
			readers[i] = newBitrotReader(disk, bucket, pathJoin(object, partName), tillOffset, checksumAlgo, checksumInfo.Hash, erasure.ShardSize())
		}
		writers := make([]io.Writer, len(outDatedDisks))
		for i, disk := range outDatedDisks {
			if disk == OfflineDisk {
				continue
			}
			writers[i] = newBitrotWriter(disk, bucket, pathJoin(object, partName), tillOffset, checksumAlgo, erasure.ShardSize())
		}
		hErr := erasure.Heal(ctx, readers, writers, partSize)
		closeBitrotReaders(readers)
		closeBitrotWriters(writers)
		if hErr != nil {
			return result, toObjectErr(hErr, bucket, object)
		}
		// outDatedDisks that had write errors should not be
		// written to for remaining parts, so we nil it out.
		for i, disk := range outDatedDisks {
			if disk == nil {
				continue
			}
			// A non-nil stale disk which did not receive
			// a healed part checksum had a write error.
			if writers[i] == nil {
				outDatedDisks[i] = nil
				disksToHealCount--
				continue
			}
			partsMetadata[i].AddObjectPart(partNumber, partName, "", partSize, partActualSize)
			partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, checksumAlgo, bitrotWriterSum(writers[i])})
		}

		// If all disks are having errors, we give up.
		if disksToHealCount == 0 {
			return result, fmt.Errorf("all disks without up-to-date data had write errors")
		}
	}

	// Generate and write `xl.json` generated from other disks.
	outDatedDisks, aErr := writeUniqueXLMetadata(ctx, outDatedDisks, bucket, object,
		partsMetadata, diskCount(outDatedDisks))
	if aErr != nil {
		return result, toObjectErr(aErr, bucket, object)
	}

	// Rename from tmp location to the actual location.
	for _, disk := range outDatedDisks {
		if disk == nil {
			continue
		}

		// Attempt a rename now from healed data to final location.
		// aErr = disk.RenameFile(minioMetaTmpBucket, retainSlash(tmpID), bucket,
		// 	retainSlash(object))
		// if aErr != nil {
		// 	logger.LogIf(ctx, aErr)
		// 	return result, toObjectErr(aErr, bucket, object)
		// }

		for i, v := range result.Before.Drives {
			if v.Endpoint == disk.String() {
				result.After.Drives[i].State = madmin.DriveStateOk
			}
		}
	}

	// Set the size of the object in the heal rsfesult
	result.ObjectSize = latestMeta.Stat.Size

	return result, nil

}
