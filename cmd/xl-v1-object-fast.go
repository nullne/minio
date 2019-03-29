/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
)

// change the object stortage method
func (xl xlObjects) putObjectFast(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	// Get parity and data drive count based on storage class metadata
	dataDrives, parityDrives := getRedundancyCount(opts.UserDefined[amzStorageClass], len(xl.getDisks()))

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives + 1

	// @TODO handle empty directory later
	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		// -- FIXME. (needs a new kind of lock).
		// -- FIXME (this also causes performance issue when disks are down).
		if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
		}

		if err = xl.putObjectDir(ctx, bucket, object, writeQuorum); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Rename the successfully written temporary object to final location. Ignore errFileAccessDenied
		// error because it means that the target object dir exists and we want to be close to S3 specification.
		// if _, err = rename(ctx, xl.getDisks(), minioMetaTmpBucket, tempObj, bucket, object, true, writeQuorum, []error{errFileAccessDenied}); err != nil {
		// 	return ObjectInfo{}, toObjectErr(err, bucket, object)
		// }

		return dirObjectInfo(bucket, object, data.Size(), opts.UserDefined), nil
	}

	// Validate put object input args.
	// if err = checkPutObjectArgs(ctx, bucket, object, xl, data.Size()); err != nil {
	// 	return ObjectInfo{}, err
	// }

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	// if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
	// 	return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	// }

	// if xl.isObject(bucket, object) {
	// 	// Deny if WORM is enabled
	// 	if globalWORMEnabled {
	// 		return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
	// 	}
	//
	// 	// delete object
	// 	xl.deleteObject(ctx, bucket, object, writeQuorum, false)
	//
	// 	// Rename if an object already exists to temporary location.
	// 	// newUniqueID := mustGetUUID()
	//
	// 	// Delete successfully renamed object.
	// 	// defer xl.deleteObject(ctx, minioMetaTmpBucket, newUniqueID, writeQuorum, false)
	//
	// 	// NOTE: Do not use online disks slice here: the reason is that existing object should be purged
	// 	// regardless of `xl.json` status and rolled back in case of errors. Also allow renaming the
	// 	// existing object if it is not present in quorum disks so users can overwrite stale objects.
	// 	// _, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, newUniqueID, true, writeQuorum, []error{errFileNotFound})
	// 	// if err != nil {
	// 	// 	return ObjectInfo{}, toObjectErr(err, bucket, object)
	// 	// }
	// }

	// Limit the reader to its provided size if specified.
	var reader io.Reader = data

	// Initialize parts metadata
	partsMetadata := make([]xlMetaV1, len(xl.getDisks()))

	xlMeta := newXLMetaV1(object, dataDrives, parityDrives)

	// Initialize xl meta.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(xl.getDisks(), partsMetadata[0].Erasure.Distribution)

	// Total size of the written object
	var sizeWritten int64

	erasure, err := NewErasure(ctx, xlMeta.Erasure.DataBlocks, xlMeta.Erasure.ParityBlocks, xlMeta.Erasure.BlockSize)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch size := data.Size(); {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size == -1 || size >= blockSizeV1:
		buffer = xl.bp.Get()
		defer xl.bp.Put(buffer)
	case size < blockSizeV1:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size)
	}

	if len(buffer) > int(xlMeta.Erasure.BlockSize) {
		buffer = buffer[:xlMeta.Erasure.BlockSize]
	}

	// Read data and split into parts - similar to multipart mechanism
	for partIdx := 1; ; partIdx++ {
		// Compute part name
		partName := "part." + strconv.Itoa(partIdx)
		// Compute the path of current part
		// tempErasureObj := pathJoin(uniqueID, partName)

		// Calculate the size of the current part.
		var curPartSize int64
		curPartSize, err = calculatePartSizeFromIdx(ctx, data.Size(), globalPutPartSize, partIdx)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Hint the filesystem to pre-allocate one continuous large block.
		// This is only an optimization.
		var curPartReader io.Reader

		if curPartSize < data.Size() {
			curPartReader = io.LimitReader(reader, curPartSize)
		} else {
			curPartReader = reader
		}

		writers := make([]io.Writer, len(onlineDisks))
		for i, disk := range onlineDisks {
			if disk == nil {
				continue
			}
			writers[i] = newBitrotWriter(disk, bucket, pathJoin(object, partName), erasure.ShardFileSize(curPartSize), DefaultBitrotAlgorithm, erasure.ShardSize())
		}

		n, erasureErr := erasure.Encode(ctx, curPartReader, writers, buffer, erasure.dataBlocks+1)
		// Note: we should not be defer'ing the following closeBitrotWriters() call as we are inside a for loop i.e if we use defer, we would accumulate a lot of open files by the time
		// we return from this function.
		closeBitrotWriters(writers)
		if erasureErr != nil {
			return ObjectInfo{}, toObjectErr(erasureErr, bucket, object)
			// return ObjectInfo{}, toObjectErr(erasureErr, minioMetaTmpBucket, tempErasureObj)
		}

		// Should return IncompleteBody{} error when reader has fewer bytes
		// than specified in request header.

		if n < curPartSize && data.Size() > 0 {
			logger.LogIf(ctx, IncompleteBody{})
			return ObjectInfo{}, IncompleteBody{}
		}

		if n == 0 && data.Size() == -1 {
			// The last part of a compressed object will always be empty
			// Since the compressed size is unpredictable.
			// Hence removing the last (empty) part from all `xl.disks`.
			// @TODO
			// dErr := xl.deleteObject(ctx, minioMetaTmpBucket, tempErasureObj, writeQuorum, true)
			// if dErr != nil {
			// 	return ObjectInfo{}, toObjectErr(dErr, minioMetaTmpBucket, tempErasureObj)
			// }
			break
		}

		// Update the total written size
		sizeWritten += n

		for i, w := range writers {
			if w == nil {
				onlineDisks[i] = nil
				continue
			}
			partsMetadata[i].AddObjectPart(partIdx, partName, "", n, data.ActualSize())
			partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, DefaultBitrotAlgorithm, bitrotWriterSum(w)})
		}

		// We wrote everything, break out.
		if sizeWritten == data.Size() {
			break
		}
	}

	// Save additional erasureMetadata.
	modTime := UTCNow()

	opts.UserDefined["etag"] = r.MD5CurrentHexString()

	// Guess content-type from the extension if possible.
	if opts.UserDefined["content-type"] == "" {
		opts.UserDefined["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	// Fill all the necessary metadata.
	// Update `xl.json` content on each disks.
	for index := range partsMetadata {
		partsMetadata[index].Meta = opts.UserDefined
		partsMetadata[index].Stat.Size = sizeWritten
		partsMetadata[index].Stat.ModTime = modTime
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(ctx, onlineDisks, bucket, object, partsMetadata, writeQuorum); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Rename the successfully written temporary object to final location.
	// if _, err = rename(ctx, onlineDisks, minioMetaTmpBucket, tempObj, bucket, object, true, writeQuorum, nil); err != nil {
	// 	return ObjectInfo{}, toObjectErr(err, bucket, object)
	// }

	// Object info is the same in all disks, so we can pick the first meta
	// of the first disk
	xlMeta = partsMetadata[0]

	objInfo = ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            xlMeta.Stat.Size,
		ModTime:         xlMeta.Stat.ModTime,
		ETag:            xlMeta.Meta["etag"],
		ContentType:     xlMeta.Meta["content-type"],
		ContentEncoding: xlMeta.Meta["content-encoding"],
		UserDefined:     xlMeta.Meta,
	}

	// Success, return object info.
	return objInfo, nil
}

func (xl xlObjects) deleteObjectFast(ctx context.Context, bucket, object string, writeQuorum int, isDir bool) error {
	disks := xl.getDisks()
	// var err error

	// tmpObj := mustGetUUID()
	// if bucket == minioMetaTmpBucket {
	// 	tmpObj = object
	// 	disks = xl.getDisks()
	// } else {
	// 	// Rename the current object while requiring write quorum, but also consider
	// 	// that a non found object in a given disk as a success since it already
	// 	// confirms that the object doesn't have a part in that disk (already removed)
	// 	if isDir {
	// 		disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
	// 			[]error{errFileNotFound, errFileAccessDenied})
	// 	} else {
	// 		disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObj, true, writeQuorum,
	// 			[]error{errFileNotFound})
	// 	}
	// 	if err != nil {
	// 		return toObjectErr(err, bucket, object)
	// 	}
	// }

	// Initialize sync waitgroup.
	var wg = &sync.WaitGroup{}

	// Initialize list of errors.
	var dErrs = make([]error, len(disks))

	for index, disk := range disks {
		if disk == nil {
			dErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		go func(index int, disk StorageAPI, isDir bool) {
			defer wg.Done()
			e := disk.DeleteFile(bucket, object)
			// var e error
			// if isDir {
			// 	// DeleteFile() simply tries to remove a directory
			// 	// and will succeed only if that directory is empty.
			// 	e = disk.DeleteFile(bucket, object)
			// } else {
			// 	e = cleanupDir(ctx, disk, bucket, object)
			// }
			if e != nil && e != errVolumeNotFound {
				dErrs[index] = e
			}
		}(index, disk, isDir)
	}

	// Wait for all routines to finish.
	wg.Wait()

	// return errors if any during deletion
	return reduceWriteQuorumErrs(ctx, dErrs, objectOpIgnoredErrs, writeQuorum)
}
