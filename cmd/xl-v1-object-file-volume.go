package cmd

import (
	"context"
	"io"
	"path"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/mimedb"
)

func (xl xlObjects) putObjectViaFileVolume(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	data := r.Reader

	// uniqueID := mustGetUUID()
	// tempObj := uniqueID
	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	// Get parity and data drive count based on storage class metadata
	dataDrives, parityDrives := getRedundancyCount(opts.UserDefined[amzStorageClass], len(xl.getDisks()))

	// we now know the number of blocks this object needs for data and parity.
	// writeQuorum is dataBlocks + 1
	writeQuorum := dataDrives + 1

	// delete all if failed
	undo := false
	defer func() {
		if !undo {
			return
		}
		xl.deleteObject(ctx, bucket, object, writeQuorum, hasSuffix(object, SlashSeparator))
	}()

	// Delete temporary object in the event of failure.
	// If PutObject succeeded there would be no temporary
	// object to delete.
	// defer xl.deleteObject(ctx, minioMetaTmpBucket, tempObj, writeQuorum, false)

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
			undo = true
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// Rename the successfully written temporary object to final location. Ignore errFileAccessDenied
		// error because it means that the target object dir exists and we want to be close to S3 specification.
		// if _, err = rename(ctx, xl.getDisks(), minioMetaTmpBucket, tempObj, bucket, object, true, writeQuorum, []error{errFileAccessDenied}); err != nil {
		// 	return ObjectInfo{}, toObjectErr(err, bucket, object)
		// }

		return dirObjectInfo(bucket, object, data.Size(), opts.UserDefined), nil
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	// -- FIXME (this also causes performance issue when disks are down).
	if xl.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	if xl.isObject(bucket, object) {
		// Deny if WORM is enabled
		if globalWORMEnabled {
			return ObjectInfo{}, ObjectAlreadyExists{Bucket: bucket, Object: object}
		}
		if err := xl.deleteObject(ctx, bucket, object, writeQuorum, hasSuffix(object, SlashSeparator)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		// // Rename if an object already exists to temporary location.
		// newUniqueID := mustGetUUID()
		//
		// // Delete successfully renamed object.
		// defer xl.deleteObject(ctx, minioMetaTmpBucket, newUniqueID, writeQuorum, false)
		//
		// // NOTE: Do not use online disks slice here: the reason is that existing object should be purged
		// // regardless of `xl.json` status and rolled back in case of errors. Also allow renaming the
		// // existing object if it is not present in quorum disks so users can overwrite stale objects.
		// _, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, newUniqueID, true, writeQuorum, []error{errFileNotFound})
		// if err != nil {
		// 	return ObjectInfo{}, toObjectErr(err, bucket, object)
		// }
	}

	// Initialize parts metadata
	partsMetadata := make([]xlMetaV1, len(xl.getDisks()))

	xlMeta := newXLMetaV1(object, dataDrives, parityDrives)

	// Initialize xl meta.
	for index := range partsMetadata {
		partsMetadata[index] = xlMeta
	}

	// Order disks according to erasure distribution
	onlineDisks := shuffleDisks(xl.getDisks(), xlMeta.Erasure.Distribution)

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

	partName := "part.1"
	fileName := pathJoin(object, partName)
	// tempErasureObj := pathJoin(uniqueID, partName)

	writers := make([]io.Writer, len(onlineDisks))
	for i, disk := range onlineDisks {
		if disk == nil {
			continue
		}
		writers[i] = newBitrotWriter(disk, bucket, fileName, erasure.ShardFileSize(data.Size()), DefaultBitrotAlgorithm, erasure.ShardSize())
	}

	n, erasureErr := erasure.Encode(ctx, data, writers, buffer, erasure.dataBlocks+1)
	closeBitrotWriters(writers)
	if erasureErr != nil {
		undo = true
		return ObjectInfo{}, toObjectErr(erasureErr, bucket, fileName)
	}

	// Should return IncompleteBody{} error when reader has fewer bytes
	// than specified in request header.
	if n < data.Size() {
		logger.LogIf(ctx, IncompleteBody{})
		undo = true
		return ObjectInfo{}, IncompleteBody{}
	}

	for i, w := range writers {
		if w == nil {
			onlineDisks[i] = nil
			continue
		}
		partsMetadata[i].AddObjectPart(1, partName, "", n, data.ActualSize())
		partsMetadata[i].Erasure.AddChecksumInfo(ChecksumInfo{partName, DefaultBitrotAlgorithm, bitrotWriterSum(w)})
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
		partsMetadata[index].Stat.Size = n
		partsMetadata[index].Stat.ModTime = modTime
	}

	// Write unique `xl.json` for each disk.
	if onlineDisks, err = writeUniqueXLMetadata(ctx, onlineDisks, bucket, object, partsMetadata, writeQuorum); err != nil {
		undo = true
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

	return objInfo, nil
}

func (xl xlObjects) deleteObjectViaFileVolume(ctx context.Context, bucket, object string, writeQuorum int, isDir bool) error {
	// var disks []StorageAPI
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
	var wg sync.WaitGroup
	disks := xl.getDisks()

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
			// 	e = disk.DeleteFile(minioMetaTmpBucket, tmpObj)
			// } else {
			// 	e = cleanupDir(ctx, disk, minioMetaTmpBucket, tmpObj)
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

func (xl xlObjects) doDeleteObjectsViaFileVolume(ctx context.Context, bucket string, objects []string, errs []error, writeQuorums []int, isDirs []bool) ([]error, error) {
	// var tmpObjs = make([]string, len(objects))
	var disks = xl.getDisks()
	//
	// if bucket == minioMetaTmpBucket {
	// 	copy(tmpObjs, objects)
	// } else {
	// 	for i, object := range objects {
	// 		if errs[i] != nil {
	// 			continue
	// 		}
	// 		var err error
	// 		tmpObjs[i] = mustGetUUID()
	// 		// Rename the current object while requiring write quorum, but also consider
	// 		// that a non found object in a given disk as a success since it already
	// 		// confirms that the object doesn't have a part in that disk (already removed)
	// 		if isDirs[i] {
	// 			disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObjs[i], true, writeQuorums[i],
	// 				[]error{errFileNotFound, errFileAccessDenied})
	// 		} else {
	// 			disks, err = rename(ctx, xl.getDisks(), bucket, object, minioMetaTmpBucket, tmpObjs[i], true, writeQuorums[i],
	// 				[]error{errFileNotFound})
	// 		}
	// 		if err != nil {
	// 			errs[i] = err
	// 		}
	// 	}
	// }

	// Initialize list of errors.
	var opErrs = make([]error, len(disks))
	var delObjErrs = make([][]error, len(disks))

	// Remove objects in bulk for each disk
	for index, disk := range disks {
		if disk == nil {
			opErrs[index] = errDiskNotFound
			continue
		}
		delObjErrs[index], opErrs[index] = cleanupObjectsBulkFromFileVolume(ctx, disk, bucket, objects, errs)
		if opErrs[index] == errVolumeNotFound {
			opErrs[index] = nil
		}
	}

	// Return errors if any during deletion
	if err := reduceWriteQuorumErrs(ctx, opErrs, objectOpIgnoredErrs, len(xl.getDisks())/2+1); err != nil {
		return nil, err
	}

	// Reduce errors for each object
	for objIndex := range objects {
		if errs[objIndex] != nil {
			continue
		}
		listErrs := make([]error, len(xl.getDisks()))
		for i := range delObjErrs {
			if delObjErrs[i] != nil {
				listErrs[i] = delObjErrs[i][objIndex]
			}
		}
		errs[objIndex] = reduceWriteQuorumErrs(ctx, listErrs, objectOpIgnoredErrs, writeQuorums[objIndex])
	}

	return errs, nil
}

func cleanupObjectsBulkFromFileVolume(ctx context.Context, storage StorageAPI, volume string, objsPaths []string, errs []error) ([]error, error) {
	dErrs, err := storage.DeleteFileBulk(volume, objsPaths)
	if err != nil {
		return nil, err
	}

	// Map files deletion errors to the correspondent objects
	for i := range dErrs {
		if dErrs[i] != nil {
			if errs[i] != nil {
				errs[i] = dErrs[i]
			}
		}
	}

	return errs, nil
}
