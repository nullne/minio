// volume stores small files into a bigger one, and records the corresponding
// index into rocksdb now which can be replaced by other db. In some cases,
// such that the file is extremely small, we store it directly into rocksdb
// for faster reading
package volume

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/volume/interfaces"
	"go.uber.org/multierr"

	bf "gopkg.in/bufio.v1"
)

const (
	xlJSONFile = "xl.json"

	IndexDir       = "index"
	DataDir        = "data"
	IndexBackupDir = "backup"
	MaintenanceDir = "maintenance"

	listFileSuffix          = ".list"
	completedListFileSuffix = ".completed"
	noNeedListFileSuffix    = ".no-need"

	slashSeperator = "/"
)

var (
	ErrUnderMaintenance = errors.New("there are other ongoing maintenance")
	ErrInvalidArgument  = errors.New("invalid argument specified")
)

type Volume struct {
	//data dir, index dir, backup dir
	dir   string
	index Index
	files *files

	// save the data directly in the index db rather than big file
	directIndexSaving func(string) bool
}

var disableDirectIndexSaving = func(string) bool { return false }

func NewVolume(ctx context.Context, dir string, idx Index) (*Volume, error) {
	var err error
	if dir, err = GetValidPath(dir); err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, errors.New("nil index")
	}

	files, err := newFiles(ctx, path.Join(dir, DataDir))
	if err != nil {
		return nil, err
	}
	return &Volume{
		dir:               dir,
		index:             idx,
		files:             files,
		directIndexSaving: disableDirectIndexSaving,
	}, nil
}

// MUST set before volume is working
func (v *Volume) SetDirectIndexSaving(fn func(string) bool) {
	v.directIndexSaving = fn
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
	if isDirectory(key) {
		return nil, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}

	if v.directIndexSaving(key) {
		if len(bs) < len8 {
			return nil, io.ErrUnexpectedEOF
		}
		return bs[len8:], nil
	}

	info, err := UnmarshalFileInfo(key, bs)
	if err != nil {
		return nil, err
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	data := make([]byte, info.size)
	_, err = file.read(data, int64(info.offset))
	return data, err
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF.
//
// If an EOF happens after reading some but not all the bytes,
// ReadFile returns ErrUnexpectedEOF.
func (v *Volume) ReadFile(key string, offset int64, buffer []byte) (int64, error) {
	if isDirectory(key) {
		return 0, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return 0, err
	}

	if v.directIndexSaving(key) {
		if int64(len(bs)-len8) < +offset {
			return 0, io.ErrUnexpectedEOF
		}
		n := copy(buffer, bs[int64(len8)+offset:])
		return int64(n), nil
	}

	info, err := UnmarshalFileInfo(key, bs)
	if err != nil {
		return 0, err
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return 0, err
	}
	n, err := file.read(buffer, int64(info.offset)+offset)
	return n, err
}

func (v *Volume) ReadFileStream(key string, offset, length int64) (io.ReadCloser, error) {
	if isDirectory(key) {
		return nil, interfaces.ErrIsNotReguler
	}
	bs, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}

	if v.directIndexSaving(key) {
		offset += len8
		if int64(len(bs)) < offset {
			return nil, io.ErrUnexpectedEOF
		}
		if offset+length > int64(len(bs)) {
			length = int64(len(bs)) - offset
		}
		return ioutil.NopCloser(bf.NewBuffer(bs[offset : offset+length])), nil
	}

	info, err := UnmarshalFileInfo(key, bs)
	if err != nil {
		return nil, err
	}
	if offset > int64(info.size) {
		return ioutil.NopCloser(bf.NewBuffer([]byte{})), nil
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	if offset+length > int64(info.size) {
		length = int64(info.size) - offset
	}
	data := make([]byte, length)
	_, err = file.read(data, int64(info.offset)+offset)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bf.NewBuffer(data)), nil
}

func (v *Volume) WriteAll(key string, bs []byte) error {
	if v.directIndexSaving(key) {
		info := NewShortFileInfo(key).MarshalBinary()
		return v.index.Set(key, append(info, bs...))
	}

	fi, err := v.files.write(bs)
	if err != nil {
		return err
	}
	return v.index.Set(key, fi.MarshalBinary())
}

// delete /data/bucket/object will delete all following keys
// /data1/bucket/object/xl.json
// /data1/bucket/object/part.1
// /data1/bucket/object/part.2
// return error "not empty", if delete /data1/bucket (not the object directory)
func (v *Volume) Delete(path string) error {
	if path == "" {
		return nil
	}
	return v.index.Delete(path)
}

func (v *Volume) List(p string, count int) ([]string, error) {
	return v.index.ListN(p, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	data, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	var fi FileInfo
	if v.directIndexSaving(key) {
		fi = NewShortFileInfo(key)
	} else {
		fi = NewFileInfo(key)
	}
	if err := fi.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return fi, nil
}

// Mkdir is a little bit different from os.Mkdir, but should work well with object storage
func (v *Volume) Mkdir(p string) error {
	if !strings.HasSuffix(p, slashSeperator) {
		p += slashSeperator
	}
	if p == "" {
		return ErrInvalidArgument
	}
	_, err := v.index.Get(p)
	if err == nil {
		return os.ErrExist
	} else if err != interfaces.ErrKeyNotExisted {
		return err
	}

	nfi := NewDirInfo(p)
	return v.index.Set(p, nfi.MarshalBinary())
}

func (v *Volume) Maintain(ctx context.Context) error {
	dir := path.Join(v.dir, MaintenanceDir)
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), listFileSuffix) {
			continue
		}
		idx := strings.Index(fi.Name(), ".")
		if idx == -1 {
			return errors.New("cannot parse index")
		}

		id, err := strconv.Atoi(fi.Name()[:idx])
		if err != nil {
			return err
		}
		volumeID := uint32(id)
		oldFile, err := v.files.getFileToRead(volumeID)
		if err != nil {
			return err
		}
		f, err := os.Open(path.Join(dir, fi.Name()))
		if err != nil {
			return err
		}

		var items maintainItems
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			ss := strings.Split(scanner.Text(), ",")
			if len(ss) != 2 {
				continue
			}
			i, err := strconv.Atoi(ss[1])
			if err != nil {
				return err
			}
			items = append(items, maintainItem{
				key:  ss[0],
				size: i,
			})
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		// it's faster to read in sequence rather than in random
		sort.Sort(items)
		for _, item := range items {
			key := item.key
			data, err := v.index.Get(key)
			if err != nil {
				return err
			}
			nfi, err := UnmarshalFileInfo(key, data)
			if err != nil {
				return err
			}

			// wrote to the same file after dumpping
			if nfi.volumeID != volumeID {
				continue
			}
			bs := make([]byte, nfi.size)
			if _, err := oldFile.read(bs, int64(nfi.offset)); err != nil {
				return err
			}
			nnfi, err := v.files.write(bs)
			if err != nil {
				return err
			}
			if err := v.index.Set(key, nnfi.MarshalBinary()); err != nil {
				return err
			}
		}

		if err := v.files.deleteFile(volumeID); err != nil {
			return err
		}
		// if err := os.Remove(path.Join(dir, fi.Name())); err != nil {
		// 	return err
		// }
		oldPath := path.Join(dir, fi.Name())
		if err := os.Rename(oldPath, oldPath+completedListFileSuffix); err != nil {
			return err
		}
	}

	return nil
}

func (v *Volume) DumpListToMaintain(ctx context.Context, rate float64) error {
	if finished, err := v.isDumpingFinished(); err != nil {
		return err
	} else if finished {
		return nil
	}
	dir := path.Join(v.dir, MaintenanceDir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0777); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	fids := v.files.readOnlyFiles()
	readOnlyFIDs := make(map[uint32]struct{})
	for _, fid := range fids {
		readOnlyFIDs[uint32(fid)] = struct{}{}
	}
	ch, ech := v.index.ScanAll(ctx, fileVolumeStoring)
	files := make(map[uint32]*os.File)
	sizes := make(map[uint32]int64)

	for e := range ch {
		if e.IsDir() || directIndexStoring(e.name) {
			continue
		}
		if _, ok := readOnlyFIDs[e.volumeID]; !ok {
			continue
		}
		if _, ok := files[e.volumeID]; !ok {
			file, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", e.volumeID, listFileSuffix)), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			defer file.Close()
			files[e.volumeID] = file
		}
		if _, err := fmt.Fprintf(files[e.volumeID], "%s,%d\n", e.name, e.offset); err != nil {
			return err
		}
		sizes[e.volumeID] += e.Size()
	}
	for err := range ech {
		if err != nil {
			return err
		}
	}

	// renmae no-need compacting list
	for vid, size := range sizes {
		if float64(size)/float64(MaxFileSize) < rate {
			continue
		}
		oldPath := path.Join(dir, fmt.Sprintf("%d%s", vid, listFileSuffix))
		if err := os.Rename(oldPath, oldPath+noNeedListFileSuffix); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) CleanMaintain() error {
	dir := path.Join(v.dir, MaintenanceDir)
	return os.RemoveAll(dir)
}

// if not finished, clean the remaining files
func (v *Volume) isDumpingFinished() (bool, error) {
	dir := path.Join(v.dir, MaintenanceDir)
	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer f.Close()
	dirs, err := f.Readdirnames(-1)
	if err != nil {
		return false, err
	}
	var dumped, completed int
	for _, d := range dirs {
		if strings.HasSuffix(d, listFileSuffix) {
			dumped += 1
		} else if strings.HasSuffix(d, completedListFileSuffix) {
			dumped += 1
			completed += 1
		} else if strings.HasSuffix(d, noNeedListFileSuffix) {
			dumped += 1
			completed += 1
		}
	}
	if completed == 0 {
		// dumpling failed most likely, remove the remaining file
		if err := os.RemoveAll(dir); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// remove the volume itself including data and index
// remove the backup dir also
func (v *Volume) Remove() (err error) {
	err = multierr.Append(err, v.index.Remove())
	err = multierr.Append(err, v.files.remove())
	err = multierr.Append(err, os.RemoveAll(v.dir))
	return
}

func (v *Volume) Close() (err error) {
	err = multierr.Append(err, v.index.Close())
	err = multierr.Append(err, v.files.close())
	return
}

func PathJoin(elem ...string) string {
	return pathJoin(elem...)
}

// pathJoin - like path.Join() but retains trailing "/" of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if strings.HasSuffix(elem[len(elem)-1], "/") {
			trailingSlash = "/"
		}
	}
	ps := path.Join(elem...)
	if ps == "/" {
		return ps
	}
	return ps + trailingSlash
}

func catchPanic() {
	if err := recover(); err != nil {
		logger.Info("catch panic: %+v", err)
	}
}

type maintainItem struct {
	size int
	key  string
}

type maintainItems []maintainItem

func (s maintainItems) Len() int {
	return len(s)
}

func (s maintainItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s maintainItems) Less(i, j int) bool {
	return s[i].size < s[i].size
}
