// volume stores small files into a bigger one, and records the corresponding
// index into rocksdb now which can be replaced by other db. In some cases,
// such that the file is extremely small, we store it directly into rocksdb
// for faster reading

package volume

import (
	"bufio"
	"bytes"
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
	"sync/atomic"
	"time"

	"github.com/minio/minio/cmd/logger"
	"go.uber.org/multierr"
)

const (
	xlJSONFile = "xl.json"

	IndexDir       = "index"
	DataDir        = "data"
	IndexBackupDir = "backup"
	MaintenanceDir = "maintenance"

	listFileSuffix = ".list"
)

var (
	ErrUnderMaintenance = errors.New("there are other ongoing maintenance")
	ErrClosed           = errors.New("volume has been closed")
)

const (
	maintainenceStatusIdle        uint32 = iota
	maintainenceStatusDumpingList        // start
	maintainenceStatusCompacting
)

type Volume struct {
	dir   string
	index Index
	files *files

	ctx                context.Context
	cancel             context.CancelFunc
	requestCount       int32
	closedFlag         uint32
	maintainenceStatus uint32
}

func NewVolume(ctx context.Context, dir string) (*Volume, error) {
	ctx, cancel := context.WithCancel(ctx)
	index, err := NewRocksDBIndex(ctx, dir, parseRocksDBOptionsFromEnv())
	if err != nil {
		return nil, err
	}
	files, err := newFiles(ctx, path.Join(dir, DataDir))
	if err != nil {
		return nil, err
	}

	v := Volume{
		dir:    dir,
		index:  index,
		files:  files,
		ctx:    ctx,
		cancel: cancel,
	}
	return &v, nil
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return nil, ErrClosed
	}
	info, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", key)
	}
	if strings.HasSuffix(key, xlJSONFile) {
		return info.Data(), nil
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, info.size)
	_, err = file.read(bs, int64(info.offset))
	return bs, err
}

// ReadFile reads exactly len(buf) bytes into buf. It returns the
// number of bytes copied. The error is EOF only if no bytes were
// read. On return, n == len(buf) if and only if err == nil. n == 0
// for io.EOF.
//
// If an EOF happens after reading some but not all the bytes,
// ReadFile returns ErrUnexpectedEOF.
func (v *Volume) ReadFile(key string, offset int64, buffer []byte) (int64, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return -1, ErrClosed
	}
	info, err := v.index.Get(key)
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("%s is a directory", key)
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return 0, err
	}
	n, err := file.read(buffer, int64(info.offset)+offset)
	return n, err
}

func (v *Volume) ReadFileStream(key string, offset, length int64) (io.ReadCloser, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return nil, ErrClosed
	}
	info, err := v.index.Get(key)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", key)
	}
	file, err := v.files.getFileToRead(info.volumeID)
	if err != nil {
		return nil, err
	}
	if offset > int64(info.size) {
		offset = int64(info.size)
		length = 0
	}
	if offset+length > int64(info.size) {
		length = int64(info.size) - offset
	}
	bs := make([]byte, length)
	_, err = file.read(bs, int64(info.offset)+offset)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewBuffer(bs)), nil
}

func (v *Volume) WriteAll(key string, size int64, r io.Reader) error {
	atomic.AddInt32(&(v.requestCount), 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return ErrClosed
	}
	// io.Reader isn't predictable
	bs := make([]byte, size)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}

	if strings.HasSuffix(key, xlJSONFile) {
		return v.index.Set(key, FileInfo{
			fileName: key,
			data:     bs,
		})
	}

	fi, err := v.files.write(bs)
	if err != nil {
		return err
	}
	return v.index.Set(key, fi)
}

// delete /data/bucket/object will delete all following keys
// /data1/bucket/object/xl.json
// /data1/bucket/object/part.1
// /data1/bucket/object/part.2
// return error "not empty", if delete /data1/bucket (not the object directory)
func (v *Volume) Delete(path string) error {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return ErrClosed
	}
	if path == "" {
		return nil
	}
	return v.index.Delete(path)
}

func (v *Volume) List(p string, count int) ([]string, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return nil, ErrClosed
	}
	return v.index.ListN(p, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return nil, ErrClosed
	}
	return v.index.Get(key)
}

// fake dir mod time
func (v *Volume) StatDir(p string) (os.FileInfo, error) {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return nil, ErrClosed
	}
	if p == "" {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return v.index.StatDir(p)
}

func (v *Volume) MakeDir(p string) error {
	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return ErrClosed
	}
	if p == "" {
		return nil
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return v.index.Set(p, FileInfo{
		fileName: p,
		isDir:    true,
		modTime:  time.Now(),
	})
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

type maintainItem struct {
	key    string
	offset int
	size   int
}

type maintainItems []maintainItem

func (s maintainItems) Len() int {
	return len(s)
}

func (s maintainItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s maintainItems) Less(i, j int) bool {
	return s[i].offset < s[i].offset
}

// will send status into ch if it is not nil, will be closed at the end
func (v *Volume) Maintain(ctx context.Context, rate float64, ch chan string) (err error) {
	if ch != nil {
		defer close(ch)
	}

	atomic.AddInt32(&v.requestCount, 1)
	defer atomic.AddInt32(&v.requestCount, -1)
	if v.closed() {
		return ErrClosed
	}

	defer func() {
		if err != nil {
			pushMaintainStatus(ch, fmt.Sprintf("failed to maitain: %s", err.Error()))
		}
	}()

	if !v.changeMaintainStatus(maintainenceStatusDumpingList) {
		return ErrUnderMaintenance
	}
	pushMaintainStatus(ch, "dumping")
	defer v.changeMaintainStatus(maintainenceStatusIdle)

	defer os.RemoveAll(v.maintainPath())

	list, err := v.dumpListToMaintain(ctx, rate, ch)
	if err != nil {
		return err
	}

	v.changeMaintainStatus(maintainenceStatusCompacting)
	pushMaintainStatus(ch, "compacting")

	for i, vid := range list {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-v.ctx.Done():
			return v.ctx.Err()
		default:
		}
		if err := v.maintainSingle(ctx, vid, v.maintainListPath(vid)); err != nil {
			logger.Info("failed to maintainSingle: %d %v", vid, err)
			return err
		}
		pushMaintainStatus(ch, fmt.Sprintf("compacting: %d/%d", i+1, len(list)))
	}
	return nil
}

func (v *Volume) changeMaintainStatus(s uint32) bool {
	switch s {
	case maintainenceStatusIdle:
		atomic.StoreUint32(&(v.maintainenceStatus), s)
		return true
	case maintainenceStatusDumpingList:
		if !atomic.CompareAndSwapUint32(&(v.maintainenceStatus), maintainenceStatusIdle, s) {
			return false
		}
		return true
	case maintainenceStatusCompacting:
		atomic.StoreUint32(&(v.maintainenceStatus), s)
		return true
	default:
		return false
	}
}

func pushMaintainStatus(ch chan string, s string) {
	if ch == nil {
		return
	}
	select {
	case ch <- s:
	default:
	}
}

func (v *Volume) maintainSingle(ctx context.Context, volumeID uint32, listFile string) error {
	f, err := os.Open(listFile)
	if err != nil {
		logger.Info("failed to open %s: %v", listFile, err)
		return err
	}
	defer f.Close()

	var items maintainItems
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ss := strings.Split(scanner.Text(), ",")
		if len(ss) != 3 {
			continue
		}
		offset, err := strconv.Atoi(ss[1])
		if err != nil {
			return err
		}
		size, err := strconv.Atoi(ss[2])
		if err != nil {
			return err
		}
		items = append(items, maintainItem{
			key:    ss[0],
			size:   size,
			offset: offset,
		})
	}
	if err := scanner.Err(); err != nil {
		logger.Info("failed to scan %s: %v", listFile, err)
		return err
	}
	sort.Sort(items)
	for _, item := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		reader, err := v.ReadFileStream(item.key, 0, int64(item.size))
		if err != nil {
			logger.Info("failed to read %s: %v", item.key, err)
			return err
		}
		if err := v.WriteAll(item.key, int64(item.size), reader); err != nil {
			logger.Info("failed to write %s: %v", item.key, err)
			reader.Close()
			return err
		}
		reader.Close()
	}
	if err := v.files.deleteFile(volumeID); err != nil {
		logger.Info("failed to delete volume %v: %v", volumeID, err)
		return err
	}
	return nil
}

func (v *Volume) dumpListToMaintain(ctx context.Context, rate float64, logch chan string) ([]uint32, error) {
	if err := os.RemoveAll(v.maintainPath()); err != nil {
		return nil, err
	}
	if err := os.Mkdir(v.maintainPath(), 0777); err != nil {
		return nil, err
	}

	fids := v.files.readOnlyFiles()
	readOnlyFIDs := make(map[uint32]struct{})
	for _, fid := range fids {
		readOnlyFIDs[uint32(fid)] = struct{}{}
	}
	ch, ech := v.index.ScanAll(ctx, isData)
	files := make(map[uint32]*os.File)
	sizes := make(map[uint32]int64)

	for e := range ch {
		if _, ok := readOnlyFIDs[e.volumeID]; !ok {
			continue
		}
		if _, ok := files[e.volumeID]; !ok {
			file, err := os.OpenFile(v.maintainListPath(e.volumeID), os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			defer file.Close()
			files[e.volumeID] = file
		}
		if _, err := fmt.Fprintf(files[e.volumeID], "%s,%d,%d\n", e.fileName, e.offset, e.size); err != nil {
			return nil, err
		}
		sizes[e.volumeID] += e.Size()
	}
	for err := range ech {
		if err != nil {
			return nil, err
		}
	}

	list := make([]uint32, 0, len(sizes))
	for vid, size := range sizes {
		if float64(size)/float64(MaxFileSize) < rate {
			list = append(list, vid)
			continue
		}
		if err := os.Remove(files[vid].Name()); err != nil {
			return nil, err
		}
	}
	pushMaintainStatus(logch, fmt.Sprintf("finish dumping, total: %d, need compaction: %d", len(sizes), len(list)))
	return list, nil
}

func (v *Volume) backupPath() string {
	return path.Join(v.dir, IndexBackupDir)
}

func (v *Volume) dataPath() string {
	return path.Join(v.dir, DataDir)
}

func (v *Volume) maintainPath() string {
	return path.Join(v.dir, MaintenanceDir)
}

func (v *Volume) maintainListPath(vid uint32) string {
	return path.Join(v.dir, MaintenanceDir, fmt.Sprintf("%d%s", vid, listFileSuffix))
}

// remove the volume itself including data and index
// remove the backup dir also
func (v *Volume) Remove() (err error) {
	err = v.Close()
	err = multierr.Append(err, v.index.Remove())
	err = multierr.Append(err, v.files.remove())
	err = multierr.Append(err, os.RemoveAll(v.dir))
	return
}

func (v *Volume) closed() bool {
	return atomic.LoadUint32(&(v.closedFlag)) == 1
}

func (v *Volume) Close() (err error) {
	if v.closed() {
		return nil
	}
	atomic.StoreUint32(&(v.closedFlag), 1)

	// inform the long runing or background jobs to exit
	v.cancel()

	// wait for all requests finished or timeout
	err = func() error {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				if atomic.LoadInt32(&v.requestCount) == 0 {
					return nil
				}
			case <-timer.C:
				return fmt.Errorf("prematurely closed volume %s after timeout", v.dir)
			}
		}
	}()
	err = multierr.Append(err, v.index.Close())
	err = multierr.Append(err, v.files.close())
	return
}
