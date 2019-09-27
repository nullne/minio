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
	"time"

	"github.com/minio/minio/cmd/logger"
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
)

var (
	ErrUnderMaintenance = errors.New("there are other ongoing maintenance")
)

type Volume struct {
	//data dir, index dir, backup dir
	dir   string
	index Index
	files *files
}

func NewVolume(ctx context.Context, dir string) (v *Volume, err error) {
	index, err := NewRocksDBIndex(dir, parseRocksDBOptionsFromEnv())
	if err != nil {
		return nil, err
	}
	v = new(Volume)
	v.dir = dir
	v.index = index
	v.files, err = newFiles(ctx, path.Join(dir, DataDir))
	if err != nil {
		return nil, err
	}
	return v, nil
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (v *Volume) ReadAll(key string) ([]byte, error) {
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
	return ioutil.NopCloser(bf.NewBuffer(bs)), nil
}

func (v *Volume) WriteAll(key string, size int64, r io.Reader) error {
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
	if path == "" {
		return nil
	}
	return v.index.Delete(path)
}

func (v *Volume) List(p string, count int) ([]string, error) {
	return v.index.ListN(p, count)
}

func (v *Volume) Stat(key string) (os.FileInfo, error) {
	return v.index.Get(key)
}

// fake dir mod time
func (v *Volume) StatDir(p string) (os.FileInfo, error) {
	if p == "" {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return v.index.StatDir(p)
}

func (v *Volume) MakeDir(p string) error {
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
		sort.Sort(items)
		for _, item := range items {
			key := item.key
			nfi, err := v.index.Get(key)
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
			if err := v.index.Set(key, nnfi); err != nil {
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
		if e.IsDir() || directIndexStoring(e.fileName) {
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
		// if _, err := files[e.volumeID].WriteString(fmt.Sprintf("%s,%d\n", e.fileName, e.offset)); err != nil {
		if _, err := fmt.Fprintf(files[e.volumeID], "%s,%d\n", e.fileName, e.offset); err != nil {
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
