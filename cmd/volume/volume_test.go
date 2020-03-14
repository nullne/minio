package volume_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math"
	mrand "math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/cmd/volume"
	"github.com/nullne/go-ycsb/pkg/generator"
	"github.com/nullne/go-ycsb/pkg/ycsb"
	"golang.org/x/time/rate"
	"gopkg.in/bufio.v1"
)

var (
	tmpDir = flag.String("tmp-dir", "/tmp", "")
)

// will be deleted
func TestVolumeAndFileConcurrently(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sizes := make([]int64, 10)
			for i := 0; i < len(sizes); i++ {
				sizes[i] = mrand.Int63n(1024 * 100)
			}
			for i, size := range sizes {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprintf("%d-%d", i, mrand.Int())
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Errorf("%v: bytes not equal, wanna: %v, got: %v", i, len(data), len(bs))
				}

				err = v.Delete(key)
				if err != nil {
					t.Error(err)
					continue
				}

				file3, err := v.ReadAll(key)
				if err == nil || file3 != nil {
					t.Error("delete failed?")
				}
			}
		}()
	}
	wg.Wait()
}

// will be deleted
func TestVolumeAndFile(t *testing.T) {
	volume.MaxFileSize = 4 << 20
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	for idx := 0; idx < 3; idx++ {
		func() {
			v, err := volume.NewVolume(context.Background(), dir)
			if err != nil {
				t.Error(err)
				return
			}
			defer v.Close()

			_, err = volume.NewVolume(context.Background(), dir)
			// _, err = volume.NewVolume(context.Background(), dir)
			if err == nil {
				t.Error("should not init twice ")
			}

			for i, size := range []int64{1, 100, 1024, 1024 * 1024, 1024 * 1024 * 10, 1024, 100, 1} {
				data := make([]byte, size)
				rand.Read(data)
				r := bufio.NewBuffer(data)

				key := fmt.Sprint(i)
				err := v.WriteAll(key, size, r)
				if err != nil {
					t.Error(i, err)
					continue
				}

				bs, err := v.ReadAll(key)
				if err != nil {
					t.Error(i, err)
					continue
				}

				if !bytes.Equal(data, bs) {
					t.Error(i, "bytes not equal")
				}

				err = v.Delete(key)
				if err != nil {
					t.Error(err)
				}

				file3, err := v.ReadAll(key)
				if err == nil || file3 != nil {
					t.Fatal("delete failed?")
				}
			}
		}()
	}
}

func TestVolume_ReadAll(t *testing.T) {
}

func TestVolume_ReadFile(t *testing.T) {
}

func TestVolume_ReadFileStream(t *testing.T) {
}

func TestVolume_WriteAll(t *testing.T) {
}

func TestVolume_Delete(t *testing.T) {
}

func TestVolume_Stat(t *testing.T) {
}

// will be refined
func TestVolume_List(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	for i := 30; i > 10; i-- {
		r := bufio.NewBuffer([]byte("nice"))
		key := strings.Join(strings.Split(fmt.Sprint(i), ""), "/") + ".go"

		if err := v.WriteAll(key, 4, r); err != nil {
			t.Error(err)
			return
		}
	}
	entries, err := v.List("", -1)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(entries, ",") != "1/,2/,3/" {
		t.Error("wrong list entries", entries)
	}

	entries, err = v.List("1", -1)
	if err != nil {
		t.Error(err)
		return
	}
	if strings.Join(entries, " ") != "1.go 2.go 3.go 4.go 5.go 6.go 7.go 8.go 9.go" {
		t.Error("wrong list entries", entries)
	}
}

// will be refined
func TestVolume_StatDir(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Error(err)
		return
	}
	defer v.Close()

	paths := []string{
		"/",
		"/a/b/c/",
		"/a/b",
	}
	for _, p := range paths {
		if err := v.MakeDir(p); err != nil {
			t.Error(err)
			return
		}
		fi, err := v.StatDir(p)
		if err != nil {
			t.Error(err)
			return
		}
		if !fi.IsDir() {
			t.Error("not a directory")
			return
		}
		entries, err := v.List(p, 1)
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println("---------------------", entries)
		if err := v.Delete(p); err != nil {
			t.Error(err)
			return
		}
		if _, err := v.StatDir(p); err != os.ErrNotExist {
			t.Error("should not exist")
			return
		}
	}
}

func TestVolume_Close(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	if err := v.Close(); err != nil {
		t.Error(err)
	}

	// try to open again
	nv, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}

	// verify data
	if err := verifyAll(nv, concurrent, recordCount, p); err != nil {
		t.Error(err)
	}

	if err := nv.Close(); err != nil {
		t.Error(err)
	}
}

func TestVolume_Maintain(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Remove(); err != nil {
			t.Error(err)
		}
	}()

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	go func() {
		// only one maintenance instance
		time.Sleep(10 * time.Millisecond)
		if err := v.Maintain(ctx, 2, nil); err != volume.ErrUnderMaintenance {
			t.Error("shouldn't run")
			return
		}
	}()

	if err := v.Maintain(ctx, 2, nil); err != nil {
		t.Error(err)
		return
	}

	// verify data
	if err := verifyAll(v, concurrent, recordCount, p); err != nil {
		t.Error(err)
	}

}

func TestVolume_Maintain_closeOnDumping(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	logch := make(chan string)
	closed := make(chan error)
	go func() {
		for s := range logch {
			if strings.Contains(s, "dumping") {
				closed <- v.Close()
				return
			}
		}
		closed <- errors.New("failed to close")
	}()

	if err := v.Maintain(ctx, 2, logch); err != context.Canceled {
		t.Errorf("wanna: %v, got: %v", context.Canceled, err)
		return
	}

	if err := <-closed; err != nil {
		t.Fatal(err)
	}

	nv, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer nv.Close()
	// verify data
	if err := verifyAll(nv, concurrent, recordCount, p); err != nil {
		t.Error(err)
	}
}

func TestVolume_Maintain_closeOnCompacting(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	closed := make(chan error)
	logch := make(chan string)
	go func() {
		for s := range logch {
			if strings.Contains(s, "compacting") {
				closed <- v.Close()
				return
			}
		}
		closed <- errors.New("failed to close")
	}()

	if err := v.Maintain(ctx, 2, logch); err != context.Canceled && err != volume.ErrClosed {
		t.Errorf("wanna: %v or %v, got: %v", context.Canceled, volume.ErrClosed, err)
		return
	}

	if err := <-closed; err != nil {
		t.Fatal(err)
	}

	nv, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer nv.Close()
	// verify data
	if err := verifyAll(nv, concurrent, recordCount, p); err != nil {
		t.Error(err)
	}
}

func TestVolume_Maintain_cancelOnDumping(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Remove(); err != nil {
			t.Error(err)
		}
	}()

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	logch := make(chan string)
	go func() {
		for s := range logch {
			if strings.Contains(s, "dumping") {
				cancel()
				return
			}
		}
		t.Errorf("failed to cancel")
	}()
	if err := v.Maintain(ctx, 2, logch); err != context.Canceled {
		t.Errorf("wanna %s, got: %v", context.Canceled.Error(), err)
		return
	}
	// verify data
	if err := verifyAll(v, concurrent, recordCount, p); err != nil {
		t.Error(err)
		return
	}
}

func TestVolume_Maintain_cancelOnCompacting(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Remove(); err != nil {
			t.Error(err)
		}
	}()

	concurrent := 10
	recordCount := int64(100000)

	p := newObjectProp1()

	// prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	logch := make(chan string)
	go func() {
		for s := range logch {
			if strings.Contains(s, "compacting") {
				cancel()
				return
			}
		}
		t.Errorf("failed to cancel")
	}()
	if err := v.Maintain(ctx, 2, logch); err != context.Canceled {
		t.Errorf("wanna %s, got: %v", context.Canceled.Error(), err)
		return
	}
	// verify data
	if err := verifyAll(v, concurrent, recordCount, p); err != nil {
		t.Error(err)
		return
	}
}

// read, write, delete, maintain concurrently
func TestVolume_Maintain_whenRunning(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir(*tmpDir, "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)
	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Remove(); err != nil {
			t.Error(err)
		}
	}()

	concurrent := 10
	insertStart := int64(0)
	recordCount := int64(100000)
	insertCount := recordCount - insertStart - 1
	operationCount := int64(60000)
	qps := 10000

	limiter := rate.NewLimiter(rate.Every(time.Second/time.Duration(qps)), 10)

	p := newObjectProp1()

	// step 1: prefill data
	if err := prefill(v, concurrent, int(recordCount), p); err != nil {
		t.Fatal(err)
	}

	insertSequence := generator.NewAcknowledgedCounter(recordCount)
	keyChooser := generator.NewUniform(0, insertStart+insertCount-1)

	operationChooser := generator.NewDiscrete()
	operationChooser.Add(0.5, opReadAll)
	operationChooser.Add(0.5, opWriteAll)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 2: simulating read and write
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(ctx context.Context, i int) {
			defer wg.Done()
			defer cancel()
			r := mrand.New(mrand.NewSource(int64(time.Now().Nanosecond())))
			var err error
			for j := int64(0); j < operationCount; j++ {
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				operation := operationChooser.Next(r)
				switch operation {
				case opReadAll:
					keyNum := int64(math.MaxInt64)
					for keyNum > insertSequence.Last() {
						keyNum = keyChooser.Next(r)
					}
					if keyNum > recordCount {
						panic("impossible")
					}
					err = readAll(v, keyNum, p)
				case opWriteAll:
					err = writeAll(v, insertSequence, r, p)
				case opStat:
					fallthrough
				case opDelete:
					fallthrough
				default:
					fmt.Println("no implemented")
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(ctx, i)
	}

	// step 3: run disk maintainence
	if err := v.Maintain(ctx, 2, nil); err != nil {
		t.Error(err)
	}
	cancel()
	wg.Wait()

	// step 4: verify all written data
	if err := verifyAll(v, concurrent, insertSequence.Last(), p); err != nil {
		t.Error(err)
	}
}

const (
	opReadAll = iota + 1
	opReadFile
	opReadFileStream
	opWriteAll
	opDelete
	opList
	opStat
	opStatDir
)

func hash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(n))
	hash := fnv.New64a()
	hash.Write(b[0:8])
	result := int64(hash.Sum64())
	if result < 0 {
		return -result
	}
	return result
}

func bytesHash64(b []byte) int64 {
	hash := fnv.New64a()
	hash.Write(b)
	return int64(hash.Sum64())
}

func stringHash64(s string) int64 {
	hash := fnv.New64a()
	hash.Write([]byte(s))
	return int64(hash.Sum64())
}

type objectProp struct {
	prefix         string
	zeroPadding    int
	directoryLevel int
	orderedInserts bool
	maxSize        int
	minSize        int
}

func newObjectProp1() objectProp {
	return objectProp{
		prefix:  "prefix",
		minSize: 100,
		maxSize: 1000,
	}
}

func buildKeyName(keyNum int64, p objectProp) string {
	if !p.orderedInserts {
		keyNum = hash64(keyNum)
	}

	return fmt.Sprintf("%s%0[3]*[2]d", p.prefix, keyNum, p.zeroPadding)
}

func buildDeterministicValue(key string, p objectProp) []byte {
	size := int64(deterministicLength(key, p.maxSize, p.minSize))
	buf := make([]byte, int(size+21))
	b := bytes.NewBuffer(buf[0:0])
	b.WriteString(key)
	for int64(b.Len()) < size {
		b.WriteByte(':')
		n := bytesHash64(b.Bytes())
		b.WriteString(strconv.FormatUint(uint64(n), 10))
	}
	b.Truncate(int(size))
	return b.Bytes()
}

func deterministicLength(key string, max, min int) int {
	if min >= max {
		return min
	}
	h := stringHash64(key)
	if h < 0 {
		h = -h
	}
	return min + int(h)%(max-min)
}

func prefill(v *volume.Volume, concurrent, count int, p objectProp) error {
	insertSequence := generator.NewCounter(0)
	r := mrand.New(mrand.NewSource(int64(time.Now().Nanosecond())))
	var wg sync.WaitGroup
	c := count / concurrent
	errch := make(chan error, concurrent)
	for j := 0; j < concurrent; j++ {
		wg.Add(1)
		if j == concurrent-1 {
			c = count - c*(concurrent-1)
		}
		go func(count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				if err := writeAll(v, insertSequence, r, p); err != nil {
					errch <- err
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errch)
	for err := range errch {
		return err
	}
	return nil
}

func verifyAll(v *volume.Volume, concurrent int, count int64, p objectProp) error {
	keyChooser := generator.NewSequential(0, count)
	var wg sync.WaitGroup
	errch := make(chan error, concurrent)
	for j := 0; j < concurrent; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for keyNum := keyChooser.Next(nil); keyNum < count; keyNum = keyChooser.Next(nil) {
				if err := readAll(v, keyNum, p); err != nil {
					errch <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errch)
	for err := range errch {
		return err
	}
	return nil
}

func readAll(v *volume.Volume, keyNum int64, p objectProp) error {
	keyName := buildKeyName(keyNum, p)
	bs, err := v.ReadAll(keyName)
	if err != nil {
		return err
	}
	expected := buildDeterministicValue(keyName, p)
	if !bytes.Equal(bs, expected) {
		return fmt.Errorf("corrupted %s", keyName)
	}
	return nil
}

func writeAll(v *volume.Volume, insertSequence ycsb.Generator, r *mrand.Rand, p objectProp) error {
	keyNum := insertSequence.Next(r)
	if s, ok := insertSequence.(*(generator.AcknowledgedCounter)); ok {
		defer s.Acknowledge(keyNum)
	}
	keyName := buildKeyName(keyNum, p)
	bs := buildDeterministicValue(keyName, p)
	if err := v.WriteAll(keyName, int64(len(bs)), bytes.NewBuffer(bs)); err != nil {
		return fmt.Errorf("failed to write %s: %s", keyName, err.Error())
	}
	return nil
}
