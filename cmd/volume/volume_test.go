package volume_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
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
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"golang.org/x/time/rate"
	"gopkg.in/bufio.v1"
)

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

func TestVolumeAndFile(t *testing.T) {
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

func TestVolumeReadFile(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	defer os.RemoveAll(dir)

	v, err := volume.NewVolume(context.Background(), dir)
	if err != nil {
		t.Error(err)
		return
	}
	// defer v.Close()

	key := "key"
	data := []byte("0123456789")
	r := bufio.NewBuffer(data)
	if err := v.WriteAll(key, int64(len(data)), r); err != nil {
		t.Error(err)
		return
	}

	r = bufio.NewBuffer(data)
	if err := v.WriteAll("another KEY", int64(len(data)), r); err != nil {
		t.Error(err)
		return
	}

	data2, err := v.ReadAll(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(data, data2) {
		t.Errorf("not equal")
	}

	data3 := make([]byte, 5)
	n, err := v.ReadFile(key, 1, data3)
	if err != nil {
		t.Error(err)
	}
	if n != 5 {
		t.Errorf("not equal")
	}
	if !bytes.Equal(data[1:6], data3) {
		t.Errorf("not equal")
	}

	rr, err := v.ReadFileStream(key, 5, 6)
	if err != nil {
		t.Error(err)
		return
	}
	data4, err := ioutil.ReadAll(rr)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(data[5:], data4) {
		t.Errorf("not equal")
	}

	if err := v.Remove(); err != nil {
		t.Error(err)
	}
}

func TestVolumeList(t *testing.T) {
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

func TestDirOperation(t *testing.T) {
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

func TestVolumeMaintain(t *testing.T) {
	// volume.MaxFileSize = 4 << 20
	// size := int64(10 * 1024)
	// jobs := 32
	// objects := int(volume.MaxFileSize/size/int64(jobs)) + 10 // to generate a new file so that 0.data can be rewritten
	// dataMD5 := make([]string, jobs)
	//
	// dir, _ := ioutil.TempDir("/tmp", "volume_")
	// fmt.Println(dir)
	// // defer os.RemoveAll(dir)
	//
	// v, err := volume.NewVolume(context.Background(), dir)
	// if err != nil {
	// 	t.Error(err)
	// 	return
	// }
	// defer v.Close()
	// var wg sync.WaitGroup
	// for j := 0; j < jobs; j++ {
	// 	wg.Add(1)
	// 	data := make([]byte, size)
	// 	rand.Read(data)
	// 	dataMD5[j] = fmt.Sprintf("%x", md5.Sum(data))
	// 	go func(j int) {
	// 		defer wg.Done()
	// 		for i := 0; i < objects; i++ {
	// 			r := bufio.NewBuffer(data)
	// 			key := fmt.Sprintf("key-%d-%d", i, j)
	// 			err := v.WriteAll(key, size, r)
	// 			if err != nil {
	// 				t.Error(err)
	// 				continue
	// 			}
	// 		}
	// 	}(j)
	// }
	// wg.Wait()
	// before := time.Now()
	// if err := v.DumpListToMaintain(context.TODO(), 10); err != nil {
	// 	t.Error(err)
	// 	return
	// }
	//
	// // if err := v.DumpListToMaintain(context.TODO(), 1); err != volume.ErrUnderMaintenance {
	// // 	t.Error(err)
	// // 	return
	// // }
	//
	// if err := v.Maintain(context.TODO()); err != nil {
	// 	t.Error(err)
	// 	return
	// }
	// fmt.Println("total: ", time.Since(before))
	//
	// //
	// for j := 0; j < jobs; j++ {
	// 	wg.Add(1)
	// 	go func(j int) {
	// 		defer wg.Done()
	// 		for i := 0; i < objects; i++ {
	// 			key := fmt.Sprintf("key-%d-%d", i, j)
	// 			data, err := v.ReadAll(key)
	// 			if err != nil {
	// 				panic(err)
	// 			}
	// 			if dataMD5[j] != fmt.Sprintf("%x", md5.Sum(data)) {
	// 				panic(errors.New("invalid data"))
	// 			}
	// 		}
	// 	}(j)
	// }
	// wg.Wait()
}

func TestVolumeAll(t *testing.T) {
	volume.MaxFileSize = 4 << 20

	dir, _ := ioutil.TempDir("/tmp", "volume_")
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
	qps := 1000

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

	r := mrand.New(mrand.NewSource(int64(time.Now().Nanosecond())))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 2: simulating read and write
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(ctx context.Context, i int) {
			defer wg.Done()
			defer cancel()
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
	if err := v.Maintain(ctx, 2); err != nil {
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
