package cmd

//
// import (
// 	"bytes"
// 	"io"
// 	"io/ioutil"
// 	"os"
// 	slashpath "path"
// 	"runtime"
// 	"strings"
// 	"syscall"
// 	"testing"
// )
//
// func newFileVolumePosixTestSetup() (StorageAPI, string, error) {
// 	diskPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
// 	if err != nil {
// 		return nil, "", err
// 	}
// 	globalFileVolumeEnabled = true
// 	globalFileVolumes, err = initGlobalFileVolumes()
// 	if err != nil {
// 		return nil, "", err
// 	}
// 	// Initialize a new posix layer.
// 	posixStorage, err := newPosix(diskPath)
// 	if err != nil {
// 		return nil, "", err
// 	}
// 	return posixStorage, diskPath, nil
// }
//
// func TestFileVolumePosixMakeVol(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	// Setup test environment.
// 	// Create a file.
// 	if err := ioutil.WriteFile(slashpath.Join(path, "vol-as-file"), []byte{}, os.ModePerm); err != nil {
// 		t.Fatalf("Unable to create file, %s", err)
// 	}
// 	// Create a directory.
// 	if err := os.Mkdir(slashpath.Join(path, "existing-vol"), 0777); err != nil {
// 		t.Fatalf("Unable to create directory, %s", err)
// 	}
//
// 	testCases := []struct {
// 		volName     string
// 		ioErrCount  int
// 		expectedErr error
// 	}{
// 		// TestPosix case - 1.
// 		// A valid case, volume creation is expected to succeed.
// 		{
// 			volName:     "success-vol",
// 			ioErrCount:  0,
// 			expectedErr: nil,
// 		},
// 		// TestPosix case - 2.
// 		// Case where a file exists by the name of the volume to be created.
// 		{
// 			volName:     "vol-as-file",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeExists,
// 		},
// 		// TestPosix case - 3.
// 		{
// 			volName:     "existing-vol",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeExists,
// 		},
// 		// TestPosix case - 4.
// 		// IO error > maxAllowedIOError, should fail with errFaultyDisk.
// 		{
// 			volName:     "vol",
// 			ioErrCount:  6,
// 			expectedErr: errFaultyDisk,
// 		},
// 		// TestPosix case - 5.
// 		// TestPosix case with invalid volume name.
// 		{
// 			volName:     "ab",
// 			ioErrCount:  0,
// 			expectedErr: errInvalidArgument,
// 		},
// 	}
//
// 	for i, testCase := range testCases {
// 		if posixType, ok := posixStorage.(*posix); ok {
// 			// setting the io error count from as specified in the test case.
// 			posixType.ioErrCount = int32(testCase.ioErrCount)
// 		} else {
// 			t.Errorf("Expected the StorageAPI to be of type *posix")
// 		}
// 		if err := posixStorage.MakeVol(testCase.volName); err != testCase.expectedErr {
// 			t.Fatalf("TestPosix %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
// 		}
// 	}
// }
//
// func TestFileVolumePosixDeleteVol(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	// Setup test environment.
// 	if err = posixStorage.MakeVol("success-vol"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
//
// 	// TestPosix failure cases.
// 	vol := slashpath.Join(path, "nonempty-vol")
// 	if err = os.Mkdir(vol, 0777); err != nil {
// 		t.Fatalf("Unable to create directory, %s", err)
// 	}
// 	if err = ioutil.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
// 		t.Fatalf("Unable to create file, %s", err)
// 	}
//
// 	testCases := []struct {
// 		volName     string
// 		ioErrCount  int
// 		expectedErr error
// 	}{
// 		// TestPosix case  - 1.
// 		// A valida case. Empty vol, should be possible to delete.
// 		{
// 			volName:     "success-vol",
// 			ioErrCount:  0,
// 			expectedErr: nil,
// 		},
// 		// TestPosix case - 2.
// 		// volume is non-existent.
// 		{
// 			volName:     "nonexistent-vol",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 		// we can delete an entire volume
// 		// TestPosix case - 3.
// 		// It shouldn't be possible to delete an non-empty volume, validating the same.
// 		// {
// 		// 	volName:     "nonempty-vol",
// 		// 	ioErrCount:  0,
// 		// 	expectedErr: errVolumeNotEmpty,
// 		// },
// 		// TestPosix case - 4.
// 		// IO error > maxAllowedIOError, should fail with errFaultyDisk.
// 		{
// 			volName:     "my-disk",
// 			ioErrCount:  6,
// 			expectedErr: errFaultyDisk,
// 		},
// 		// TestPosix case - 5.
// 		// Invalid volume name.
// 		{
// 			volName:     "ab",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 	}
//
// 	for i, testCase := range testCases {
// 		if posixType, ok := posixStorage.(*posix); ok {
// 			// setting the io error count from as specified in the test case.
// 			posixType.ioErrCount = int32(testCase.ioErrCount)
// 		} else {
// 			t.Errorf("Expected the StorageAPI to be of type *posix")
// 		}
// 		if err = posixStorage.DeleteVol(testCase.volName); err != testCase.expectedErr {
// 			t.Fatalf("TestPosix: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
// 		}
// 	}
//
// 	posixDeletedStorage, diskPath, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	// removing the disk, used to recreate disk not found error.
// 	os.RemoveAll(diskPath)
//
// 	// TestPosix for delete on an removed disk.
// 	// should fail with disk not found.
// 	err = posixDeletedStorage.DeleteVol("Del-Vol")
// 	if err != errDiskNotFound {
// 		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
// 	}
// }
//
// func TestFileVolumePosixStatVol(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	// Setup test environment.
// 	if err = posixStorage.MakeVol("success-vol"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
//
// 	testCases := []struct {
// 		volName     string
// 		ioErrCount  int
// 		expectedErr error
// 	}{
// 		// TestPosix case - 1.
// 		{
// 			volName:     "success-vol",
// 			ioErrCount:  0,
// 			expectedErr: nil,
// 		},
// 		// TestPosix case - 2.
// 		{
// 			volName:     "nonexistent-vol",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 		// TestPosix case - 3.
// 		{
// 			volName:     "success-vol",
// 			ioErrCount:  6,
// 			expectedErr: errFaultyDisk,
// 		},
// 		// TestPosix case - 4.
// 		{
// 			volName:     "ab",
// 			ioErrCount:  0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 	}
//
// 	for i, testCase := range testCases {
// 		var volInfo VolInfo
// 		// setting ioErrCnt from the test case.
// 		if posixType, ok := posixStorage.(*posix); ok {
// 			// setting the io error count from as specified in the test case.
// 			posixType.ioErrCount = int32(testCase.ioErrCount)
// 		} else {
// 			t.Errorf("Expected the StorageAPI to be of type *posix")
// 		}
// 		volInfo, err = posixStorage.StatVol(testCase.volName)
// 		if err != testCase.expectedErr {
// 			t.Fatalf("TestPosix case : %d, Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
// 		}
//
// 		if err == nil {
// 			if volInfo.Name != volInfo.Name {
// 				t.Errorf("TestPosix case %d: Expected the volume name to be \"%s\", instead found \"%s\"", i+1, volInfo.Name, volInfo.Name)
// 			}
// 		}
// 	}
//
// 	posixDeletedStorage, diskPath, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	// removing the disk, used to recreate disk not found error.
// 	os.RemoveAll(diskPath)
//
// 	// TestPosix for delete on an removed disk.
// 	// should fail with disk not found.
// 	_, err = posixDeletedStorage.StatVol("Stat vol")
// 	if err != errDiskNotFound {
// 		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
// 	}
// }
//
// func TestFileVolumePosixListVols(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
//
// 	var volInfo []VolInfo
// 	// TestPosix empty list vols.
// 	if volInfo, err = posixStorage.ListVols(); err != nil {
// 		t.Fatalf("expected: <nil>, got: %s", err)
// 	} else if len(volInfo) != 0 {
// 		t.Fatalf("expected: [], got: %s", volInfo)
// 	}
//
// 	// TestPosix non-empty list vols.
// 	if err = posixStorage.MakeVol("success-vol"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
// 	if volInfo, err = posixStorage.ListVols(); err != nil {
// 		t.Fatalf("expected: <nil>, got: %s", err)
// 	} else if len(volInfo) != 1 {
// 		t.Fatalf("expected: 1, got: %d", len(volInfo))
// 	} else if volInfo[0].Name != "success-vol" {
// 		t.Errorf("expected: success-vol, got: %s", volInfo[0].Name)
// 	}
// 	// setting ioErrCnt to be > maxAllowedIOError.
// 	// should fail with errFaultyDisk.
// 	if posixType, ok := posixStorage.(*posix); ok {
// 		// setting the io error count from as specified in the test case.
// 		posixType.ioErrCount = int32(6)
// 	} else {
// 		t.Errorf("Expected the StorageAPI to be of type *posix")
// 	}
// 	if _, err = posixStorage.ListVols(); err != errFaultyDisk {
// 		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errFaultyDisk, err)
// 	}
// 	// removing the path and simulating disk failure
// 	os.RemoveAll(path)
// 	// Resetting the IO error.
// 	// should fail with errDiskNotFound.
// 	if posixType, ok := posixStorage.(*posix); ok {
// 		// setting the io error count from as specified in the test case.
// 		posixType.ioErrCount = int32(0)
// 	} else {
// 		t.Errorf("Expected the StorageAPI to be of type *posix")
// 	}
// 	if _, err = posixStorage.ListVols(); err != errDiskNotFound {
// 		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errDiskNotFound, err)
// 	}
// }
//
// func TestFileVolumePosixPosixListDir(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	// create posix test setup.
// 	posixDeletedStorage, diskPath, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	// removing the disk, used to recreate disk not found error.
// 	os.RemoveAll(diskPath)
// 	// Setup test environment.
// 	if err = posixStorage.MakeVol("success-vol"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
// 	if err = posixStorage.AppendFile("success-vol", "abc/def/ghi/success-file", []byte("Hello, world")); err != nil {
// 		t.Fatalf("Unable to create file, %s", err)
// 	}
// 	if err = posixStorage.AppendFile("success-vol", "abc/xyz/ghi/success-file", []byte("Hello, world")); err != nil {
// 		t.Fatalf("Unable to create file, %s", err)
// 	}
//
// 	testCases := []struct {
// 		srcVol   string
// 		srcPath  string
// 		ioErrCnt int
// 		// expected result.
// 		expectedListDir []string
// 		expectedErr     error
// 	}{
// 		// TestPosix case - 1.
// 		// valid case with existing volume and file to delete.
// 		{
// 			srcVol:          "success-vol",
// 			srcPath:         "abc",
// 			ioErrCnt:        0,
// 			expectedListDir: []string{"def/", "xyz/"},
// 			expectedErr:     nil,
// 		},
// 		// TestPosix case - 1.
// 		// valid case with existing volume and file to delete.
// 		{
// 			srcVol:          "success-vol",
// 			srcPath:         "abc/def",
// 			ioErrCnt:        0,
// 			expectedListDir: []string{"ghi/"},
// 			expectedErr:     nil,
// 		},
// 		// TestPosix case - 1.
// 		// valid case with existing volume and file to delete.
// 		{
// 			srcVol:          "success-vol",
// 			srcPath:         "abc/def/ghi",
// 			ioErrCnt:        0,
// 			expectedListDir: []string{"success-file"},
// 			expectedErr:     nil,
// 		},
// 		// TestPosix case - 2.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "abcdef",
// 			ioErrCnt:    0,
// 			expectedErr: errFileNotFound,
// 		},
// 		// TestPosix case - 3.
// 		// TestPosix case with invalid volume name.
// 		{
// 			srcVol:      "ab",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 		// TestPosix case - 4.
// 		// TestPosix case with io error count > max limit.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    6,
// 			expectedErr: errFaultyDisk,
// 		},
// 		// TestPosix case - 5.
// 		// TestPosix case with non existent volume.
// 		{
// 			srcVol:      "non-existent-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 	}
//
// 	for i, testCase := range testCases {
// 		var dirList []string
// 		// setting ioErrCnt from the test case.
// 		if posixType, ok := posixStorage.(*posix); ok {
// 			// setting the io error count from as specified in the test case.
// 			posixType.ioErrCount = int32(testCase.ioErrCnt)
// 		} else {
// 			t.Errorf("Expected the StorageAPI to be of type *posix")
// 		}
// 		dirList, err = posixStorage.ListDir(testCase.srcVol, testCase.srcPath, -1, "")
// 		if err != testCase.expectedErr {
// 			t.Fatalf("TestPosix case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
// 		}
// 		if err == nil {
// 			for _, expected := range testCase.expectedListDir {
// 				if !strings.Contains(strings.Join(dirList, ","), expected) {
// 					t.Errorf("TestPosix case %d: Expected the directory listing to be \"%v\", but got \"%v\"", i+1, testCase.expectedListDir, dirList)
// 				}
// 			}
// 		}
// 	}
//
// 	// TestPosix for permission denied.
// 	if runtime.GOOS != globalWindowsOSName {
// 		permDeniedDir := createPermDeniedFile(t)
// 		defer removePermDeniedFile(permDeniedDir)
//
// 		// Initialize posix storage layer for permission denied error.
// 		_, err = newPosix(permDeniedDir)
// 		if err != nil && !os.IsPermission(err) {
// 			t.Fatalf("Unable to initialize posix, %s", err)
// 		}
//
// 		if err = os.Chmod(permDeniedDir, 0755); err != nil {
// 			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
// 		}
//
// 		posixStorage, err = newPosix(permDeniedDir)
// 		if err != nil {
// 			t.Fatalf("Unable to initialize posix, %s", err)
// 		}
//
// 		if err = posixStorage.DeleteFile("mybucket", "myobject"); err != errFileAccessDenied {
// 			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
// 		}
// 	}
//
// 	// TestPosix for delete on an removed disk.
// 	// should fail with disk not found.
// 	err = posixDeletedStorage.DeleteFile("del-vol", "my-file")
// 	if err != errDiskNotFound {
// 		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
// 	}
// }
//
// // TestPosixDeleteFile - Series of test cases construct valid and invalid input data and validates the result and the error response.
// func TestFileVolumePosixDeleteFile(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	// create posix test setup
// 	posixDeletedStorage, diskPath, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	// removing the disk, used to recreate disk not found error.
// 	os.RemoveAll(diskPath)
// 	// Setup test environment.
// 	if err = posixStorage.MakeVol("success-vol"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
// 	if err = posixStorage.AppendFile("success-vol", "success-file", []byte("Hello, world")); err != nil {
// 		t.Fatalf("Unable to create file, %s", err)
// 	}
//
// 	if err = posixStorage.MakeVol("no-permissions"); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err.Error())
// 	}
// 	if err = posixStorage.AppendFile("no-permissions", "dir/file", []byte("Hello, world")); err != nil {
// 		t.Fatalf("Unable to create file, %s", err.Error())
// 	}
// 	// Parent directory must have write permissions, this is read + execute.
// 	if err = os.Chmod(pathJoin(path, "no-permissions"), 0555); err != nil {
// 		t.Fatalf("Unable to chmod directory, %s", err.Error())
// 	}
//
// 	testCases := []struct {
// 		srcVol      string
// 		srcPath     string
// 		ioErrCnt    int
// 		expectedErr error
// 	}{
// 		// TestPosix case - 1.
// 		// valid case with existing volume and file to delete.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: nil,
// 		},
// 		// TestPosix case - 2.
// 		// The file was deleted in the last  case, so DeleteFile should fail.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: errFileNotFound,
// 		},
// 		// TestPosix case - 3.
// 		// TestPosix case with io error count > max limit.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    6,
// 			expectedErr: errFaultyDisk,
// 		},
// 		// TestPosix case - 4.
// 		// TestPosix case with segment of the volume name > 255.
// 		{
// 			srcVol:      "my",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 		// TestPosix case - 5.
// 		// TestPosix case with non-existent volume.
// 		{
// 			srcVol:      "non-existent-vol",
// 			srcPath:     "success-file",
// 			ioErrCnt:    0,
// 			expectedErr: errVolumeNotFound,
// 		},
// 		// TestPosix case - 6.
// 		// TestPosix case with src path segment > 255.
// 		{
// 			srcVol:      "success-vol",
// 			srcPath:     "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
// 			ioErrCnt:    0,
// 			expectedErr: errFileNameTooLong,
// 		},
// 		// TestPosix case - 7.
// 		// TestPosix case with undeletable parent directory.
// 		// File can delete, dir cannot delete because no-permissions doesn't have write perms.
// 		{
// 			srcVol:      "no-permissions",
// 			srcPath:     "dir/file",
// 			ioErrCnt:    0,
// 			expectedErr: nil,
// 		},
// 	}
//
// 	for i, testCase := range testCases {
// 		// setting ioErrCnt from the test case.
// 		if posixType, ok := posixStorage.(*posix); ok {
// 			// setting the io error count from as specified in the test case.
// 			posixType.ioErrCount = int32(testCase.ioErrCnt)
// 		} else {
// 			t.Errorf("Expected the StorageAPI to be of type *posix")
// 		}
// 		if err = posixStorage.DeleteFile(testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
// 			t.Errorf("TestPosix case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
// 		}
// 	}
//
// 	// TestPosix for delete on an removed disk.
// 	// should fail with disk not found.
// 	err = posixDeletedStorage.DeleteFile("del-vol", "my-file")
// 	if err != errDiskNotFound {
// 		t.Errorf("Expected: \"Disk not found\", got \"%s\"", err)
// 	}
// }
//
// // TestPosixReadAll - TestPosixs the functionality implemented by posix ReadAll storage API.
// func TestFileVolumePosixReadAll(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
//
// 	defer os.RemoveAll(path)
//
// 	// Create files for the test cases.
// 	if err = posixStorage.MakeVol("exists"); err != nil {
// 		t.Fatalf("Unable to create a volume \"exists\", %s", err)
// 	}
// 	if err = posixStorage.AppendFile("exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
// 		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
// 	}
// 	if err = posixStorage.AppendFile("exists", "as-file", []byte("Hello, World")); err != nil {
// 		t.Fatalf("Unable to create a file \"as-file\", %s", err)
// 	}
// 	if err = posixStorage.AppendFile("exists", "as-file-parent", []byte("Hello, World")); err != nil {
// 		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
// 	}
//
// 	// TestPosixcases to validate different conditions for ReadAll API.
// 	testCases := []struct {
// 		volume string
// 		path   string
// 		err    error
// 	}{
// 		// TestPosix case - 1.
// 		// Validate volume does not exist.
// 		{
// 			volume: "i-dont-exist",
// 			path:   "",
// 			err:    errVolumeNotFound,
// 		},
// 		// TestPosix case - 2.
// 		// Validate bad condition file does not exist.
// 		{
// 			volume: "exists",
// 			path:   "as-file-not-found",
// 			err:    errFileNotFound,
// 		},
// 		// TestPosix case - 3.
// 		// Validate bad condition file exists as prefix/directory and
// 		// we are attempting to read it.
// 		{
// 			volume: "exists",
// 			path:   "as-directory",
// 			err:    errFileNotFound,
// 		},
// 		// TestPosix case - 4.
// 		{
// 			volume: "exists",
// 			path:   "as-file-parent/as-file",
// 			err:    errFileNotFound,
// 		},
// 		// TestPosix case - 5.
// 		// Validate the good condition file exists and we are able to read it.
// 		{
// 			volume: "exists",
// 			path:   "as-file",
// 			err:    nil,
// 		},
// 		// TestPosix case - 6.
// 		// TestPosix case with invalid volume name.
// 		{
// 			volume: "ab",
// 			path:   "as-file",
// 			err:    errVolumeNotFound,
// 		},
// 	}
//
// 	var dataRead []byte
// 	// Run through all the test cases and validate for ReadAll.
// 	for i, testCase := range testCases {
// 		dataRead, err = posixStorage.ReadAll(testCase.volume, testCase.path)
// 		if err != testCase.err {
// 			t.Fatalf("TestPosix %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
// 		}
// 		if err == nil {
// 			if string(dataRead) != string([]byte("Hello, World")) {
// 				t.Errorf("TestPosix %d: Expected the data read to be \"%s\", but instead got \"%s\"", i+1, "Hello, World", string(dataRead))
// 			}
// 		}
// 	}
// 	// TestPosixing for faulty disk.
// 	// Setting ioErrCount > maxAllowedIOError.
// 	if posixType, ok := posixStorage.(*posix); ok {
// 		// setting the io error count from as specified in the test case.
// 		posixType.ioErrCount = int32(6)
// 	} else {
// 		t.Errorf("Expected the StorageAPI to be of type *posix")
// 	}
// 	_, err = posixStorage.ReadAll("abcd", "efg")
// 	if err != errFaultyDisk {
// 		t.Errorf("Expected err \"%s\", got err \"%s\"", errFaultyDisk, err)
// 	}
// }
//
// func TestFileVolumePosixReadFile(t *testing.T) {
// 	// create posix test setup
// 	posixStorage, path, err := newFileVolumePosixTestSetup()
// 	if err != nil {
// 		t.Fatalf("Unable to create posix test setup, %s", err)
// 	}
// 	defer os.RemoveAll(path)
//
// 	volume := "success-vol"
// 	// Setup test environment.
// 	if err = posixStorage.MakeVol(volume); err != nil {
// 		t.Fatalf("Unable to create volume, %s", err)
// 	}
//
// 	// Create directory to make errIsNotRegular
// 	if err = posixStorage.MakeVol(slashpath.Join("success-vol", "object-as-dir")); err != nil {
// 		t.Fatalf("Unable to create directory, %s", err)
// 	}
//
// 	testCases := []struct {
// 		volume      string
// 		fileName    string
// 		offset      int64
// 		bufSize     int
// 		expectedBuf []byte
// 		expectedErr error
// 	}{
// 		// Successful read at offset 0 and proper buffer size. - 1
// 		{
// 			volume, "myobject", 0, 5,
// 			[]byte("hello"), nil,
// 		},
// 		// Success read at hierarchy. - 2
// 		{
// 			volume, "path/to/my/object", 0, 5,
// 			[]byte("hello"), nil,
// 		},
// 		// Object is a directory. - 3
// 		{
// 			volume, "object-as-dir",
// 			// 0, 5, nil, errIsNotRegular},
// 			0, 5, nil, nil}, // in rocksdb directory and file with same name will exist at the same time
// 		// One path segment length is > 255 chars long. - 4
// 		{
// 			volume, "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
// 			// 0, 5, nil, errFileNameTooLong},
// 			0, 5, nil, nil}, // no limit on file name
// 		// Path length is > 1024 chars long. - 5
// 		{
// 			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
// 			0, 5, nil, errFileNameTooLong},
// 		// Buffer size greater than object size. - 6
// 		{
// 			volume, "myobject", 0, 16,
// 			[]byte("hello, world"),
// 			io.ErrUnexpectedEOF,
// 		},
// 		// Reading from an offset success. - 7
// 		{
// 			volume, "myobject", 7, 5,
// 			[]byte("world"), nil,
// 		},
// 		// Reading from an object but buffer size greater. - 8
// 		{
// 			volume, "myobject",
// 			7, 8,
// 			[]byte("world"),
// 			io.ErrUnexpectedEOF,
// 		},
// 		// Seeking ahead returns io.EOF. - 9
// 		{
// 			volume, "myobject", 14, 1, nil, io.EOF,
// 		},
// 		// Empty volume name. - 10
// 		{
// 			"", "myobject", 14, 1, nil, errVolumeNotFound,
// 		},
// 		// Empty filename name. - 11
// 		{
// 			volume, "", 14, 1, nil, errIsNotRegular,
// 		},
// 		// Non existent volume name - 12
// 		{
// 			"abcd", "", 14, 1, nil, errVolumeNotFound,
// 		},
// 		// Non existent filename - 13
// 		{
// 			volume, "abcd", 14, 1, nil, errFileNotFound,
// 		},
// 	}
//
// 	// Create all files needed during testing.
// 	appendFiles := testCases[:4]
// 	v := NewBitrotVerifier(SHA256, getSHA256Sum([]byte("hello, world")))
// 	// Create test files for further reading.
// 	for i, appendFile := range appendFiles {
// 		err = posixStorage.AppendFile(volume, appendFile.fileName, []byte("hello, world"))
// 		if err != appendFile.expectedErr {
// 			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
// 		}
// 	}
//
// 	{
// 		buf := make([]byte, 5)
// 		// Test for negative offset.
// 		if _, err = posixStorage.ReadFile(volume, "myobject", -1, buf, v); err == nil {
// 			t.Fatalf("expected: error, got: <nil>")
// 		}
// 	}
//
// 	// Following block validates all ReadFile test cases.
// 	for i, testCase := range testCases {
// 		var n int64
// 		// Common read buffer.
// 		var buf = make([]byte, testCase.bufSize)
// 		n, err = posixStorage.ReadFile(testCase.volume, testCase.fileName, testCase.offset, buf, v)
// 		if err != nil && testCase.expectedErr != nil {
// 			// Validate if the type string of the errors are an exact match.
// 			if err.Error() != testCase.expectedErr.Error() {
// 				if runtime.GOOS != globalWindowsOSName {
// 					t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
// 				} else {
// 					var resultErrno, expectErrno uintptr
// 					if pathErr, ok := err.(*os.PathError); ok {
// 						if errno, pok := pathErr.Err.(syscall.Errno); pok {
// 							resultErrno = uintptr(errno)
// 						}
// 					}
// 					if pathErr, ok := testCase.expectedErr.(*os.PathError); ok {
// 						if errno, pok := pathErr.Err.(syscall.Errno); pok {
// 							expectErrno = uintptr(errno)
// 						}
// 					}
// 					if !(expectErrno != 0 && resultErrno != 0 && expectErrno == resultErrno) {
// 						t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
// 					}
// 				}
// 			}
// 			// Err unexpected EOF special case, where we verify we have provided a larger
// 			// buffer than the data itself, but the results are in-fact valid. So we validate
// 			// this error condition specifically treating it as a good condition with valid
// 			// results. In this scenario return 'n' is always lesser than the input buffer.
// 			if err == io.ErrUnexpectedEOF {
// 				if !bytes.Equal(testCase.expectedBuf, buf[:n]) {
// 					t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:n]))
// 				}
// 				if n > int64(len(buf)) {
// 					t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
// 				}
// 			}
// 		}
// 		// ReadFile has returned success, but our expected error is non 'nil'.
// 		if err == nil && err != testCase.expectedErr {
// 			t.Errorf("Case: %d %#v, expected: %s, got :%s", i+1, testCase, testCase.expectedErr, err)
// 		}
// 		// Expected error retured, proceed further to validate the returned results.
// 		if err == nil && err == testCase.expectedErr {
// 			if !bytes.Equal(testCase.expectedBuf, buf) {
// 				t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:testCase.bufSize]))
// 			}
// 			if n != int64(testCase.bufSize) {
// 				t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
// 			}
// 		}
// 	}
//
// 	// TestPosixing for faulty disk.
// 	// setting ioErrCnt to 6.
// 	// should fail with errFaultyDisk.
// 	if posixType, ok := posixStorage.(*posix); ok {
// 		// setting the io error count from as specified in the test case.
// 		posixType.ioErrCount = int32(6)
// 		// Common read buffer.
// 		var buf = make([]byte, 10)
// 		_, err = posixType.ReadFile("abc", "yes", 0, buf, nil)
// 		if err != errFaultyDisk {
// 			t.Fatalf("Expected \"Faulty Disk\", got: \"%s\"", err)
// 		}
// 	} else {
// 		t.Fatalf("Expected the StorageAPI to be of type *posix")
// 	}
// }
