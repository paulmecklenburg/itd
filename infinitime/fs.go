package infinitime

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"go.elara.ws/itd/internal/fsproto"
	"tinygo.org/x/bluetooth"
)

const fsResponseTimeout = 10 * time.Second

// FS represents a remote BLE filesystem
type FS struct {
	mtx sync.Mutex
	dev *Device
}

// Stat gets information about a file at the given path.
//
// WARNING: Since there's no stat command in the BLE FS protocol,
// this function does a ReadDir and then finds the requested file
// in the results, which makes it pretty slow.
func (ifs *FS) Stat(p string) (fs.FileInfo, error) {
	dir := path.Dir(p)
	entries, err := ifs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.Name() == path.Base(p) {
			return entry.Info()
		}
	}

	return nil, fsproto.ErrFileNotExists
}

// Remove removes a file or empty directory at the given path.
//
// For a function that removes directories recursively, see [FS.RemoveAll]
func (ifs *FS) Remove(path string) error {
	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return err
	}

	return ifs.requestThenAwaitResponse(
		char,
		fsproto.DeleteFileOpcode,
		fsproto.DeleteFileRequest{
			PathLen: uint16(len(path)),
			Path:    path,
		},
		func(buf []byte) (bool, error) {
			var mdr fsproto.DeleteFileResponse
			return true, fsproto.ReadResponse(buf, fsproto.DeleteFileResp, &mdr)
		},
	)
}

// Rename moves a file or directory from an old path to a new path.
func (ifs *FS) Rename(old, new string) error {
	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return err
	}

	return ifs.requestThenAwaitResponse(
		char,
		fsproto.MoveFileOpcode,
		fsproto.MoveFileRequest{
			OldPathLen: uint16(len(old)),
			OldPath:    old,
			NewPathLen: uint16(len(new)),
			NewPath:    new,
		},
		func(buf []byte) (bool, error) {
			var mfr fsproto.MoveFileResponse
			return true, fsproto.ReadResponse(buf, fsproto.MoveFileResp, &mfr)
		},
	)
}

// Mkdir creates a new directory at the specified path.
//
// For a function that creates necessary parents as well, see [FS.MkdirAll]
func (ifs *FS) Mkdir(path string) error {
	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return err
	}

	return ifs.requestThenAwaitResponse(
		char,
		fsproto.MakeDirectoryOpcode,
		fsproto.MkdirRequest{
			PathLen: uint16(len(path)),
			Path:    path,
		},
		func(buf []byte) (bool, error) {
			var mdr fsproto.MkdirResponse
			return true, fsproto.ReadResponse(buf, fsproto.MakeDirectoryResp, &mdr)
		},
	)
}

// ReadDir reads the directory at the specified path and returns a list of directory entries.
func (ifs *FS) ReadDir(path string) ([]fs.DirEntry, error) {
	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return nil, err
	}

	var out []fs.DirEntry
	return out, ifs.requestThenAwaitResponse(
		char,
		fsproto.ListDirectoryOpcode,
		fsproto.ListDirRequest{
			PathLen: uint16(len(path)),
			Path:    path,
		},
		func(buf []byte) (bool, error) {
			var ldr fsproto.ListDirResponse
			err := fsproto.ReadResponse(buf, fsproto.ListDirectoryResp, &ldr)
			if err != nil {
				return true, err
			}

			if ldr.EntryNum == ldr.TotalEntries {
				return true, nil
			}

			out = append(out, DirEntry{
				flags:   ldr.Flags,
				modtime: ldr.ModTime,
				size:    ldr.FileSize,
				path:    string(ldr.Path),
			})

			return false, nil
		},
	)
}

// ChecksumFile queries the watch for the size and IEEE CRC-32 of the file at path p.
func (ifs *FS) ChecksumFile(p string) (uint32, uint32, error) {
	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return 0, 0, err
	}

	var resp fsproto.ChecksumFileResponse
	err = ifs.requestThenAwaitResponse(
		char,
		fsproto.ChecksumFileOpcode,
		fsproto.ChecksumFileRequest{
			PathLen: uint16(len(p)),
			Path:    p,
		},
		func(buf []byte) (bool, error) {
			if len(buf) >= 2 && fsproto.FSRespOpcode(buf[0]) == fsproto.ChecksumFileResp && int8(buf[1]) != 0x01 {
				return true, fsproto.Error{Code: int8(buf[1])}
			}
			return true, fsproto.ReadResponse(buf, fsproto.ChecksumFileResp, &resp)
		},
	)
	if err != nil {
		return 0, 0, err
	}

	return resp.FileSize, resp.CRC32, nil
}

// SupportsCRC reports whether the connected watch supports the BLE FS CRC-32 command (0x70).
func (ifs *FS) SupportsCRC() (bool, error) {
	switch ifs.dev.fsCRCSupport.Load() {
	case 1:
		return true, nil
	case -1:
		return false, nil
	}

	// Verify BLE FS is available on the connected watch before probing opcode 0x70
	// so missing-characteristic or disconnect errors are not misclassified as lack of CRC support.
	if _, err := ifs.ReadDir("/"); err != nil {
		return false, err
	}

	ifs.mtx.Lock()
	defer ifs.mtx.Unlock()

	switch ifs.dev.fsCRCSupport.Load() {
	case 1:
		return true, nil
	case -1:
		return false, nil
	}

	char, err := ifs.dev.getChar(fsTransferChar)
	if err != nil {
		return false, err
	}

	if err := ifs.ensureNotifications(char); err != nil {
		return false, err
	}

	for len(ifs.dev.fsNotifyCh) > 0 {
		<-ifs.dev.fsNotifyCh
	}

	if err := fsproto.WriteRequest(char, fsproto.ChecksumFileOpcode, fsproto.ChecksumFileRequest{
		PathLen: 1,
		Path:    "/",
	}); err != nil {
		return false, err
	}

	disconnectCh := ifs.dev.getDisconnectCh()
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()

	select {
	case buf := <-ifs.dev.fsNotifyCh:
		if len(buf) >= 2 && fsproto.FSRespOpcode(buf[0]) == fsproto.ChecksumFileResp && int8(buf[1]) != 0x04 {
			ifs.dev.fsCRCSupport.Store(1)
			return true, nil
		}
		ifs.dev.fsCRCSupport.Store(-1)
		return false, nil
	case <-disconnectCh:
		ifs.dev.fsNotifyEnabled.Store(false)
		return false, errors.New("device disconnected during filesystem operation")
	case <-timer.C:
		if !ifs.dev.IsConnected() {
			return false, errors.New("device disconnected during filesystem operation")
		}
		ifs.dev.fsCRCSupport.Store(-1)
		return false, nil
	}
}

// RemoveAll removes the file at the specified path and any children it contains,
// similar to the rm -r command.
func (ifs *FS) RemoveAll(p string) error {
	if p == "" {
		return nil
	}

	if path.Clean(p) == "/" {
		return fsproto.ErrNoRemoveRoot
	}

	fi, err := ifs.Stat(p)
	if err != nil {
		return nil
	}

	if fi.IsDir() {
		return ifs.removeWithChildren(p)
	} else {
		err = ifs.Remove(p)

		var code int8
		if err, ok := err.(fsproto.Error); ok {
			code = err.Code
		}

		if err != nil && code != -2 {
			return err
		}
	}

	return nil
}

// removeWithChildren removes the directory at the given path and its children recursively.
func (ifs *FS) removeWithChildren(p string) error {
	list, err := ifs.ReadDir(p)
	if err != nil {
		return err
	}

	for _, entry := range list {
		name := entry.Name()

		if name == "." || name == ".." {
			continue
		}
		entryPath := path.Join(p, name)

		if entry.IsDir() {
			err = ifs.removeWithChildren(entryPath)
		} else {
			err = ifs.Remove(entryPath)
		}

		var code int8
		if err, ok := err.(fsproto.Error); ok {
			code = err.Code
		}

		if err != nil && code != -2 {
			return err
		}
	}

	return ifs.Remove(p)
}

// MkdirAll creates a directory and any necessary parents in the file system,
// similar to the mkdir -p command.
func (ifs *FS) MkdirAll(path string) error {
	if path == "" || path == "/" {
		return nil
	}

	splitPath := strings.Split(path, "/")
	for i := 1; i < len(splitPath); i++ {
		curPath := strings.Join(splitPath[0:i+1], "/")

		err := ifs.Mkdir(curPath)

		var code int8
		if err, ok := err.(fsproto.Error); ok {
			code = err.Code
		}

		if err != nil && code != -17 {
			return err
		}
	}

	return nil
}

var _ fs.File = (*File)(nil)

// File represents a remote file on a BLE filesystem.
//
// If ProgressFunc is set, it will be called whenever a read or write happens
// with the amount of bytes transferred and the total size of the file.
type File struct {
	fs           *FS
	path         string
	offset       uint32
	size         uint32
	readOnly     bool
	closed       bool
	ProgressFunc func(transferred, total uint32)
}

// Open opens an existing file at the specified path.
// It returns a handle for the file and an error, if any.
func (ifs *FS) Open(path string) (*File, error) {
	return &File{
		fs:       ifs,
		path:     path,
		offset:   0,
		readOnly: true,
	}, nil
}

// Create creates a new file with the specified path and size.
// It returns a handle for the created file and an error, if any.
func (ifs *FS) Create(path string, size uint32) (*File, error) {
	return &File{
		fs:     ifs,
		path:   path,
		offset: 0,
		size:   size,
	}, nil
}

// Write writes data from the byte slice b to the file.
// It returns the number of bytes written and an error, if any.
func (fl *File) Write(b []byte) (int, error) {
	if fl.closed {
		return 0, fsproto.ErrFileClosed
	}

	if fl.readOnly {
		return 0, fsproto.ErrFileReadOnly
	}

	fl.fs.mtx.Lock()
	defer fl.fs.mtx.Unlock()

	char, err := fl.fs.dev.getChar(fsTransferChar)
	if err != nil {
		return 0, err
	}

	if err := fl.fs.ensureNotifications(char); err != nil {
		return 0, err
	}

	for len(fl.fs.dev.fsNotifyCh) > 0 {
		<-fl.fs.dev.fsNotifyCh
	}

	var chunkLen uint32

	dataLen := uint32(len(b))
	transferred := uint32(0)
	mtu := uint32(fl.fs.mtu(char))

	err = fsproto.WriteRequest(char, fsproto.WriteFileHeaderOpcode, fsproto.WriteFileHeaderRequest{
		PathLen:  uint16(len(fl.path)),
		Offset:   fl.offset,
		FileSize: fl.size,
		Path:     fl.path,
	})
	if err != nil {
		return 0, err
	}

	for {
		buf, err := fl.fs.awaitNotification()
		if err != nil {
			return int(transferred), err
		}
		var wfr fsproto.WriteFileResponse
		err = fsproto.ReadResponse(buf, fsproto.WriteFileResp, &wfr)
		if err != nil {
			return int(transferred), err
		}

		transferred += chunkLen
		fl.offset += chunkLen

		if wfr.FreeSpace == 0 || transferred == dataLen {
			return int(transferred), nil
		}

		if fl.ProgressFunc != nil {
			fl.ProgressFunc(fl.offset, fl.size)
		}

		amountLeft := dataLen - transferred
		chunkLen = mtu
		if amountLeft < mtu {
			chunkLen = amountLeft
		}

		err = fsproto.WriteRequest(char, fsproto.WriteFileOpcode, fsproto.WriteFileRequest{
			Status:   0x01,
			Offset:   fl.offset,
			ChunkLen: chunkLen,
			Data:     b[transferred : transferred+chunkLen],
		})
		if err != nil {
			return int(transferred), err
		}
	}
}

// Read reads data from the file into the byte slice b.
// It returns the number of bytes read and an error, if any.
func (fl *File) Read(b []byte) (int, error) {
	if fl.closed {
		return 0, fsproto.ErrFileClosed
	}

	fl.fs.mtx.Lock()
	defer fl.fs.mtx.Unlock()

	char, err := fl.fs.dev.getChar(fsTransferChar)
	if err != nil {
		return 0, err
	}

	if err := fl.fs.ensureNotifications(char); err != nil {
		return 0, err
	}

	for len(fl.fs.dev.fsNotifyCh) > 0 {
		<-fl.fs.dev.fsNotifyCh
	}

	transferred := uint32(0)
	maxLen := uint32(len(b))
	mtu := uint32(fl.fs.mtu(char))

	amountLeft := maxLen - transferred
	chunkLen := mtu
	if amountLeft < mtu {
		chunkLen = amountLeft
	}

	err = fsproto.WriteRequest(char, fsproto.ReadFileHeaderOpcode, fsproto.ReadFileHeaderRequest{
		PathLen: uint16(len(fl.path)),
		Offset:  fl.offset,
		ReadLen: chunkLen,
		Path:    fl.path,
	})
	if err != nil {
		return 0, err
	}

	for {
		buf, err := fl.fs.awaitNotification()
		if err != nil {
			return int(transferred), err
		}
		var rfr fsproto.ReadFileResponse
		err = fsproto.ReadResponse(buf, fsproto.ReadFileResp, &rfr)
		if err != nil {
			return int(transferred), err
		}

		fl.size = rfr.FileSize

		if rfr.Offset == rfr.FileSize || rfr.ChunkLen == 0 {
			return int(transferred), io.EOF
		}

		n := copy(b[transferred:], rfr.Data[:rfr.ChunkLen])
		fl.offset += uint32(n)
		transferred += uint32(n)

		if fl.ProgressFunc != nil {
			fl.ProgressFunc(fl.offset, rfr.FileSize)
		}

		if transferred == maxLen {
			return int(transferred), nil
		}

		amountLeft = maxLen - transferred
		chunkLen = mtu
		if amountLeft < mtu {
			chunkLen = amountLeft
		}

		err = fsproto.WriteRequest(char, fsproto.ReadFileOpcode, fsproto.ReadFileRequest{
			Status:  0x01,
			Offset:  fl.offset,
			ReadLen: chunkLen,
		})
		if err != nil {
			return int(transferred), err
		}
	}
}

// Stat returns information about the file,
func (fl *File) Stat() (fs.FileInfo, error) {
	return fl.fs.Stat(fl.path)
}

// Seek sets the offset for the next Read or Write on the file to the specified offset.
// The whence parameter specifies the seek reference point:
//
//	io.SeekStart: offset is relative to the start of the file.
//	io.SeekCurrent: offset is relative to the current offset.
//	io.SeekEnd: offset is relative to the end of the file.
//
// Seek returns the new offset and an error, if any.
func (fl *File) Seek(offset int64, whence int) (int64, error) {
	if fl.closed {
		return 0, fsproto.ErrFileClosed
	}

	if offset > math.MaxUint32 {
		return 0, fsproto.ErrInvalidOffset
	}
	u32Offset := uint32(offset)

	fl.fs.mtx.Lock()
	defer fl.fs.mtx.Unlock()

	if fl.size == 0 {
		return 0, errors.New("file size unknown")
	}

	var newOffset uint32
	switch whence {
	case io.SeekStart:
		newOffset = u32Offset
	case io.SeekCurrent:
		newOffset = fl.offset + u32Offset
	case io.SeekEnd:
		newOffset = fl.size + u32Offset
	}

	if newOffset > fl.size || newOffset < 0 {
		return 0, fsproto.ErrInvalidOffset
	}
	fl.offset = newOffset

	return int64(fl.offset), nil
}

// Close closes the file for future operations
func (fl *File) Close() error {
	fl.fs.mtx.Lock()
	defer fl.fs.mtx.Unlock()
	fl.closed = true
	return nil
}

func (ifs *FS) awaitNotification() ([]byte, error) {
	disconnectCh := ifs.dev.getDisconnectCh()
	timer := time.NewTimer(fsResponseTimeout)
	defer timer.Stop()

	select {
	case buf := <-ifs.dev.fsNotifyCh:
		return buf, nil
	case <-disconnectCh:
		ifs.dev.fsNotifyEnabled.Store(false)
		return nil, errors.New("device disconnected during filesystem operation")
	case <-timer.C:
		return nil, errors.New("filesystem response timeout")
	}
}

func (ifs *FS) ensureNotifications(char *bluetooth.DeviceCharacteristic) error {
	if ifs.dev.fsNotifyEnabled.Load() {
		return nil
	}

	ifs.dev.fsNotifyMtx.Lock()
	defer ifs.dev.fsNotifyMtx.Unlock()

	if ifs.dev.fsNotifyEnabled.Load() {
		return nil
	}

	err := ifs.dev.enableNotifications(fsTransferChar, char, func(buf []byte) {
		b := make([]byte, len(buf))
		copy(b, buf)
		select {
		case ifs.dev.fsNotifyCh <- b:
		default:
		}
	})

	if err == nil {
		ifs.dev.fsNotifyEnabled.Store(true)
	}

	return err
}

// requestThenAwaitResponse executes a BLE FS request and then waits for one or more responses,
// until fn returns true or an error is encountered.
func (ifs *FS) requestThenAwaitResponse(char *bluetooth.DeviceCharacteristic, opcode fsproto.FSReqOpcode, req any, fn func(buf []byte) (bool, error)) error {
	if err := ifs.ensureNotifications(char); err != nil {
		return err
	}

	for len(ifs.dev.fsNotifyCh) > 0 {
		<-ifs.dev.fsNotifyCh
	}

	err := fsproto.WriteRequest(char, opcode, req)
	if err != nil {
		return err
	}

	for {
		buf, err := ifs.awaitNotification()
		if err != nil {
			return err
		}
		stop, err := fn(buf)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
}

func (ifs *FS) mtu(char *bluetooth.DeviceCharacteristic) uint16 {
	mtuVal, _ := char.GetMTU()
	if mtuVal == 0 {
		mtuVal = 256
	}
	return mtuVal - 20
}

var _ fs.FS = (*GoFS)(nil)
var _ fs.StatFS = (*GoFS)(nil)
var _ fs.ReadDirFS = (*GoFS)(nil)

// GoFS implements [io/fs.FS], [io/fs.StatFS], and [io/fs.ReadDirFS]
// for the InfiniTime filesystem
type GoFS struct {
	*FS
}

// Open opens an existing file at the specified path.
// It returns a handle for the file and an error, if any.
func (gfs GoFS) Open(path string) (fs.File, error) {
	return gfs.FS.Open(path)
}
