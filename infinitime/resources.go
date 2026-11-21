package infinitime

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"
	"strings"

	"go.elara.ws/itd/internal/fsproto"
)

type ResourceOperation int

const (
	// ResourceUpload represents the upload phase
	// of resource loading
	ResourceUpload = iota
	// ResourceRemove represents the obsolete
	// file removal phase of resource loading
	ResourceRemove
	// ResourceCheck represents checking a destination file
	ResourceCheck
	// ResourceSkip represents skipping an unchanged destination file
	ResourceSkip
)

// ResourceLoadMode controls how [LoadResourcesWithMode] decides which files to upload.
type ResourceLoadMode string

const (
	// ResourceLoadModeAuto dynamically selects size+crc when supported by the watch,
	// falling back to delete+copy on older firmware without CRC support.
	ResourceLoadModeAuto ResourceLoadMode = "auto"
	// ResourceLoadModeDeleteCopy removes existing target files and uploads all resources unconditionally.
	ResourceLoadModeDeleteCopy ResourceLoadMode = "delete+copy"
	// ResourceLoadModeSizeOnly skips uploading files whose remote size matches the archive entry.
	ResourceLoadModeSizeOnly ResourceLoadMode = "size-only"
	// ResourceLoadModeSizeCRC skips uploading files whose remote size and IEEE CRC-32 match the archive entry.
	ResourceLoadModeSizeCRC ResourceLoadMode = "size+crc"
)

var (
	// ErrCRCUnsupported is returned when size+crc mode is requested against firmware without CRC support.
	ErrCRCUnsupported = errors.New("target device does not support BLE FS CRC32 checksums")
	// ErrInvalidResourceMode is returned when an unknown resource load mode string is given.
	ErrInvalidResourceMode = errors.New("invalid resource load mode")
)

// ParseResourceLoadMode parses a CLI/RPC resource load mode string.
// Empty string maps to [ResourceLoadModeAuto].
func ParseResourceLoadMode(s string) (ResourceLoadMode, error) {
	switch ResourceLoadMode(s) {
	case "", ResourceLoadModeAuto:
		return ResourceLoadModeAuto, nil
	case ResourceLoadModeDeleteCopy:
		return ResourceLoadModeDeleteCopy, nil
	case ResourceLoadModeSizeOnly:
		return ResourceLoadModeSizeOnly, nil
	case ResourceLoadModeSizeCRC:
		return ResourceLoadModeSizeCRC, nil
	default:
		return "", fmt.Errorf("%w: %q (valid modes: delete+copy, size-only, size+crc)", ErrInvalidResourceMode, s)
	}
}

// resourceManifest is the structure of the resource manifest file
type resourceManifest struct {
	Resources []resource         `json:"resources"`
	Obsolete  []obsoleteResource `json:"obsolete_files"`
}

// resource represents a resource entry in the manifest
type resource struct {
	Name string `json:"filename"`
	Path string `json:"path"`
}

// obsoleteResource represents an obsolete file entry in the manifest
type obsoleteResource struct {
	Path  string `json:"path"`
	Since string `json:"since"`
}

// ResourceLoadProgress contains information on the progress of
// a resource load
type ResourceLoadProgress struct {
	Operation   ResourceOperation
	Name        string
	Total       uint32
	Transferred uint32
}

// LoadResources accepts the path of an InfiniTime resource archive and loads its contents to the watch's filesystem.
func LoadResources(archivePath string, fs *FS, progress func(ResourceLoadProgress)) error {
	return LoadResourcesWithMode(archivePath, fs, ResourceLoadModeAuto, progress)
}

// LoadResourcesWithMode accepts the path of an InfiniTime resource archive and loads its contents
// to the watch's filesystem using the specified sync mode.
func LoadResourcesWithMode(archivePath string, fs *FS, mode ResourceLoadMode, progress func(ResourceLoadProgress)) error {
	parsedMode, err := ParseResourceLoadMode(string(mode))
	if err != nil {
		return err
	}

	emit := func(evt ResourceLoadProgress) {
		if progress != nil {
			progress(evt)
		}
	}

	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return err
	}
	defer r.Close()

	manifestFl, err := r.Open("resources.json")
	if err != nil {
		return err
	}

	var manifest resourceManifest
	err = json.NewDecoder(manifestFl).Decode(&manifest)
	if err != nil {
		_ = manifestFl.Close()
		return err
	}

	err = manifestFl.Close()
	if err != nil {
		return err
	}

	var effectiveMode ResourceLoadMode
	switch parsedMode {
	case ResourceLoadModeDeleteCopy:
		effectiveMode = ResourceLoadModeDeleteCopy
	case ResourceLoadModeSizeOnly:
		effectiveMode = ResourceLoadModeSizeOnly
	case ResourceLoadModeSizeCRC:
		supported, err := fs.SupportsCRC()
		if err != nil {
			return err
		}
		if !supported {
			return ErrCRCUnsupported
		}
		effectiveMode = ResourceLoadModeSizeCRC
	case ResourceLoadModeAuto:
		supported, err := fs.SupportsCRC()
		if err != nil {
			return err
		}
		if supported {
			effectiveMode = ResourceLoadModeSizeCRC
		} else {
			effectiveMode = ResourceLoadModeDeleteCopy
		}
	}

	state := newWatchFSState(fs)

	for _, file := range manifest.Obsolete {
		obsPath := cleanWatchPath(file.Path)
		if obsPath == "/" {
			continue
		}

		if err := state.ensureDirScanned(path.Dir(obsPath)); err != nil {
			return err
		}

		_, isFile := state.files[obsPath]
		isDir := state.knownDirs[obsPath]
		if !isFile && !isDir {
			continue
		}

		emit(ResourceLoadProgress{
			Operation: ResourceRemove,
			Name:      path.Base(obsPath),
		})

		if isDir {
			if err := fs.RemoveAll(obsPath); err != nil {
				return err
			}
			state.removeSubtree(obsPath)
		} else {
			if err := removeIfExists(fs, obsPath); err != nil {
				return err
			}
			delete(state.files, obsPath)
		}
	}

	for _, file := range manifest.Resources {
		dstPath := cleanWatchPath(file.Path)

		expectedSize, expectedCRC, err := zipEntrySizeAndCRC32(r, file.Name)
		if err != nil {
			return err
		}

		if err := state.ensureDirScanned(path.Dir(dstPath)); err != nil {
			return err
		}

		actualSize, exists := state.files[dstPath]

		needsUpload := true
		switch effectiveMode {
		case ResourceLoadModeDeleteCopy:
			needsUpload = true
		case ResourceLoadModeSizeOnly:
			emit(ResourceLoadProgress{
				Operation: ResourceCheck,
				Name:      file.Name,
				Total:     expectedSize,
			})
			if exists && actualSize == expectedSize {
				needsUpload = false
			}
		case ResourceLoadModeSizeCRC:
			emit(ResourceLoadProgress{
				Operation: ResourceCheck,
				Name:      file.Name,
				Total:     expectedSize,
			})
			if exists && actualSize == expectedSize {
				remoteSize, remoteCRC, err := fs.ChecksumFile(dstPath)
				if err != nil {
					var fsErr fsproto.Error
					if errors.As(err, &fsErr) && fsErr.Code == -2 {
						exists = false
						delete(state.files, dstPath)
					} else {
						return err
					}
				} else if remoteSize == expectedSize && remoteCRC == expectedCRC {
					needsUpload = false
				}
			}
		}

		if !needsUpload {
			emit(ResourceLoadProgress{
				Operation:   ResourceSkip,
				Name:        file.Name,
				Total:       expectedSize,
				Transferred: expectedSize,
			})
			continue
		}

		if err := state.ensureDirCreated(path.Dir(dstPath)); err != nil {
			return err
		}

		if exists {
			emit(ResourceLoadProgress{
				Operation: ResourceRemove,
				Name:      file.Name,
			})
			if err := removeIfExists(fs, dstPath); err != nil {
				return err
			}
			delete(state.files, dstPath)
		}

		src, err := r.Open(file.Name)
		if err != nil {
			return err
		}

		dst, err := fs.Create(dstPath, expectedSize)
		if err != nil {
			_ = src.Close()
			return err
		}

		emit(ResourceLoadProgress{
			Operation:   ResourceUpload,
			Name:        file.Name,
			Transferred: 0,
			Total:       expectedSize,
		})

		dst.ProgressFunc = func(transferred, total uint32) {
			emit(ResourceLoadProgress{
				Operation:   ResourceUpload,
				Name:        file.Name,
				Transferred: transferred,
				Total:       total,
			})
		}

		_, err = io.Copy(dst, src)
		if err != nil {
			return errors.Join(
				err,
				src.Close(),
				dst.Close(),
			)
		}

		if err := src.Close(); err != nil {
			_ = dst.Close()
			return err
		}

		if err := dst.Close(); err != nil {
			return err
		}

		emit(ResourceLoadProgress{
			Operation:   ResourceUpload,
			Name:        file.Name,
			Transferred: expectedSize,
			Total:       expectedSize,
		})

		state.files[dstPath] = expectedSize
	}

	return nil
}

func cleanWatchPath(p string) string {
	return path.Clean("/" + strings.TrimLeft(p, "/"))
}

func removeIfExists(fs *FS, p string) error {
	err := fs.Remove(p)
	if err == nil {
		return nil
	}
	var fsErr fsproto.Error
	if errors.As(err, &fsErr) && fsErr.Code == -2 {
		return nil
	}
	return err
}

func zipEntrySizeAndCRC32(r *zip.ReadCloser, name string) (uint32, uint32, error) {
	rc, err := r.Open(name)
	if err != nil {
		return 0, 0, err
	}
	defer rc.Close()

	return streamSizeAndCRC32(rc)
}

func streamSizeAndCRC32(r io.Reader) (uint32, uint32, error) {
	h := crc32.NewIEEE()
	n, err := io.Copy(h, r)
	if err != nil {
		return 0, 0, err
	}
	return uint32(n), h.Sum32(), nil
}

type watchFSState struct {
	fs          *FS
	scannedDirs map[string]bool
	knownDirs   map[string]bool
	files       map[string]uint32
}

func newWatchFSState(fs *FS) *watchFSState {
	return &watchFSState{
		fs:          fs,
		scannedDirs: make(map[string]bool),
		knownDirs:   map[string]bool{"/": true},
		files:       make(map[string]uint32),
	}
}

func (s *watchFSState) ensureDirScanned(dir string) error {
	dir = cleanWatchPath(dir)
	if s.scannedDirs[dir] {
		return nil
	}

	if dir != "/" {
		if err := s.ensureDirScanned(path.Dir(dir)); err != nil {
			return err
		}
		if !s.knownDirs[dir] {
			s.scannedDirs[dir] = true
			return nil
		}
	}

	entries, err := s.fs.ReadDir(dir)
	if err != nil {
		var fsErr fsproto.Error
		if errors.As(err, &fsErr) && fsErr.Code == -2 {
			delete(s.knownDirs, dir)
			s.scannedDirs[dir] = true
			return nil
		}
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if name == "" || name == "." || name == ".." {
			continue
		}
		entryPath := path.Join(dir, name)
		if entry.IsDir() {
			s.knownDirs[entryPath] = true
		} else {
			fi, err := entry.Info()
			if err != nil {
				return err
			}
			s.files[entryPath] = uint32(fi.Size())
		}
	}

	s.scannedDirs[dir] = true
	return nil
}

func (s *watchFSState) ensureDirCreated(dir string) error {
	dir = cleanWatchPath(dir)
	if dir == "/" || s.knownDirs[dir] {
		return nil
	}

	parent := path.Dir(dir)
	if err := s.ensureDirCreated(parent); err != nil {
		return err
	}
	if err := s.ensureDirScanned(parent); err != nil {
		return err
	}
	if s.knownDirs[dir] {
		return nil
	}

	err := s.fs.Mkdir(dir)
	var fsErr fsproto.Error
	alreadyExists := errors.As(err, &fsErr) && fsErr.Code == -17
	if err != nil && !alreadyExists {
		return err
	}

	s.knownDirs[dir] = true
	if !alreadyExists {
		s.scannedDirs[dir] = true
	}
	return nil
}

func (s *watchFSState) removeSubtree(dir string) {
	dir = cleanWatchPath(dir)
	prefix := dir + "/"
	for d := range s.knownDirs {
		if d == dir || strings.HasPrefix(d, prefix) {
			delete(s.knownDirs, d)
		}
	}
	for d := range s.scannedDirs {
		if d == dir || strings.HasPrefix(d, prefix) {
			delete(s.scannedDirs, d)
		}
	}
	for f := range s.files {
		if strings.HasPrefix(f, prefix) {
			delete(s.files, f)
		}
	}
}
