package store

import (
	"errors"
	"io"
	"os"

	mmap "github.com/edsrzf/mmap-go"
)

const MMapSize int64 = 16 << 30
const MMapSize32 int64 = 1 << 31

type MMap struct {
	fileAppendPos int
	appendPos     int
	size          int
	file          *os.File
	mmap.MMap
}

func NewMMap(f *os.File) (*MMap, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	appendPos := stat.Size()
	var size int64 = MMapSize
	if int64(int(size)) != size {
		size = MMapSize32
	}
	if int64(int(appendPos)) != appendPos {
		return nil, errors.New("File is too large for arch")
	}
	data, err := mmap.MapRegion(f, int(size), mmap.COPY, mmap.ANON, 0)
	if err != nil {
		return nil, err
	}
	_, err = f.ReadAt(data[:appendPos], 0)
	if err != nil {
		return nil, err
	}
	return &MMap{fileAppendPos: int(appendPos), appendPos: int(appendPos), size: int(size), MMap: data, file: f}, nil
}

func (m *MMap) At(len int64, offset int64) ([]byte, error) {
	if offset < 0 || offset+len > int64(m.size) {
		return nil, ErrOutOfBounds
	}
	return m.MMap[offset : offset+len], nil
}

func (m *MMap) Write(into []byte) (int, error) {
	if int64(m.appendPos)+int64(len(into)) > int64(m.size) {
		return 0, ErrOutOfBounds
	}
	copy(m.MMap[int(m.appendPos):], into)
	m.appendPos += len(into)
	return len(into), nil
}

func (m *MMap) Flush() error {
	if m.fileAppendPos == m.appendPos {
		return nil
	}
	if _, err := m.file.Write(m.MMap[m.fileAppendPos:m.appendPos]); err != nil {
		return err
	}
	m.fileAppendPos = m.appendPos
	return nil
}

func (m *MMap) Sync() error {
	return m.file.Sync()
}

func (m *MMap) Close() error {
	if err := m.MMap.Unmap(); err != nil {
		return err
	}
	return m.file.Close()
}

var _ io.Writer = &MMap{}
var _ io.Closer = &MMap{}
