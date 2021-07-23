package litestream

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

const (
	StreamRecordTypeSnapshot   = 1
	StreamRecordTypeWALSegment = 2
)

const StreamRecordHeaderSize = 4 + 4 + // type, flags
	8 + 8 + 8 + 8 // generation, index, offset, size

type StreamRecordHeader struct {
	Type       int
	Flags      int
	Generation string
	Index      int
	Offset     int64
	Size       int64
}

func (hdr *StreamRecordHeader) MarshalBinary() ([]byte, error) {
	generation, err := strconv.ParseUint(hdr.Generation, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid generation: %q", generation)
	}

	data := make([]byte, StreamRecordHeaderSize)
	binary.BigEndian.PutUint32(data[0:4], uint32(hdr.Type))
	binary.BigEndian.PutUint32(data[4:8], uint32(hdr.Flags))
	binary.BigEndian.PutUint64(data[8:16], generation)
	binary.BigEndian.PutUint64(data[16:24], uint64(hdr.Index))
	binary.BigEndian.PutUint64(data[24:32], uint64(hdr.Offset))
	binary.BigEndian.PutUint64(data[32:40], uint64(hdr.Size))
	return data, nil
}

// UnmarshalBinary from data into hdr.
func (hdr *StreamRecordHeader) UnmarshalBinary(data []byte) error {
	if len(data) < StreamRecordHeaderSize {
		return io.ErrUnexpectedEOF
	}
	hdr.Type = int(binary.BigEndian.Uint32(data[0:4]))
	hdr.Flags = int(binary.BigEndian.Uint32(data[4:8]))
	hdr.Generation = fmt.Sprintf("%16x", binary.BigEndian.Uint64(data[8:16]))
	hdr.Index = int(binary.BigEndian.Uint64(data[16:24]))
	hdr.Offset = int64(binary.BigEndian.Uint64(data[24:32]))
	hdr.Size = int64(binary.BigEndian.Uint64(data[32:40]))
	return nil
}

type StreamClient interface {
	Stream(ctx context.Context, pos Pos) (StreamReader, error)
}

type StreamReader interface {
	io.Reader
	io.Closer

	Next() (*StreamRecordHeader, error)
}
