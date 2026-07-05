// Package formats writes algorithm outputs in the container formats ML
// frameworks consume directly: TFRecord for TensorFlow and WebDataset tar shards
// for PyTorch. Both are dependency-free (stdlib archive/tar, hash/crc32) and
// treat record payloads as opaque bytes — serialization of the payload itself
// (e.g. tf.train.Example protobuf) is the caller's concern.
package formats

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// TFRecord frames each record as, all little-endian:
//
//	uint64 length | uint32 masked_crc32c(length bytes) | payload | uint32 masked_crc32c(payload)
//
// The CRC is CRC32C (Castagnoli) run through TensorFlow's mask.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// maskCRC applies TensorFlow's rotate-and-offset mask, mirroring the C++
// implementation so files interoperate with tf.data.TFRecordDataset.
func maskCRC(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

// TFRecordWriter streams length-prefixed, CRC-checked records to an io.Writer.
type TFRecordWriter struct {
	w io.Writer
	count int64
}

func NewTFRecordWriter(w io.Writer) *TFRecordWriter {
	return &TFRecordWriter{w: w}
}

// Write appends one record. payload is opaque; the container only frames and
// checksums the bytes.
func (t *TFRecordWriter) Write(payload []byte) error {
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:12], maskCRC(crc32c(hdr[0:8])))
	if _, err := t.w.Write(hdr[:]); err != nil {
		return fmt.Errorf("tfrecord: write header: %w", err)
	}
	if _, err := t.w.Write(payload); err != nil {
		return fmt.Errorf("tfrecord: write payload: %w", err)
	}
	var foot [4]byte
	binary.LittleEndian.PutUint32(foot[:], maskCRC(crc32c(payload)))
	if _, err := t.w.Write(foot[:]); err != nil {
		return fmt.Errorf("tfrecord: write data crc: %w", err)
	}
	t.count++
	return nil
}

// Count returns how many records have been written.
func (t *TFRecordWriter) Count() int64 { return t.count }

// TFRecordReader reads back records written by TFRecordWriter, verifying both
// CRCs. It exists for round-trip validation and for re-reading platform output.
type TFRecordReader struct {
	r io.Reader
}

func NewTFRecordReader(r io.Reader) *TFRecordReader { return &TFRecordReader{r: r} }

// Read returns the next record, or io.EOF at a clean end of stream. A truncated
// frame or a CRC mismatch is reported as an error.
func (t *TFRecordReader) Read() ([]byte, error) {
	var hdr [12]byte
	if _, err := io.ReadFull(t.r, hdr[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("tfrecord: read header: %w", err)
	}
	if maskCRC(crc32c(hdr[0:8])) != binary.LittleEndian.Uint32(hdr[8:12]) {
		return nil, fmt.Errorf("tfrecord: length crc mismatch")
	}
	length := binary.LittleEndian.Uint64(hdr[0:8])
	payload := make([]byte, length)
	if _, err := io.ReadFull(t.r, payload); err != nil {
		return nil, fmt.Errorf("tfrecord: read payload (%d bytes): %w", length, err)
	}
	var foot [4]byte
	if _, err := io.ReadFull(t.r, foot[:]); err != nil {
		return nil, fmt.Errorf("tfrecord: read data crc: %w", err)
	}
	if maskCRC(crc32c(payload)) != binary.LittleEndian.Uint32(foot[:]) {
		return nil, fmt.Errorf("tfrecord: data crc mismatch")
	}
	return payload, nil
}
