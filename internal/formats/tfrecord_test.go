package formats

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// The standard CRC-32C (Castagnoli) check value for "123456789". Guards against
// accidentally framing with the wrong polynomial (which round-trip alone, using
// the same wrong function on both ends, would not catch).
func TestTFRecord_crc32cCheckValue(t *testing.T) {
	if got := crc32c([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("crc32c check value = %#08x, want 0xE3069283 (wrong polynomial?)", got)
	}
}

func TestTFRecord_roundTrip(t *testing.T) {
	records := [][]byte{
		[]byte("first record"),
		{}, // empty payload is valid
		[]byte("a much longer third record with some bytes \x00\x01\x02"),
	}
	var buf bytes.Buffer
	w := NewTFRecordWriter(&buf)
	for _, r := range records {
		if err := w.Write(r); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if w.Count() != int64(len(records)) {
		t.Fatalf("Count = %d, want %d", w.Count(), len(records))
	}

	// Frame size is exactly 8 + 4 + len + 4 per record.
	var wantSize int
	for _, r := range records {
		wantSize += 16 + len(r)
	}
	if buf.Len() != wantSize {
		t.Fatalf("encoded size = %d, want %d", buf.Len(), wantSize)
	}

	r := NewTFRecordReader(&buf)
	var got [][]byte
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		got = append(got, rec)
	}
	if len(got) != len(records) {
		t.Fatalf("read %d records, want %d", len(got), len(records))
	}
	for i := range records {
		if !bytes.Equal(got[i], records[i]) {
			t.Fatalf("record %d = %q, want %q", i, got[i], records[i])
		}
	}
}

func TestTFRecord_detectsPayloadCorruption(t *testing.T) {
	var buf bytes.Buffer
	NewTFRecordWriter(&buf).Write([]byte("payload-to-corrupt"))
	data := buf.Bytes()
	data[12] ^= 0xFF // first payload byte (after 12-byte header)

	if _, err := NewTFRecordReader(bytes.NewReader(data)).Read(); err == nil {
		t.Fatal("expected data crc mismatch, got nil")
	}
}

func TestTFRecord_detectsLengthCorruption(t *testing.T) {
	var buf bytes.Buffer
	NewTFRecordWriter(&buf).Write([]byte("hello"))
	data := buf.Bytes()
	data[0] ^= 0x01 // corrupt the length field

	if _, err := NewTFRecordReader(bytes.NewReader(data)).Read(); err == nil {
		t.Fatal("expected length crc mismatch, got nil")
	}
}

func TestTFRecord_truncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	NewTFRecordWriter(&buf).Write([]byte("complete record"))
	data := buf.Bytes()

	// Drop the trailing data CRC: a torn write.
	if _, err := NewTFRecordReader(bytes.NewReader(data[:len(data)-2])).Read(); err == nil {
		t.Fatal("expected error on truncated frame, got nil")
	}
}
