package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestBytesReader_readsAllThenEOF(t *testing.T) {
	data := []byte("hello world, this is a multipart chunk")
	r := newBytesReader(data)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("read %q, want %q", got, data)
	}

	// Subsequent read must report EOF, not re-read.
	n, err := r.Read(make([]byte, 4))
	if n != 0 || err != io.EOF {
		t.Errorf("read after exhaustion = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestBytesReader_smallBuffer(t *testing.T) {
	data := []byte("abcdefghij")
	r := newBytesReader(data)

	var out []byte
	buf := make([]byte, 3) // smaller than data → multiple reads
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if string(out) != string(data) {
		t.Errorf("reassembled %q, want %q", out, data)
	}
}

func TestBytesReader_empty(t *testing.T) {
	r := newBytesReader(nil)
	n, err := r.Read(make([]byte, 8))
	if n != 0 || err != io.EOF {
		t.Errorf("empty reader = (%d, %v), want (0, EOF)", n, err)
	}
}
