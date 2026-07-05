package formats

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

type tarEntry struct {
	name string
	data []byte
}

func readTar(t *testing.T, r io.Reader) []tarEntry {
	t.Helper()
	tr := tar.NewReader(r)
	var out []tarEntry
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read tar body: %v", err)
		}
		out = append(out, tarEntry{name: hdr.Name, data: data})
	}
	return out
}

func TestWebDataset_roundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewShardWriter(&buf)
	if err := w.WriteSample("img000001",
		Component{Ext: "jpg", Data: []byte("JPEGBYTES")},
		Component{Ext: "cls", Data: []byte("7")},
	); err != nil {
		t.Fatalf("WriteSample 1: %v", err)
	}
	if err := w.WriteSample("img000002",
		Component{Ext: "jpg", Data: []byte("MOREJPEG")},
		Component{Ext: "cls", Data: []byte("3")},
	); err != nil {
		t.Fatalf("WriteSample 2: %v", err)
	}
	if w.Samples() != 2 {
		t.Fatalf("Samples = %d, want 2", w.Samples())
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries := readTar(t, &buf)
	want := []tarEntry{
		{"img000001.cls", []byte("7")}, // components sorted by ext within a sample
		{"img000001.jpg", []byte("JPEGBYTES")},
		{"img000002.cls", []byte("3")},
		{"img000002.jpg", []byte("MOREJPEG")},
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i := range want {
		if entries[i].name != want[i].name {
			t.Fatalf("entry %d name = %q, want %q (sample grouping/order wrong)", i, entries[i].name, want[i].name)
		}
		if !bytes.Equal(entries[i].data, want[i].data) {
			t.Fatalf("entry %d data = %q, want %q", i, entries[i].data, want[i].data)
		}
	}
}

func TestWebDataset_deterministicOutput(t *testing.T) {
	build := func() []byte {
		var buf bytes.Buffer
		w := NewShardWriter(&buf)
		// Same sample, components supplied in different orders each call.
		w.WriteSample("s1", Component{Ext: "json", Data: []byte("{}")}, Component{Ext: "jpg", Data: []byte("X")})
		w.Close()
		return buf.Bytes()
	}
	build2 := func() []byte {
		var buf bytes.Buffer
		w := NewShardWriter(&buf)
		w.WriteSample("s1", Component{Ext: "jpg", Data: []byte("X")}, Component{Ext: "json", Data: []byte("{}")})
		w.Close()
		return buf.Bytes()
	}
	if !bytes.Equal(build(), build2()) {
		t.Fatal("shard output not deterministic across component input order")
	}
}

func TestWebDataset_rejectsBadInput(t *testing.T) {
	cases := map[string]func(*ShardWriter) error{
		"empty key": func(w *ShardWriter) error {
			return w.WriteSample("", Component{Ext: "jpg", Data: []byte("x")})
		},
		"no components": func(w *ShardWriter) error {
			return w.WriteSample("k")
		},
		"empty extension": func(w *ShardWriter) error {
			return w.WriteSample("k", Component{Ext: "", Data: []byte("x")})
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			w := NewShardWriter(&bytes.Buffer{})
			if err := fn(w); err == nil {
				t.Fatalf("%s: expected error, got nil", name)
			}
		})
	}
}

func TestWebDataset_rejectsDuplicateKey(t *testing.T) {
	w := NewShardWriter(&bytes.Buffer{})
	if err := w.WriteSample("dup", Component{Ext: "jpg", Data: []byte("a")}); err != nil {
		t.Fatalf("first WriteSample: %v", err)
	}
	if err := w.WriteSample("dup", Component{Ext: "cls", Data: []byte("1")}); err == nil {
		t.Fatal("expected duplicate key error")
	}
}
