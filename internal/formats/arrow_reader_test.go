package formats

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func sampleImages() []ImageRecord {
	return []ImageRecord{
		{Key: "train/a3/cat_007842.jpg", Width: 224, Height: 224, Label: "cat", Data: []byte("\xff\xd8jpeg1")},
		{Key: "train/7f/dog_002341.jpg", Width: 128, Height: 96, Label: "dog", Data: []byte("\xff\xd8jpeg2")},
		{Key: "train/00/bird_1.png", Width: 64, Height: 64, Label: "bird", Data: []byte("png3")},
	}
}

func TestArrow_buildImageBatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	imgs := sampleImages()

	rec := BuildImageBatch(alloc, imgs)
	if got := rec.NumRows(); got != int64(len(imgs)) {
		t.Fatalf("NumRows = %d, want %d", got, len(imgs))
	}
	if got := rec.NumCols(); got != 5 {
		t.Fatalf("NumCols = %d, want 5", got)
	}
	if !rec.Schema().Equal(ImageBatchSchema()) {
		t.Fatalf("schema mismatch:\n got %v\nwant %v", rec.Schema(), ImageBatchSchema())
	}

	keys := rec.Column(0).(*array.String)
	widths := rec.Column(1).(*array.Int32)
	heights := rec.Column(2).(*array.Int32)
	labels := rec.Column(3).(*array.String)
	data := rec.Column(4).(*array.Binary)
	for i, im := range imgs {
		if keys.Value(i) != im.Key || widths.Value(i) != im.Width ||
			heights.Value(i) != im.Height || labels.Value(i) != im.Label ||
			!bytes.Equal(data.Value(i), im.Data) {
			t.Fatalf("row %d = {%q %d %d %q %q}, want %+v",
				i, keys.Value(i), widths.Value(i), heights.Value(i), labels.Value(i), data.Value(i), im)
		}
	}

	rec.Release()
	alloc.AssertSize(t, 0) // every Arrow buffer must be released
}

func TestArrow_ipcRoundTrip(t *testing.T) {
	imgs := sampleImages()
	rec := BuildImageBatch(nil, imgs)
	defer rec.Release()

	encoded, err := EncodeIPC(rec)
	if err != nil {
		t.Fatalf("EncodeIPC: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("IPC stream is empty")
	}

	batches, err := DecodeIPC(encoded)
	if err != nil {
		t.Fatalf("DecodeIPC: %v", err)
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	var total int64
	for _, b := range batches {
		total += b.NumRows()
	}
	if total != int64(len(imgs)) {
		t.Fatalf("decoded %d rows across %d batches, want %d", total, len(batches), len(imgs))
	}

	first := batches[0]
	if got := first.Column(0).(*array.String).Value(0); got != imgs[0].Key {
		t.Fatalf("decoded key[0] = %q, want %q", got, imgs[0].Key)
	}
}

func TestArrow_emptyBatch(t *testing.T) {
	rec := BuildImageBatch(nil, nil)
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Fatalf("empty batch NumRows = %d, want 0", rec.NumRows())
	}
	encoded, err := EncodeIPC(rec)
	if err != nil {
		t.Fatalf("EncodeIPC empty: %v", err)
	}
	if _, err := DecodeIPC(encoded); err != nil {
		t.Fatalf("DecodeIPC empty: %v", err)
	}
}
