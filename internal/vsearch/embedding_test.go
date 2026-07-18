package vsearch

import (
	"bytes"
	"testing"
)

func TestEmbeddingParquet_roundTrip(t *testing.T) {
	rows := []Embedding{
		{ID: "train/a3/cat_000001.jpg", Dataset: "train", Vector: []float32{0.1, -0.2, 0.3, 0.4}},
		{ID: "train/7f/dog_000002.jpg", Dataset: "train", Vector: []float32{-1, 0, 1, 0.5}},
		{ID: "train/00/car_000003.jpg", Dataset: "train", Vector: []float32{0, 0, 0, 1}},
	}
	var buf bytes.Buffer
	if err := WriteEmbeddingsParquet(&buf, 4, rows); err != nil {
		t.Fatalf("WriteEmbeddingsParquet: %v", err)
	}
	if got := buf.Bytes()[:4]; !bytes.Equal(got, []byte("PAR1")) {
		t.Fatalf("missing PAR1 magic, got %q", got)
	}

	dim, got, err := ReadEmbeddingsParquet(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadEmbeddingsParquet: %v", err)
	}
	if dim != 4 {
		t.Fatalf("dim = %d, want 4", dim)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i].ID != rows[i].ID || got[i].Dataset != rows[i].Dataset {
			t.Fatalf("row %d id/dataset = %+v, want %+v", i, got[i], rows[i])
		}
		for j := range rows[i].Vector {
			if got[i].Vector[j] != rows[i].Vector[j] {
				t.Fatalf("row %d vector = %v, want %v", i, got[i].Vector, rows[i].Vector)
			}
		}
	}
}

func TestEmbeddingParquet_roundTripsThroughIndex(t *testing.T) {
	rows := craftedRows()
	var buf bytes.Buffer
	if err := WriteEmbeddingsParquet(&buf, 2, rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	dim, got, err := ReadEmbeddingsParquet(buf.Bytes())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	ix, err := NewIndex(dim, got)
	if err != nil {
		t.Fatalf("NewIndex from parquet: %v", err)
	}
	res, err := ix.Search([]float32{1, 0}, 1)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if res[0].ID != "east" {
		t.Fatalf("nearest to east query = %q, want east", res[0].ID)
	}
}

func TestWriteEmbeddings_dimMismatch(t *testing.T) {
	var buf bytes.Buffer
	err := WriteEmbeddingsParquet(&buf, 4, []Embedding{{ID: "x", Vector: []float32{1, 2}}})
	if err == nil {
		t.Fatal("expected dim-mismatch error")
	}
}

func TestReadEmbeddings_rejectsGarbage(t *testing.T) {
	if _, _, err := ReadEmbeddingsParquet([]byte("not parquet")); err == nil {
		t.Fatal("expected error on non-parquet bytes")
	}
}
