package formats

import (
	"bytes"
	"testing"
)

func sampleRows() []ResultRow {
	return []ResultRow{
		{TaskID: "t-1", Shard: "a3", OutputKey: "results/train/a3/t-1.json", ImagesProcessed: 3500, BytesRead: 1 << 30},
		{TaskID: "t-2", Shard: "7f", OutputKey: "results/train/7f/t-2.json", ImagesProcessed: 3480, BytesRead: 900 << 20},
		{TaskID: "t-3", Shard: "00", OutputKey: "results/train/00/t-3.json", ImagesProcessed: 0, BytesRead: 0},
	}
}

func TestParquet_roundTrip(t *testing.T) {
	rows := sampleRows()
	var buf bytes.Buffer
	if err := WriteResultsParquet(&buf, rows); err != nil {
		t.Fatalf("WriteResultsParquet: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("parquet output is empty")
	}
	// Parquet files start with the "PAR1" magic.
	if got := buf.Bytes()[:4]; !bytes.Equal(got, []byte("PAR1")) {
		t.Fatalf("missing PAR1 magic, got %q", got)
	}

	got, err := ReadResultsParquet(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadResultsParquet: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i] != rows[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], rows[i])
		}
	}
}

func TestParquet_empty(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteResultsParquet(&buf, nil); err != nil {
		t.Fatalf("WriteResultsParquet(nil): %v", err)
	}
	got, err := ReadResultsParquet(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadResultsParquet: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty write produced %d rows", len(got))
	}
}

func TestParquet_rejectsGarbage(t *testing.T) {
	if _, err := ReadResultsParquet([]byte("not a parquet file")); err == nil {
		t.Fatal("expected error reading non-parquet bytes")
	}
}
