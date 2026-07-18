// Package vsearch is an exact nearest-neighbour image similarity search over CLIP
// embeddings. The corpus is a Parquet file of unit vectors (written by the Python
// CLIP encoder, or any producer honouring the schema); queries are resolved to a
// vector and ranked by cosine similarity against every row (brute force, O(N*dim)).
//
// Brute force is the right default here: at <=1M vectors an exact scan is a few
// hundred MB of contiguous float32 dot products -- fast, simple, and correct, with
// no index to build, tune, or keep consistent. An ANN index (HNSW/IVF) only earns
// its complexity well past that scale.
package vsearch

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Embedding is one image's CLIP vector plus its identity. Written columnar to
// Parquet so the corpus is directly queryable by Athena / DuckDB / Spark, and
// re-loadable by the Go k-NN engine without a separate index format.
type Embedding struct {
	ID string      // object key or filename -- the search result identity
	Dataset string  // logical dataset the image belongs to
	Vector []float32 // CLIP embedding; length must equal the file's dim
}

// embeddingSchema builds the Arrow/Parquet schema for a given vector width. The
// vector is a fixed-size list so the dimension is carried by the type itself --
// readers recover dim from the file without a side channel.
func embeddingSchema(dim int) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "dataset", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)
}

// WriteEmbeddingsParquet writes rows to w as a single-row-group Parquet file. Every
// row's Vector must have length dim; a mismatch is a programming error and fails fast.
func WriteEmbeddingsParquet(w io.Writer, dim int, rows []Embedding) error {
	if dim <= 0 {
		return fmt.Errorf("vsearch: dim must be positive, got %d", dim)
	}
	schema := embeddingSchema(dim)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	idB := b.Field(0).(*array.StringBuilder)
	dsB := b.Field(1).(*array.StringBuilder)
	vecB := b.Field(2).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	for i, r := range rows {
		if len(r.Vector) != dim {
			return fmt.Errorf("vsearch: row %d (%q) has vector len %d, want dim %d", i, r.ID, len(r.Vector), dim)
		}
		idB.Append(r.ID)
		dsB.Append(r.Dataset)
		vecB.Append(true) // open one fixed-size list element...
		valB.AppendValues(r.Vector, nil) // ...then fill its dim slots.
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	fw, err := pqarrow.NewFileWriter(schema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("vsearch: new parquet writer: %w", err)
	}
	if err := fw.Write(rec); err != nil {
		fw.Close()
		return fmt.Errorf("vsearch: write batch: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("vsearch: close parquet: %w", err)
	}
	return nil
}

// ReadEmbeddingsParquet parses a Parquet file produced by WriteEmbeddingsParquet
// back into rows, recovering the vector dimension from the file schema.
func ReadEmbeddingsParquet(data []byte) (dim int, rows []Embedding, err error) {
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return 0, nil, fmt.Errorf("vsearch: open parquet: %w", err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return 0, nil, fmt.Errorf("vsearch: arrow reader: %w", err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		return 0, nil, fmt.Errorf("vsearch: read table: %w", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 0)
	defer tr.Release()

	for tr.Next() {
		rec := tr.RecordBatch()
		id := rec.Column(0).(*array.String)
		ds := rec.Column(1).(*array.String)
		// Parquet stores our fixed-size vector as a repeated (variable-list) column, so
		// on read the column comes back as a List regardless of how it was written.
		// vecAt handles both list encodings and the width is recovered per row.
		vec := rec.Column(2)
		for i := range int(rec.NumRows()) {
			v, err := vecAt(vec, i)
			if err != nil {
				return 0, nil, err
			}
			if dim == 0 {
				dim = len(v)
			} else if len(v) != dim {
				return 0, nil, fmt.Errorf("vsearch: row has vector len %d, want dim %d (ragged corpus)", len(v), dim)
			}
			rows = append(rows, Embedding{ID: id.Value(i), Dataset: ds.Value(i), Vector: v})
		}
	}
	if err := tr.Err(); err != nil {
		return 0, nil, fmt.Errorf("vsearch: iterate table: %w", err)
	}
	return dim, rows, nil
}

// vecAt copies row i of a float32 vector column, accepting either a variable List
// (how Parquet round-trips it) or a FixedSizeList (an in-memory record).
func vecAt(col arrow.Array, i int) ([]float32, error) {
	switch c := col.(type) {
	case *array.List:
		vals := c.ListValues().(*array.Float32).Float32Values()
		start, end := c.ValueOffsets(i)
		return append([]float32(nil), vals[start:end]...), nil
	case *array.FixedSizeList:
		vals := c.ListValues().(*array.Float32).Float32Values()
		start, end := c.ValueOffsets(i)
		return append([]float32(nil), vals[start:end]...), nil
	default:
		return nil, fmt.Errorf("vsearch: vector column is %s, want a float32 list", col.DataType())
	}
}
