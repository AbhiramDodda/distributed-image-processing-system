package formats

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

// ResultRow is one algorithm task result. Written columnar to Parquet, results
// are immediately queryable by Athena / Spark / DuckDB without a load step.
type ResultRow struct {
	TaskID string
	Shard string
	OutputKey string
	ImagesProcessed int64
	BytesRead int64
}

var resultSchema = arrow.NewSchema([]arrow.Field{
	{Name: "task_id", Type: arrow.BinaryTypes.String},
	{Name: "shard", Type: arrow.BinaryTypes.String},
	{Name: "output_key", Type: arrow.BinaryTypes.String},
	{Name: "images_processed", Type: arrow.PrimitiveTypes.Int64},
	{Name: "bytes_read", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// ResultSchema returns the Arrow schema mirrored by the Parquet result file.
func ResultSchema() *arrow.Schema { return resultSchema }

// WriteResultsParquet writes rows to w as a single-row-group Parquet file.
func WriteResultsParquet(w io.Writer, rows []ResultRow) error {
	b := array.NewRecordBuilder(memory.DefaultAllocator, resultSchema)
	defer b.Release()
	taskB := b.Field(0).(*array.StringBuilder)
	shardB := b.Field(1).(*array.StringBuilder)
	outB := b.Field(2).(*array.StringBuilder)
	imgB := b.Field(3).(*array.Int64Builder)
	bytesB := b.Field(4).(*array.Int64Builder)
	for _, r := range rows {
		taskB.Append(r.TaskID)
		shardB.Append(r.Shard)
		outB.Append(r.OutputKey)
		imgB.Append(r.ImagesProcessed)
		bytesB.Append(r.BytesRead)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	fw, err := pqarrow.NewFileWriter(resultSchema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("parquet: new writer: %w", err)
	}
	if err := fw.Write(rec); err != nil {
		fw.Close()
		return fmt.Errorf("parquet: write batch: %w", err)
	}
	if err := fw.Close(); err != nil {
		return fmt.Errorf("parquet: close: %w", err)
	}
	return nil
}

// ReadResultsParquet parses a Parquet file produced by WriteResultsParquet back
// into rows. Primarily for round-trip validation and re-ingesting results.
func ReadResultsParquet(data []byte) ([]ResultRow, error) {
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parquet: open: %w", err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("parquet: arrow reader: %w", err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("parquet: read table: %w", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, 0)
	defer tr.Release()

	var rows []ResultRow
	for tr.Next() {
		rec := tr.RecordBatch()
		task := rec.Column(0).(*array.String)
		shard := rec.Column(1).(*array.String)
		out := rec.Column(2).(*array.String)
		img := rec.Column(3).(*array.Int64)
		br := rec.Column(4).(*array.Int64)
		for i := range int(rec.NumRows()) {
			rows = append(rows, ResultRow{
				TaskID: task.Value(i),
				Shard: shard.Value(i),
				OutputKey: out.Value(i),
				ImagesProcessed: img.Value(i),
				BytesRead: br.Value(i),
			})
		}
	}
	if err := tr.Err(); err != nil {
		return nil, fmt.Errorf("parquet: iterate table: %w", err)
	}
	return rows, nil
}
