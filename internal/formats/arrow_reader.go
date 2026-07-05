package formats

import (
	"bytes"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ImageRecord is one image's metadata plus its encoded bytes, destined for a
// columnar batch.
type ImageRecord struct {
	Key string
	Width int32
	Height int32
	Label string
	Data []byte
}

// imageBatchSchema lays out an image batch column-by-column. A columnar batch
// keeps each field in one contiguous buffer, which is what a vectorized or
// GPU-resident pipeline consumes (and what maps to CUDA unified memory later).
var imageBatchSchema = arrow.NewSchema([]arrow.Field{
	{Name: "key", Type: arrow.BinaryTypes.String},
	{Name: "width", Type: arrow.PrimitiveTypes.Int32},
	{Name: "height", Type: arrow.PrimitiveTypes.Int32},
	{Name: "label", Type: arrow.BinaryTypes.String},
	{Name: "data", Type: arrow.BinaryTypes.Binary},
}, nil)

// ImageBatchSchema returns the Arrow schema of an image RecordBatch.
func ImageBatchSchema() *arrow.Schema { return imageBatchSchema }

// BuildImageBatch assembles image records into a single Arrow RecordBatch. The
// caller owns the returned Record and must Release it.
func BuildImageBatch(mem memory.Allocator, imgs []ImageRecord) arrow.RecordBatch {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	b := array.NewRecordBuilder(mem, imageBatchSchema)
	defer b.Release()

	keyB := b.Field(0).(*array.StringBuilder)
	widthB := b.Field(1).(*array.Int32Builder)
	heightB := b.Field(2).(*array.Int32Builder)
	labelB := b.Field(3).(*array.StringBuilder)
	dataB := b.Field(4).(*array.BinaryBuilder)

	for _, im := range imgs {
		keyB.Append(im.Key)
		widthB.Append(im.Width)
		heightB.Append(im.Height)
		labelB.Append(im.Label)
		dataB.Append(im.Data)
	}
	return b.NewRecordBatch()
}

// EncodeIPC serializes a batch to the Arrow IPC stream format for zero-copy
// transport between processes.
func EncodeIPC(rec arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("arrow: ipc write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("arrow: ipc close: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeIPC reads all record batches from an Arrow IPC stream. Returned records
// are retained for the caller, who must Release each.
func DecodeIPC(data []byte) ([]arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("arrow: ipc reader: %w", err)
	}
	defer r.Release()

	var out []arrow.RecordBatch
	for r.Next() {
		rec := r.RecordBatch()
		rec.Retain() // the reader reuses/releases rec on the next Next; keep ours
		out = append(out, rec)
	}
	if err := r.Err(); err != nil && err != io.EOF {
		for _, rec := range out {
			rec.Release()
		}
		return nil, fmt.Errorf("arrow: ipc read: %w", err)
	}
	return out, nil
}
