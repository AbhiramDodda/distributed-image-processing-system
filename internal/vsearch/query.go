package vsearch

import (
	"context"
	"encoding/base64"
	"fmt"
)

// Query is a single similarity request resolved against an Index. Exactly one of
// its modes is set; ResolveVector turns whichever is present into a query vector.
//
//	ID     -> image->image over a corpus member (no encoder needed; self-excluded)
//	Vector -> a raw pre-computed vector (bypasses the encoder; used by tests/tools)
//	Text   -> text->image, encoded live by the CLIP text encoder
//	Image  -> novel image->image, encoded live by the CLIP image encoder
type Query struct {
	ID string
	Vector []float32
	Text string
	Image []byte // raw image bytes; base64-encoded for the encoder wire format
	K int
}

// ResolveVector produces the query vector and the set of IDs to exclude from
// results. For ID queries the corpus vector is looked up and the query image itself
// is excluded; Text/Image queries call enc (which may be nil only when unused).
func (q Query) ResolveVector(ctx context.Context, ix *Index, enc Encoder) (vec []float32, exclude []string, err error) {
	switch {
	case q.ID != "":
		v, ok := ix.Lookup(q.ID)
		if !ok {
			return nil, nil, fmt.Errorf("vsearch: id %q not in index", q.ID)
		}
		return v, []string{q.ID}, nil
	case len(q.Vector) != 0:
		return q.Vector, nil, nil
	case q.Text != "":
		if enc == nil {
			return nil, nil, fmt.Errorf("vsearch: text query needs an encoder, none configured")
		}
		v, err := enc.Encode(ctx, EncodeRequest{Text: q.Text})
		return v, nil, err
	case len(q.Image) != 0:
		if enc == nil {
			return nil, nil, fmt.Errorf("vsearch: image query needs an encoder, none configured")
		}
		v, err := enc.Encode(ctx, EncodeRequest{ImageB64: base64.StdEncoding.EncodeToString(q.Image)})
		return v, nil, err
	default:
		return nil, nil, fmt.Errorf("vsearch: empty query (set one of id/vector/text/image)")
	}
}

// Run resolves the query to a vector and returns its top-k neighbours.
func (ix *Index) Run(ctx context.Context, q Query, enc Encoder) ([]Result, error) {
	k := q.K
	if k <= 0 {
		k = 10
	}
	vec, exclude, err := q.ResolveVector(ctx, ix, enc)
	if err != nil {
		return nil, err
	}
	return ix.Search(vec, k, exclude...)
}
