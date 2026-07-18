package vsearch

import (
	"context"
	"testing"
)

// fakeEncoder returns a canned vector, standing in for the Python CLIP sidecar.
type fakeEncoder struct {
	vec []float32
	gotText string
	gotB64 string
}

func (f *fakeEncoder) Encode(_ context.Context, req EncodeRequest) ([]float32, error) {
	f.gotText = req.Text
	f.gotB64 = req.ImageB64
	return f.vec, nil
}

func TestRun_idExcludesSelf(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	got, err := ix.Run(context.Background(), Query{ID: "east", K: 2}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, r := range got {
		if r.ID == "east" {
			t.Fatalf("id query included self: %v", ids(got))
		}
	}
	if got[0].ID != "ne" {
		t.Fatalf("nearest to east (self-excluded) = %q, want ne", got[0].ID)
	}
}

func TestRun_vectorNeedsNoEncoder(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	got, err := ix.Run(context.Background(), Query{Vector: []float32{1, 0}, K: 1}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got[0].ID != "east" {
		t.Fatalf("vector query nearest = %q, want east", got[0].ID)
	}
}

func TestRun_textUsesEncoder(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	enc := &fakeEncoder{vec: []float32{0, 1}}
	got, err := ix.Run(context.Background(), Query{Text: "northish", K: 1}, enc)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if enc.gotText != "northish" {
		t.Fatalf("encoder text = %q", enc.gotText)
	}
	if got[0].ID != "north" {
		t.Fatalf("text query nearest = %q, want north", got[0].ID)
	}
}

func TestRun_imageBase64Encodes(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	enc := &fakeEncoder{vec: []float32{1, 0}}
	if _, err := ix.Run(context.Background(), Query{Image: []byte("ABC"), K: 1}, enc); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if enc.gotB64 != "QUJD" { // base64("ABC")
		t.Fatalf("image base64 = %q, want QUJD", enc.gotB64)
	}
}

func TestRun_errors(t *testing.T) {
	ix := mustIndex(t, 2, craftedRows())
	if _, err := ix.Run(context.Background(), Query{}, nil); err == nil {
		t.Fatal("expected empty-query error")
	}
	if _, err := ix.Run(context.Background(), Query{Text: "x"}, nil); err == nil {
		t.Fatal("expected missing-encoder error")
	}
	if _, err := ix.Run(context.Background(), Query{ID: "nope"}, nil); err == nil {
		t.Fatal("expected unknown-id error")
	}
}
