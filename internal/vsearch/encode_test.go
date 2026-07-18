package vsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPEncoder_text(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/encode" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req EncodeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		if req.Text != "a photo of a dog" {
			t.Errorf("text = %q", req.Text)
		}
		json.NewEncoder(w).Encode(encodeResponse{Vector: []float32{0.1, 0.2, 0.3}})
	}))
	defer srv.Close()

	enc := NewHTTPEncoder(srv.URL, srv.Client())
	v, err := enc.Encode(context.Background(), EncodeRequest{Text: "a photo of a dog"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(v) != 3 || v[0] != 0.1 {
		t.Fatalf("vector = %v", v)
	}
}

func TestHTTPEncoder_imagePassesB64(t *testing.T) {
	var gotB64 string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req EncodeRequest
		json.NewDecoder(r.Body).Decode(&req)
		gotB64 = req.ImageB64
		json.NewEncoder(w).Encode(encodeResponse{Vector: []float32{1}})
	}))
	defer srv.Close()

	enc := NewHTTPEncoder(srv.URL, srv.Client())
	if _, err := enc.Encode(context.Background(), EncodeRequest{ImageB64: "QUJD"}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if gotB64 != "QUJD" {
		t.Fatalf("image_b64 = %q, want QUJD", gotB64)
	}
}

func TestHTTPEncoder_propagatesErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not loaded", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	enc := NewHTTPEncoder(srv.URL, srv.Client())
	if _, err := enc.Encode(context.Background(), EncodeRequest{Text: "x"}); err == nil {
		t.Fatal("expected error on non-200 status")
	}
}

func TestHTTPEncoder_rejectsEmptyRequest(t *testing.T) {
	enc := NewHTTPEncoder("http://127.0.0.1:0", nil)
	if _, err := enc.Encode(context.Background(), EncodeRequest{}); err == nil {
		t.Fatal("expected error for empty request")
	}
}
