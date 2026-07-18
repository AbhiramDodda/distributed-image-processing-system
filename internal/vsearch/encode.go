package vsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// EncodeRequest is a query to turn into a vector. Exactly one of Text or ImageB64
// is set: Text runs CLIP's text encoder (text->image search), ImageB64 (a
// base64-encoded image) runs the image encoder (novel image->image search).
type EncodeRequest struct {
	Text string `json:"text,omitempty"`
	ImageB64 string `json:"image_b64,omitempty"`
}

// Encoder turns a text or image query into an embedding vector. It is the seam to
// the Python CLIP sidecar; a fake implementation keeps the CLI/HTTP handler and
// their tests independent of a live model.
type Encoder interface {
	Encode(ctx context.Context, req EncodeRequest) ([]float32, error)
}

// HTTPEncoder calls the Python CLIP sidecar's POST {BaseURL}/encode endpoint. The
// request/response contract is deliberately tiny JSON so the two languages meet at
// a stable wire format rather than a shared library.
type HTTPEncoder struct {
	BaseURL string
	HTTP *http.Client
}

// NewHTTPEncoder builds an encoder targeting the sidecar at baseURL. A nil client
// uses http.DefaultClient.
func NewHTTPEncoder(baseURL string, client *http.Client) *HTTPEncoder {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPEncoder{BaseURL: baseURL, HTTP: client}
}

type encodeResponse struct {
	Vector []float32 `json:"vector"`
}

// Encode posts req to the sidecar and returns the resulting vector.
func (e *HTTPEncoder) Encode(ctx context.Context, req EncodeRequest) ([]float32, error) {
	if req.Text == "" && req.ImageB64 == "" {
		return nil, fmt.Errorf("vsearch: encode request has neither text nor image")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("vsearch: marshal encode request: %w", err)
	}
	url := e.BaseURL + "/encode"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("vsearch: build encode request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("vsearch: call encoder %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("vsearch: encoder %s returned %s: %s", url, resp.Status, bytes.TrimSpace(msg))
	}
	var out encodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("vsearch: decode encoder response: %w", err)
	}
	if len(out.Vector) == 0 {
		return nil, fmt.Errorf("vsearch: encoder returned empty vector")
	}
	return out.Vector, nil
}
