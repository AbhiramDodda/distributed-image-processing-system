package ray

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client wraps the Ray Dashboard REST API.
type Client struct {
	dashboardURL string
	httpClient   *http.Client
}

func NewClient(dashboardURL string) *Client {
	return &Client{
		dashboardURL: dashboardURL,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}
}

type ClusterInfo struct {
	NodeCount      int    `json:"node_count"`
	CPUCount       int    `json:"cpu_count"`
	GPUCount       int    `json:"gpu_count"`
	RAMBytes       int64  `json:"ram_bytes"`
	RayVersion     string `json:"ray_version"`
	PythonVersion  string `json:"python_version"`
}

// HealthCheck confirms the Ray Dashboard API is reachable and returns
// basic cluster info. Returns error if the cluster is unreachable.
func (c *Client) HealthCheck(ctx context.Context) (*ClusterInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dashboardURL+"/api/cluster_info", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ray cluster unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ray dashboard %d", resp.StatusCode)
	}
	var payload struct {
		Data ClusterInfo `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode cluster_info: %w", err)
	}
	return &payload.Data, nil
}
