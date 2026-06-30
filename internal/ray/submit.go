package ray

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusSucceeded JobStatus = "SUCCEEDED"
	JobStatusFailed    JobStatus = "FAILED"
	JobStatusStopped   JobStatus = "STOPPED"
)

type SubmitRequest struct {
	Entrypoint    string            `json:"entrypoint"`
	RuntimeEnvJSON string           `json:"runtime_env"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type SubmitResponse struct {
	SubmissionID string `json:"submission_id"`
}

type JobInfo struct {
	SubmissionID string    `json:"submission_id"`
	Status       JobStatus `json:"status"`
	Message      string    `json:"message,omitempty"`
	StartTime    *int64    `json:"start_time,omitempty"`
	EndTime      *int64    `json:"end_time,omitempty"`
}

// SubmitJob sends a Ray job to the Dashboard API and returns the submission ID.
func (c *Client) SubmitJob(ctx context.Context, req SubmitRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal submit request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.dashboardURL+"/api/jobs/", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("submit ray job: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return "", fmt.Errorf("ray submit %d: %v", resp.StatusCode, e)
	}
	var sr SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return "", fmt.Errorf("decode submit response: %w", err)
	}
	return sr.SubmissionID, nil
}

// GetJob returns the current status of a Ray job.
func (c *Client) GetJob(ctx context.Context, submissionID string) (*JobInfo, error) {
	url := fmt.Sprintf("%s/api/jobs/%s", c.dashboardURL, submissionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get ray job: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ray get job %d", resp.StatusCode)
	}
	var info JobInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode job info: %w", err)
	}
	return &info, nil
}

// WaitForJob polls until the job reaches a terminal state or ctx is cancelled.
func (c *Client) WaitForJob(ctx context.Context, submissionID string, pollInterval time.Duration) (*JobInfo, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			info, err := c.GetJob(ctx, submissionID)
			if err != nil {
				return nil, err
			}
			switch info.Status {
			case JobStatusSucceeded, JobStatusFailed, JobStatusStopped:
				return info, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
