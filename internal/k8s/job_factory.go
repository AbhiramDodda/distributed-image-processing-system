package k8s

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/AbhiramDodda/distributed-image-processing-system/internal/cluster"
	"github.com/AbhiramDodda/distributed-image-processing-system/internal/scheduler"
)

// Minimal K8s batch/v1 Job types — avoids pulling in the full k8s.io/api tree.

type jobMeta struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type envVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type resourceList struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
	GPU    string `json:"nvidia.com/gpu,omitempty"`
}

type resources struct {
	Requests resourceList `json:"requests,omitempty"`
	Limits   resourceList `json:"limits,omitempty"`
}

type container struct {
	Name      string    `json:"name"`
	Image     string    `json:"image"`
	Command   []string  `json:"command,omitempty"`
	Env       []envVar  `json:"env"`
	Resources resources `json:"resources"`
}

type preferredSchedulingTerm struct {
	Weight     int32 `json:"weight"`
	Preference struct {
		MatchExpressions []struct {
			Key      string   `json:"key"`
			Operator string   `json:"operator"`
			Values   []string `json:"values"`
		} `json:"matchExpressions"`
	} `json:"preference"`
}

type nodeAffinity struct {
	PreferredDuringSchedulingIgnoredDuringExecution []preferredSchedulingTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty"`
}

type affinity struct {
	NodeAffinity *nodeAffinity `json:"nodeAffinity,omitempty"`
}

type podSpec struct {
	RestartPolicy string      `json:"restartPolicy"`
	Containers    []container `json:"containers"`
	Affinity      *affinity   `json:"affinity,omitempty"`
}

type podTemplateSpec struct {
	Metadata jobMeta `json:"metadata"`
	Spec     podSpec `json:"spec"`
}

type batchJobSpec struct {
	BackoffLimit int32           `json:"backoffLimit"`
	Template     podTemplateSpec `json:"template"`
}

type batchJob struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Metadata   jobMeta      `json:"metadata"`
	Spec       batchJobSpec `json:"spec"`
}

// JobName returns the K8s Job name for a task. K8s names must be ≤63 chars,
// lowercase alphanumeric and hyphens only.
func JobName(taskID string) string {
	safe := strings.ToLower(strings.ReplaceAll(taskID, "_", "-"))
	if len(safe) > 55 {
		safe = safe[:55]
	}
	return "petabyte-task-" + safe[:8]
}

// BuildJob creates a K8s Job spec for a TaskAssignment.
// coordinatorURL is injected as an env var so the pod can report results.
// nodes is the active worker list; those with matching CachedShards get
// preferred scheduling weight to avoid cross-node data pulls.
func BuildJob(a scheduler.TaskAssignment, image, coordinatorURL string, nodes []*cluster.NodeInfo) (*batchJob, error) {
	taskJSON, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("marshal task: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(taskJSON)

	cpu := "1"
	mem := "2Gi"
	gpu := ""
	if v, ok := a.Config["cpu"]; ok {
		cpu = v
	}
	if v, ok := a.Config["memory"]; ok {
		mem = v
	}
	if v, ok := a.Config["gpu"]; ok && v != "" {
		gpu = v
	}

	res := resources{
		Requests: resourceList{CPU: cpu, Memory: mem},
		Limits:   resourceList{CPU: cpu, Memory: mem},
	}
	if gpu != "" {
		res.Requests.GPU = gpu
		res.Limits.GPU = gpu
	}

	labels := map[string]string{
		"app":     "petabyte-worker",
		"task-id": a.TaskID,
		"job-id":  a.JobID,
		"shard":   a.Shard,
	}

	job := &batchJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: jobMeta{
			Name:   JobName(a.TaskID),
			Labels: labels,
		},
		Spec: batchJobSpec{
			BackoffLimit: 0,
			Template: podTemplateSpec{
				Metadata: jobMeta{Labels: labels},
				Spec: podSpec{
					RestartPolicy: "Never",
					Containers: []container{
						{
							Name:      "worker",
							Image:     image,
							Command:   []string{"/worker", "-single-task"},
							Env:       []envVar{
								{Name: "PETABYTE_TASK_JSON", Value: encoded},
								{Name: "PETABYTE_COORDINATOR_URL", Value: coordinatorURL},
							},
							Resources: res,
						},
					},
				},
			},
		},
	}

	job.Spec.Template.Spec.Affinity = buildAffinity(a.Shard, nodes)
	return job, nil
}

// buildAffinity returns a nodeAffinity that prefers nodes already caching the
// target shard. Returns nil when no node has the shard cached.
func buildAffinity(shard string, nodes []*cluster.NodeInfo) *affinity {
	var preferred []string
	for _, n := range nodes {
		if n.NodeName == "" {
			continue
		}
		for _, cs := range n.Metrics.CachedShards {
			if cs == shard {
				preferred = append(preferred, n.NodeName)
				break
			}
		}
	}
	if len(preferred) == 0 {
		return nil
	}
	term := preferredSchedulingTerm{Weight: 100}
	term.Preference.MatchExpressions = []struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	}{
		{Key: "kubernetes.io/hostname", Operator: "In", Values: preferred},
	}
	return &affinity{
		NodeAffinity: &nodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []preferredSchedulingTerm{term},
		},
	}
}

// CreateJob posts the Job spec to the K8s API server.
func (c *Client) CreateJob(job *batchJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs", c.namespace)
	req, err := c.newRequest(http.MethodPost, path)
	if err != nil {
		return err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("k8s %d: %v", resp.StatusCode, e["message"])
	}
	return nil
}
