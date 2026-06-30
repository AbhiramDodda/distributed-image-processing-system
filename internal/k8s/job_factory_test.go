package k8s_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/abhiramd/petabyte-platform/internal/cluster"
	"github.com/abhiramd/petabyte-platform/internal/k8s"
	"github.com/abhiramd/petabyte-platform/internal/scheduler"
)

func taskAssignment() scheduler.TaskAssignment {
	return scheduler.TaskAssignment{
		TaskID:    "550e8400-e29b-41d4-a716-446655440000",
		JobID:     "job-abc-123",
		Shard:     "a3",
		Dataset:   "train",
		Algorithm: "resnet",
		Config:    map[string]string{"batch_size": "32"},
	}
}

func TestJobName_format(t *testing.T) {
	name := k8s.JobName("550e8400-e29b-41d4-a716-446655440000")
	if !strings.HasPrefix(name, "petabyte-task-") {
		t.Errorf("JobName = %q, want prefix 'petabyte-task-'", name)
	}
	if len(name) > 63 {
		t.Errorf("JobName = %q (len %d), must be ≤63 chars", name, len(name))
	}
	for _, c := range name {
		if !(c == '-' || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			t.Errorf("JobName %q contains invalid K8s character %c", name, c)
		}
	}
}

func TestJobName_deterministic(t *testing.T) {
	a := k8s.JobName("same-task-id")
	b := k8s.JobName("same-task-id")
	if a != b {
		t.Errorf("JobName is not deterministic: %q != %q", a, b)
	}
}

func TestBuildJob_basicSpec(t *testing.T) {
	a := taskAssignment()
	job, err := k8s.BuildJob(a, "petabyte-worker:v1", "http://coordinator:8090", nil)
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}
	if job == nil {
		t.Fatal("BuildJob returned nil job")
	}

	// Verify via JSON round-trip since the struct is unexported
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	var spec map[string]any
	json.Unmarshal(raw, &spec)

	if spec["apiVersion"] != "batch/v1" {
		t.Errorf("apiVersion = %v, want batch/v1", spec["apiVersion"])
	}
	if spec["kind"] != "Job" {
		t.Errorf("kind = %v, want Job", spec["kind"])
	}
	meta := spec["metadata"].(map[string]any)
	if !strings.HasPrefix(meta["name"].(string), "petabyte-task-") {
		t.Errorf("job name %q missing prefix", meta["name"])
	}
	labels := meta["labels"].(map[string]any)
	if labels["app"] != "petabyte-worker" {
		t.Errorf("label app = %v, want petabyte-worker", labels["app"])
	}
	if labels["shard"] != "a3" {
		t.Errorf("label shard = %v, want a3", labels["shard"])
	}
}

func TestBuildJob_taskJSONEncodedInEnv(t *testing.T) {
	a := taskAssignment()
	job, _ := k8s.BuildJob(a, "petabyte-worker:v1", "http://coordinator:8090", nil)

	raw, _ := json.Marshal(job)
	var spec map[string]any
	json.Unmarshal(raw, &spec)

	jobSpec := spec["spec"].(map[string]any)
	tmpl := jobSpec["template"].(map[string]any)
	podSpec := tmpl["spec"].(map[string]any)
	containers := podSpec["containers"].([]any)
	envs := containers[0].(map[string]any)["env"].([]any)

	var taskJSON, coordURL string
	for _, e := range envs {
		env := e.(map[string]any)
		switch env["name"] {
		case "PETABYTE_TASK_JSON":
			taskJSON = env["value"].(string)
		case "PETABYTE_COORDINATOR_URL":
			coordURL = env["value"].(string)
		}
	}
	if coordURL != "http://coordinator:8090" {
		t.Errorf("PETABYTE_COORDINATOR_URL = %q", coordURL)
	}

	// Decode and verify the task JSON
	decoded, err := base64.StdEncoding.DecodeString(taskJSON)
	if err != nil {
		t.Fatalf("decode PETABYTE_TASK_JSON: %v", err)
	}
	var got scheduler.TaskAssignment
	if err := json.Unmarshal(decoded, &got); err != nil {
		t.Fatalf("unmarshal task: %v", err)
	}
	if got.TaskID != a.TaskID {
		t.Errorf("decoded TaskID = %q, want %q", got.TaskID, a.TaskID)
	}
	if got.Shard != a.Shard {
		t.Errorf("decoded Shard = %q, want %q", got.Shard, a.Shard)
	}
}

func TestBuildJob_gpuResourceFromConfig(t *testing.T) {
	a := taskAssignment()
	a.Config["gpu"] = "1"
	a.Config["cpu"] = "4"
	a.Config["memory"] = "16Gi"

	job, _ := k8s.BuildJob(a, "petabyte-worker:v1", "http://coordinator:8090", nil)
	raw, _ := json.Marshal(job)
	var spec map[string]any
	json.Unmarshal(raw, &spec)

	containers := spec["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	res := containers[0].(map[string]any)["resources"].(map[string]any)
	limits := res["limits"].(map[string]any)

	if limits["nvidia.com/gpu"] != "1" {
		t.Errorf("GPU limit = %v, want 1", limits["nvidia.com/gpu"])
	}
	if limits["cpu"] != "4" {
		t.Errorf("CPU limit = %v, want 4", limits["cpu"])
	}
	if limits["memory"] != "16Gi" {
		t.Errorf("memory limit = %v, want 16Gi", limits["memory"])
	}
}

func TestBuildJob_nodeAffinityForCachedShards(t *testing.T) {
	a := taskAssignment() // shard "a3"
	nodes := []*cluster.NodeInfo{
		{ID: "w1", NodeName: "k8s-node-1", State: cluster.NodeActive,
			Metrics: cluster.NodeMetrics{CachedShards: []string{"a3", "b4"}}},
		{ID: "w2", NodeName: "k8s-node-2", State: cluster.NodeActive,
			Metrics: cluster.NodeMetrics{CachedShards: []string{"ff"}}},
	}

	job, _ := k8s.BuildJob(a, "petabyte-worker:v1", "http://coordinator:8090", nodes)
	raw, _ := json.Marshal(job)
	var spec map[string]any
	json.Unmarshal(raw, &spec)

	podSpec := spec["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	aff, ok := podSpec["affinity"].(map[string]any)
	if !ok {
		t.Fatal("no affinity set when node caches the shard")
	}
	na := aff["nodeAffinity"].(map[string]any)
	preferred := na["preferredDuringSchedulingIgnoredDuringExecution"].([]any)
	if len(preferred) == 0 {
		t.Fatal("preferredDuringSchedulingIgnoredDuringExecution is empty")
	}
	term := preferred[0].(map[string]any)
	if term["weight"].(float64) != 100 {
		t.Errorf("affinity weight = %v, want 100", term["weight"])
	}
}

func TestBuildJob_noAffinityWhenShardNotCached(t *testing.T) {
	a := taskAssignment() // shard "a3"
	nodes := []*cluster.NodeInfo{
		{ID: "w1", NodeName: "k8s-node-1", State: cluster.NodeActive,
			Metrics: cluster.NodeMetrics{CachedShards: []string{"ff", "ee"}}},
	}

	job, _ := k8s.BuildJob(a, "petabyte-worker:v1", "http://coordinator:8090", nodes)
	raw, _ := json.Marshal(job)
	var spec map[string]any
	json.Unmarshal(raw, &spec)

	podSpec := spec["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if _, ok := podSpec["affinity"]; ok {
		t.Error("affinity should not be set when no node caches the shard")
	}
}
