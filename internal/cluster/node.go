package cluster

import "time"

type NodeState string

const (
	NodeActive NodeState = "Active"
	NodeSuspect NodeState = "Suspect"
	NodeDead NodeState = "Dead"
)

type NodeMetrics struct {
	ActiveTasks int `json:"active_tasks"`
	CPUPct float64 `json:"cpu_pct"`
	MemUsedGB float64 `json:"mem_used_gb"`
	// CachedShards lists shard prefixes this node has in its local NVMe cache.
	// Used by the operator to set K8s node affinity for data-local scheduling.
	CachedShards []string `json:"cached_shards,omitempty"`
}

type NodeInfo struct {
	ID string `json:"id"`
	Address string `json:"address"`
	State NodeState `json:"state"`
	LastSeen time.Time `json:"last_seen"`
	JoinedAt time.Time `json:"joined_at"`
	Metrics NodeMetrics `json:"metrics"`
	// NodeName is the Kubernetes node name, set when the operator registers the worker.
	NodeName string `json:"node_name,omitempty"`
}

type Heartbeat struct {
	NodeID string `json:"node_id"`
	Metrics NodeMetrics `json:"metrics"`
}

type FailureEvent struct {
	NodeID string `json:"node_id"`
	OldState NodeState `json:"old_state"`
	NewState NodeState `json:"new_state"`
	At time.Time `json:"at"`
}

type RegisterRequest struct {
	ID string `json:"id"`
	Address string `json:"address"`
	NodeName string `json:"node_name,omitempty"`
}
