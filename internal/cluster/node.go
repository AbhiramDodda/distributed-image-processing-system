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
}

type NodeInfo struct {
	ID string `json:"id"`
	Address string `json:"address"`
	State NodeState `json:"state"`
	LastSeen time.Time `json:"last_seen"`
	JoinedAt time.Time `json:"joined_at"`
	Metrics NodeMetrics `json:"metrics"`
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
}
