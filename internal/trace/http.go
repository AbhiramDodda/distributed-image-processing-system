package trace

import (
	"encoding/json"
	"net/http"
	"sort"
)

// Report is the JSON body served at /debug/trace.
type Report struct {
	Enabled bool `json:"enabled"`
	EventCount int64 `json:"event_count"`
	Events []Event `json:"events,omitempty"`
	Traces []TraceGroup `json:"traces,omitempty"`
}

// TraceGroup collects every retained event for one TraceID, plus the parent
// TaskIDs observed for it, so a consumer can reconstruct the causal chain (which
// steal or retry produced which task) without re-deriving anything.
type TraceGroup struct {
	TraceID string `json:"trace_id"`
	JobID string `json:"job_id,omitempty"`
	Shard string `json:"shard,omitempty"`
	Parents []string `json:"parents,omitempty"`
	Events []Event `json:"events"`
}

// Handler serves the trace ring as JSON.
//
//	GET /debug/trace                  most recent events, oldest first
//	GET /debug/trace?task=<taskID>    only events for one task
//	GET /debug/trace?trace=<traceID>  only events for one unit of work
//	GET /debug/trace?causal=1         events grouped into causal TraceGroups
//
// It works (returning enabled=false) even when tracing is off, so it is always
// safe to register next to /debug/diag.
func Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		task := q.Get("task")
		tr := q.Get("trace")
		causal := truthy(q.Get("causal"))

		all := Events()
		filtered := all[:0:0]
		for _, e := range all {
			if task != "" && e.TaskID != task {
				continue
			}
			if tr != "" && e.TraceID != tr {
				continue
			}
			filtered = append(filtered, e)
		}

		rep := Report{Enabled: Enabled(), EventCount: Count()}
		if causal {
			rep.Traces = group(filtered)
		} else {
			rep.Events = filtered
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(rep)
	}
}

// group buckets events by TraceID and collects the distinct parent TaskIDs seen
// within each, so the caller can follow steal/retry lineage. Groups are ordered
// by the time of their first event; events within a group stay in ring order.
func group(events []Event) []TraceGroup {
	idx := map[string]int{}
	var groups []TraceGroup
	for _, e := range events {
		i, ok := idx[e.TraceID]
		if !ok {
			i = len(groups)
			idx[e.TraceID] = i
			groups = append(groups, TraceGroup{TraceID: e.TraceID, JobID: e.JobID, Shard: e.Shard})
		}
		g := &groups[i]
		if g.JobID == "" {
			g.JobID = e.JobID
		}
		if g.Shard == "" {
			g.Shard = e.Shard
		}
		if e.Parent != "" && !contains(g.Parents, e.Parent) {
			g.Parents = append(g.Parents, e.Parent)
		}
		g.Events = append(g.Events, e)
	}
	sort.SliceStable(groups, func(a, b int) bool {
		return groups[a].Events[0].Time.Before(groups[b].Events[0].Time)
	})
	return groups
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
