package quota

import (
	"testing"
	"time"
)

func TestLedger_recordCompute(t *testing.T) {
	l := NewLedger()
	l.RecordCompute("t", Request{CPUCores: 8, GPUCount: 2}, 10*time.Second)
	l.RecordCompute("t", Request{CPUCores: 4, GPUCount: 0}, 5*time.Second)

	s := l.Statement("t")
	if s.CPUSeconds != 8*10+4*5 {
		t.Fatalf("CPUSeconds = %.1f, want 100", s.CPUSeconds)
	}
	if s.GPUSeconds != 2*10 {
		t.Fatalf("GPUSeconds = %.1f, want 20", s.GPUSeconds)
	}
	if s.JobsCompleted != 2 {
		t.Fatalf("JobsCompleted = %d, want 2", s.JobsCompleted)
	}
}

func TestLedger_recordIO(t *testing.T) {
	l := NewLedger()
	l.RecordIO("t", 1<<30, 512<<20)
	l.RecordIO("t", -5, -5) // negatives ignored
	l.RecordIO("t", 1<<20, 0)

	s := l.Statement("t")
	if s.BytesRead != (1<<30)+(1<<20) {
		t.Fatalf("BytesRead = %d", s.BytesRead)
	}
	if s.BytesWritten != 512<<20 {
		t.Fatalf("BytesWritten = %d", s.BytesWritten)
	}
}

func TestLedger_unknownTenantIsZero(t *testing.T) {
	l := NewLedger()
	if s := l.Statement("ghost"); s != (Statement{}) {
		t.Fatalf("unknown tenant statement = %+v, want zero", s)
	}
}

func TestLedger_tenantsAreIsolated(t *testing.T) {
	l := NewLedger()
	l.RecordCompute("a", Request{CPUCores: 1}, time.Second)
	l.RecordCompute("b", Request{CPUCores: 2}, time.Second)
	if l.Statement("a").CPUSeconds != 1 {
		t.Fatalf("tenant a leaked: %+v", l.Statement("a"))
	}
	if l.Statement("b").CPUSeconds != 2 {
		t.Fatalf("tenant b leaked: %+v", l.Statement("b"))
	}
}
