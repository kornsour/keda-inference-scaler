package observability

import (
	"testing"
	"time"
)

func TestHealthReadyDuringStartupGraceBeforeFirstQuery(t *testing.T) {
	h := NewHealth(50 * time.Millisecond)
	if !h.Ready() {
		t.Fatal("expected ready immediately after construction (startup grace period)")
	}
}

func TestHealthNotReadyAfterGraceExpiresWithNoSuccess(t *testing.T) {
	h := NewHealth(10 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if h.Ready() {
		t.Fatal("expected not ready once the startup grace period has elapsed with no successful query")
	}
}

func TestHealthReadyAfterRecordSuccess(t *testing.T) {
	h := NewHealth(50 * time.Millisecond)
	h.RecordSuccess()
	if !h.Ready() {
		t.Fatal("expected ready immediately after a recorded success")
	}
}

func TestHealthNotReadyAfterSuccessAges(t *testing.T) {
	h := NewHealth(10 * time.Millisecond)
	h.RecordSuccess()
	time.Sleep(30 * time.Millisecond)
	if h.Ready() {
		t.Fatal("expected not ready once the last success has aged past the window")
	}
}

func TestHealthRecordSuccessRefreshesWindow(t *testing.T) {
	h := NewHealth(30 * time.Millisecond)
	h.RecordSuccess()
	time.Sleep(15 * time.Millisecond)
	h.RecordSuccess()
	time.Sleep(15 * time.Millisecond)
	if !h.Ready() {
		t.Fatal("expected ready: the second success should have refreshed the window")
	}
}

func TestHealthNilIsAlwaysReady(t *testing.T) {
	var h *Health
	if !h.Ready() {
		t.Fatal("expected a nil *Health to report ready")
	}
	h.RecordSuccess() // must not panic
}
