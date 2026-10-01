package main

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func captureDiagnostics(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &output
}

func TestDecisionDiagnosticsExcludePrivateContext(t *testing.T) {
	a, _ := loopTestActor(t)
	a.state.Profile = "secret persona"
	a.state.Memories = []string{"private memory"}
	a.recent = []recentEvent{{Type: "chat_received", Payload: []byte(`{"message":"private chat"}`), New: true}}
	a.state.Task = &task{ID: "task-id", Kind: "npc_service", Phase: "service", OperationID: "op-id"}
	output := captureDiagnostics(t)
	a.logDecisionState("queued", "chat_received", "trace-id")
	text := output.String()
	for _, required := range []string{"decision_queued", "trace-id", "new_events=1", "task_kind=npc_service", "op-id"} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing diagnostic %q: %s", required, text)
		}
	}
	for _, private := range []string{"secret persona", "private memory", "private chat"} {
		if strings.Contains(text, private) {
			t.Fatalf("diagnostic exposed %q", private)
		}
	}
}

func TestDecisionDeferDiagnosticsAreRateLimited(t *testing.T) {
	a, _ := loopTestActor(t)
	output := captureDiagnostics(t)
	a.logDecisionBlock("request_in_flight", "chat_received")
	a.logDecisionBlock("request_in_flight", "timer")
	if strings.Count(output.String(), "decision_deferred") != 1 {
		t.Fatal("same blocked decision logged on every event")
	}
	a.lastDecisionBlockAt = time.Now().Add(-diagnosticRepeatInterval)
	a.logDecisionBlock("request_in_flight", "timer")
	a.logDecisionBlock("model_queue_full", "timer")
	if strings.Count(output.String(), "decision_deferred") != 3 {
		t.Fatal("elapsed interval or changed blocker was not logged")
	}
}

func TestTaskWaitDiagnosticsRecordTransitions(t *testing.T) {
	a, _ := loopTestActor(t)
	a.state.Task = &task{ID: "task", Kind: "fishing"}
	output := captureDiagnostics(t)
	a.logTaskWait("combat")
	a.logTaskWait("combat")
	a.logTaskWait("native_owned")
	a.clearTaskWait()
	a.clearTaskWait()
	text := output.String()
	if strings.Count(text, "agent task_wait ") != 2 || strings.Count(text, "task_wait_cleared") != 1 || !strings.Contains(text, "progress_age_ms=0") {
		t.Fatalf("wait transition diagnostics are missing or noisy: %s", text)
	}
}

func TestEventDeliveryDelayHandlesMissingAndFutureTimestamps(t *testing.T) {
	if eventDeliveryDelay(0) != 0 || eventDeliveryDelay(time.Now().Add(time.Minute).UnixMilli()) != 0 {
		t.Fatal("missing or future event timestamp reported a bogus delay")
	}
	if delay := eventDeliveryDelay(time.Now().Add(-time.Second).UnixMilli()); delay < 1000 || delay > 2000 {
		t.Fatalf("wrong delivery delay: %d", delay)
	}
}

func TestDecisionQueueTracePreservesFullInput(t *testing.T) {
	a, _ := loopTestActor(t)
	a.owner.modelJobs = make(chan decisionJob, 1)
	a.owner.cfg.modelEndpoint = "http://unused.invalid"
	a.owner.cfg.modelName = "test-only"
	a.state.Task = &task{ID: "current-task", Kind: "fishing", GoalCount: 3}
	a.recent = []recentEvent{{Type: "chat_received", Payload: []byte(`{"message":"hello"}`), New: true}}
	output := captureDiagnostics(t)
	a.decide("chat_received")
	var job decisionJob
	select {
	case job = <-a.owner.modelJobs:
	case <-time.After(time.Second):
		t.Fatal("decision did not enter the local stub queue")
	}
	if job.traceID == "" || job.queuedAt.IsZero() || job.task == nil || job.task.ID != "current-task" || len(job.events) != 1 {
		t.Fatal("decision tracing lost task/events or did not assign correlation metadata")
	}
	if !strings.Contains(output.String(), job.traceID) || !strings.Contains(output.String(), "decision_queued") {
		t.Fatal("queued decision cannot be correlated to worker result")
	}
}

func TestStaleDecisionResultIsDiagnosedWithoutApplyingTool(t *testing.T) {
	a, _ := loopTestActor(t)
	a.revision = 2
	a.bridgeActive = false // Do not queue another decision in this isolated test.
	a.state.Task = &task{ID: "keep", Kind: "fishing", GoalCount: 1}
	output := captureDiagnostics(t)
	a.handleDecisionResult(decisionResult{traceID: "stale-trace", revision: 1,
		call: &toolCall{Name: "cancel_task"}, queueWait: time.Second, modelElapsed: 2 * time.Second})
	if a.state.Task == nil || a.state.Task.ID != "keep" {
		t.Fatal("stale decision replaced a newer task")
	}
	text := output.String()
	for _, expected := range []string{"stale-trace", "stale=true", "queue_ms=1000", "model_ms=2000"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("missing stale decision diagnostic %q: %s", expected, text)
		}
	}
}
