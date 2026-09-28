package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSnapshotFromEventAcceptsWrappedAndDirectSnapshots(t *testing.T) {
	wrapped := event{Payload: json.RawMessage(`{"snapshot":{"schema_version":1,"bot":{"map_id":1}}}`)}
	if got := string(snapshotFromEvent(wrapped)); got != `{"schema_version":1,"bot":{"map_id":1}}` {
		t.Fatalf("unexpected wrapped snapshot: %s", got)
	}

	direct := event{Payload: json.RawMessage(`{"schema_version":1,"bot":{"map_id":2}}`)}
	if got := string(snapshotFromEvent(direct)); got != string(direct.Payload) {
		t.Fatalf("unexpected direct snapshot: %s", got)
	}
}

func TestWithinAreaBoundsAndMissingPosition(t *testing.T) {
	center := []float64{0, 0, 0}
	if !withinArea([]float64{3, 4, 0}, center, 5) {
		t.Fatal("expected point on the radius boundary to be inside")
	}
	if withinArea([]float64{6, 0, 0}, center, 5) {
		t.Fatal("expected point outside radius to be rejected")
	}
	if !withinArea(nil, center, 5) {
		t.Fatal("missing point data should not reject a candidate")
	}
}

func TestStatefulOrdinal(t *testing.T) {
	if got := statefulOrdinal("cloud-agent-controller-17"); got != 17 {
		t.Fatalf("unexpected StatefulSet ordinal: %d", got)
	}
}

func TestDecisionJitterIsStableAndBounded(t *testing.T) {
	interval := 30 * time.Minute
	first := decisionJitter("Player-1-123", interval)
	if first < 0 || first >= interval {
		t.Fatalf("jitter outside interval: %s", first)
	}
	if again := decisionJitter("Player-1-123", interval); again != first {
		t.Fatalf("jitter should be stable for one actor: got %s, want %s", again, first)
	}
}

func TestOwnerEpochOrdering(t *testing.T) {
	if !ownerEpochOlder("100", "200") {
		t.Fatal("older numeric owner epoch was not rejected")
	}
	if ownerEpochOlder("200", "100") {
		t.Fatal("newer numeric owner epoch was treated as stale")
	}
}

func TestPlayerbotEventShardIsStable(t *testing.T) {
	first := playerbotEventShard("Player-1-12345")
	if first >= eventShardCount {
		t.Fatalf("shard out of range: %d", first)
	}
	if again := playerbotEventShard("Player-1-12345"); again != first {
		t.Fatalf("shard is unstable: got %d, want %d", again, first)
	}
}

func TestAgentToolSchemasAreJSON(t *testing.T) {
	encoded, err := json.Marshal(agentTools)
	if err != nil {
		t.Fatalf("marshal tool definitions: %v", err)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal tool definitions: %v", err)
	}
	if len(decoded) == 0 {
		t.Fatal("agent tool list is empty")
	}
}
