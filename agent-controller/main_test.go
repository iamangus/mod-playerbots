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

func TestRoutineObservationsPreserveDecisionRevision(t *testing.T) {
	a := &actor{revision: 7}
	a.state.Profile = "test bot"
	for _, kind := range []string{"snapshot", "primitive_progress", "bot_heartbeat"} {
		a.handleEvent(event{Type: kind, Payload: json.RawMessage(`{"schema_version":1,"bot":{"level":1}}`)})
		if a.revision != 7 {
			t.Fatalf("%s invalidated the model decision: revision=%d", kind, a.revision)
		}
	}
	if !a.hasSnapshot || a.latest.Bot.Level != 1 {
		t.Fatal("routine observation did not update live state")
	}
	a.handleEvent(event{Type: "combat_changed", Payload: json.RawMessage(`{}`)})
	if a.revision != 8 {
		t.Fatal("meaningful event did not invalidate the old decision")
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

func TestDestinationArrivalSurvivesCoreCooldown(t *testing.T) {
	var state snapshot
	if err := json.Unmarshal([]byte(`{"bot":{"map_id":1},"travel_target":{"map_id":1,"is_working":false,"arrived":true}}`), &state); err != nil {
		t.Fatal(err)
	}
	if !destinationArrived(state) {
		t.Fatal("core arrival was lost when the destination entered cooldown")
	}
	state.TravelTarget.IsTraveling = true
	if destinationArrived(state) {
		t.Fatal("traveling task was marked arrived")
	}
	state.TravelTarget.IsTraveling = false
	state.Bot.MapID = 0
	if destinationArrived(state) {
		t.Fatal("cross-map task was marked arrived")
	}
	state.Bot.MapID = 1
	state.TravelTarget.Arrived = false
	if destinationArrived(state) {
		t.Fatal("inactive travel target was marked arrived")
	}
	state.TravelTarget.IsWorking = true
	if !destinationArrived(state) {
		t.Fatal("legacy working-state arrival was not retained")
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

func TestParsePopulationSnapshotAggregatesCounts(t *testing.T) {
	payload := json.RawMessage(`{"status":"completed","total":7,"counts":[
		{"race":1,"class":1,"level_band":0,"count":4},
		{"race":2,"class":3,"level_band":0,"count":3}],
		"zones":[{"zone_id":12,"low_level":2},{"zone_id":14,"low_level":5}]}`)
	counts := parsePopulationSnapshot(payload)
	if counts == nil {
		t.Fatal("snapshot payload was not recognized")
	}
	if counts.Total != 7 || counts.ByRace[1] != 4 || counts.ByRace[2] != 3 ||
		counts.ByClass[1] != 4 || counts.ByClass[3] != 3 ||
		counts.Zones[12] != 2 || counts.Zones[14] != 5 {
		t.Fatalf("unexpected aggregation: %+v", counts)
	}
}

func TestParsePopulationSnapshotRejectsOtherPayloads(t *testing.T) {
	if parsePopulationSnapshot(json.RawMessage(`{"status":"rejected","reason":"throttled"}`)) != nil {
		t.Fatal("a rejection payload must not be parsed as a snapshot")
	}
	if parsePopulationSnapshot(json.RawMessage(`not json`)) != nil {
		t.Fatal("malformed payload must not be parsed as a snapshot")
	}
}

func TestChooseRaceClassHonoursZoneConstraintAndDeficits(t *testing.T) {
	manager := &populationManager{cfg: populationConfig{targetTotal: 10}}
	// Nothing exists yet: every race/class deficit is 1.0.
	race, class, ok := manager.chooseRaceClass(12, &populationCounts{ByRace: map[uint32]uint64{},
		ByClass: map[uint32]uint64{}})
	if !ok {
		t.Fatal("expected a valid allocation on an empty population")
	}
	if raceStartZones[race] != 12 {
		t.Fatalf("zone-constrained allocation produced race %d (start zone %d, want 12)", race, raceStartZones[race])
	}
	if class == 0 || class == 6 {
		t.Fatalf("unexpected class %d", class)
	}
}

func TestChooseRaceClassPrefersLargestDeficit(t *testing.T) {
	manager := &populationManager{cfg: populationConfig{targetTotal: 100}}
	counts := &populationCounts{ByRace: map[uint32]uint64{1: 40}, ByClass: map[uint32]uint64{}}
	race, _, ok := manager.chooseRaceClass(0, counts)
	if !ok {
		t.Fatal("expected a valid allocation")
	}
	// Humans are over-represented relative to an equal share of 100/10.
	if race == 1 {
		t.Fatal("expected the allocator to avoid the over-represented race")
	}
}

func TestChooseRoleMatchesClassCapabilities(t *testing.T) {
	manager := &populationManager{cfg: populationConfig{targetTotal: 100}}
	capable := map[string]bool{}
	for _, role := range classRoles[1] {
		capable[role] = true
	}
	if role := manager.chooseRole(1, nil); !capable[role] {
		t.Fatalf("warrior returned a non-capable role: %s", role)
	}
	if role := manager.chooseRole(8, nil); role != "dps" {
		t.Fatalf("mage cannot tank or heal, got %s", role)
	}
}

func TestPopulationConfigDefaultsDisableWhenNoTarget(t *testing.T) {
	t.Setenv("AGENT_POPULATION_ENABLED", "1")
	t.Setenv("AGENT_POPULATION_TARGET_TOTAL", "0")
	if cfg := loadPopulationConfig(); cfg.enabled {
		t.Fatal("population management must be disabled without a target")
	}
	t.Setenv("AGENT_POPULATION_TARGET_TOTAL", "50")
	if cfg := loadPopulationConfig(); !cfg.enabled {
		t.Fatal("population management should enable with a target set")
	}
}
