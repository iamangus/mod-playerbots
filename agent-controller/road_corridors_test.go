package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func roadTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, p := craftingActor(t)
	a.owner.cfg.roadPreference = true
	a.latest.Bot.Position = []float64{0, 0, 0}
	a.owner.cfg.roadCorridors = []roadCorridor{{ID: "verified", Destination: "Town", Evidence: "operator verified road trace", Points: [][]float64{{10, 0, 0}, {100, 0, 0}}}}
	return a, p
}

func TestRoadCorridorRealizesChildrenAndThenNamedArrival(t *testing.T) {
	a, p := roadTestActor(t)
	a.startDestinationTask(map[string]json.RawMessage{"destination": mustJSON("Town")})
	if a.state.Task.Phase != "road_segment" || nextLoopCommand(t, p).Operation != "navigate_to_position" {
		t.Fatal("no local corridor child")
	}
	op := a.state.Task.OperationID
	a.handleTaskOperation(fishingResult(op, "accepted", "queued"))
	if a.state.Task.RoadPoint != 0 || a.state.Task.OperationID != op {
		t.Fatal("queue acceptance advanced route")
	}
	a.handleTaskOperation(fishingResult(op, "completed", "observed position reached"))
	if a.state.Task.RoadPoint != 1 {
		t.Fatal("terminal child did not advance parent immediately")
	}
	if nextLoopCommand(t, p).Operation != "navigate_to_position" {
		t.Fatal("missing second road child")
	}
	a.handleTaskOperation(fishingResult(op, "completed", "observed position reached"))
	if a.state.Task.RoadPoint != 1 {
		t.Fatal("duplicate terminal advanced parent")
	}
	a.handleTaskOperation(fishingResult(a.state.Task.OperationID, "completed", "observed position reached"))
	if a.state.Task == nil || a.state.Task.Phase != "travel" || nextLoopCommand(t, p).Operation != "navigate_to_destination" {
		t.Fatal("corridor endpoint substituted for named arrival")
	}
	a.handleTaskOperation(fishingResult(a.state.Task.OperationID, "completed", "observed destination reached"))
	if a.state.Task != nil {
		t.Fatal("named arrival did not finish")
	}
}

func TestRoadCorridorFailureFallsBackAndPersistsCooldown(t *testing.T) {
	a, p := roadTestActor(t)
	a.startDestinationTask(map[string]json.RawMessage{"destination": mustJSON("Town")})
	nextLoopCommand(t, p)
	a.handleTaskOperation(fishingResult(a.state.Task.OperationID, "rejected", "path unavailable"))
	if nextLoopCommand(t, p).Operation != "navigate_to_destination" || a.state.Task.Phase != "travel" || a.preferredRoadCorridor("Town") != nil {
		t.Fatal("failed corridor did not fall back with cooldown")
	}
	var restored persistedAgent
	if err := json.Unmarshal(mustJSON(a.state), &restored); err != nil || !restored.RoadCooldowns["verified"].After(time.Now()) {
		t.Fatal("cooldown lost on restart")
	}
}

func TestRoadCorridorOptInMapEntryAndDetourGates(t *testing.T) {
	a, _ := roadTestActor(t)
	if a.preferredRoadCorridor("Town") == nil {
		t.Fatal("valid route not selected")
	}
	a.owner.cfg.roadPreference = false
	if a.preferredRoadCorridor("Town") != nil {
		t.Fatal("default routing changed")
	}
	a.owner.cfg.roadPreference = true
	a.latest.Bot.MapID = 1
	if a.preferredRoadCorridor("Town") != nil {
		t.Fatal("cross-map route used")
	}
	a.latest.Bot.MapID = 0
	a.latest.Bot.Position = []float64{1000, 0, 0}
	if a.preferredRoadCorridor("Town") != nil {
		t.Fatal("distant road entry used")
	}
	a.latest.Bot.Position = []float64{0, 0, 0}
	a.owner.cfg.roadCorridors[0].Points = [][]float64{{10, 0, 0}, {10, 400, 0}, {100, 0, 0}}
	if a.preferredRoadCorridor("Town") != nil {
		t.Fatal("excessive detour selected")
	}
}

func TestRoadCorridorValidationAndClone(t *testing.T) {
	a, _ := roadTestActor(t)
	path := filepath.Join(t.TempDir(), "roads.json")
	if err := os.WriteFile(path, mustJSON(a.owner.cfg.roadCorridors), 0600); err != nil {
		t.Fatal(err)
	}
	if corridors, err := loadRoadCorridors(path); err != nil || len(corridors) != 1 {
		t.Fatal("valid curated route rejected", err)
	}
	a.owner.cfg.roadCorridors[0].Evidence = ""
	if err := os.WriteFile(path, mustJSON(a.owner.cfg.roadCorridors), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRoadCorridors(path); err == nil {
		t.Fatal("unverified geometry labeled road")
	}
	original := &task{RoadPoints: [][]float64{{1, 2, 3}}, RoadPoint: 0}
	copy := cloneTask(original)
	copy.RoadPoints[0][0] = 99
	if original.RoadPoints[0][0] != 1 {
		t.Fatal("worker task aliases active road points")
	}
}

func TestRoadCorridorWatchdogCancelsBeforeOrdinaryFallback(t *testing.T) {
	a, p := roadTestActor(t)
	a.startDestinationTask(map[string]json.RawMessage{"destination": mustJSON("Town")})
	nextLoopCommand(t, p)
	a.state.Task.LastProgressUTC = time.Now().Add(-travelOperationTimeout - time.Minute)
	a.advanceTask()
	if nextLoopCommand(t, p).Operation != "cancel" || nextLoopCommand(t, p).Operation != "navigate_to_destination" || a.state.Task.Phase != "travel" {
		t.Fatal("road watchdog did not release native ownership before fallback")
	}
}
