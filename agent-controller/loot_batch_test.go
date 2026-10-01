package main

import (
	"encoding/json"
	"testing"
	"time"
)

func lootBatchTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.latest.NearbyCorpses = []corpseInfo{
		{GUID: "far", Distance: 30}, {GUID: "near", Distance: 5},
	}
	return a, commands
}

func lootBatchResult(operationID, status, reason string) event {
	return event{Payload: mustJSON(map[string]any{
		"operation_id": operationID, "operation": "loot_target",
		"status": status, "reason": reason, "persistent": true,
	})}
}

func TestLootBatchAdvancesFromTerminalEvents(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.startLootBatchTask(nil)
	first := nextLoopCommand(t, commands)
	var args struct {
		GUID       string `json:"target_guid"`
		CorpseOnly bool   `json:"corpse_only"`
	}
	if err := json.Unmarshal(first.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	if first.Operation != "loot_target" || args.GUID != "near" || !args.CorpseOnly || a.state.Task.GoalCount != 2 {
		t.Fatalf("incorrect first child: %+v args=%+v task=%+v", first, args, a.state.Task)
	}
	a.handleTaskOperation(lootBatchResult(first.OperationID, "accepted", "local loop started"))
	if a.state.Task.OperationID != first.OperationID || a.state.Task.Completed != 0 {
		t.Fatal("acceptance advanced the batch")
	}
	a.handleTaskOperation(lootBatchResult(first.OperationID, "completed", "corpse loot exhausted"))
	second := nextLoopCommand(t, commands)
	if second.Operation != "loot_target" || a.state.Task.TargetGUID != "far" || a.state.Task.Completed != 1 {
		t.Fatal("terminal event did not immediately start the next child")
	}
	// Redelivery of the previous terminal event must not consume the next corpse.
	a.handleTaskOperation(lootBatchResult(first.OperationID, "completed", "corpse loot exhausted"))
	if a.state.Task.Completed != 1 || a.state.Task.OperationID != second.OperationID {
		t.Fatal("duplicate terminal result advanced the next child")
	}
	a.handleTaskOperation(lootBatchResult(second.OperationID, "completed", "corpse has no remaining loot"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("batch did not complete after its last corpse")
	}
	var result struct {
		Completed uint32 `json:"completed"`
		Goal      uint32 `json:"goal"`
	}
	if err := json.Unmarshal(a.recent[len(a.recent)-1].Payload, &result); err != nil || result.Completed != 2 || result.Goal != 2 {
		t.Fatalf("incorrect batch outcome: %+v err=%v", result, err)
	}
}

func TestLootBatchValidatesRadiusWithoutReplacingTask(t *testing.T) {
	for _, radius := range []string{`0`, `101`, `-1`, `null`, `"wide"`, `1e999`, `{}`} {
		t.Run(radius, func(t *testing.T) {
			a, _ := lootBatchTestActor(t)
			previous := &task{Kind: "follow_player"}
			a.state.Task = previous
			a.startLootBatchTask(map[string]json.RawMessage{"radius": json.RawMessage(radius)})
			if a.state.Task != previous {
				t.Fatal("invalid radius replaced an existing task")
			}
		})
	}
}

func TestLootBatchFiltersAndFreezesItsTargets(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.latest.NearbyCorpses = append(a.latest.NearbyCorpses,
		corpseInfo{GUID: "near", Distance: 7}, corpseInfo{GUID: "outside", Distance: 81},
		corpseInfo{GUID: "", Distance: 1})
	a.startLootBatchTask(nil)
	_ = nextLoopCommand(t, commands)
	if a.state.Task.GoalCount != 2 {
		t.Fatal("duplicate/out-of-radius/empty GUID targets entered the batch")
	}
	previous := a.state.Task
	a.latest.NearbyCorpses = []corpseInfo{{GUID: "new", Distance: 1}}
	a.startLootBatchTask(nil)
	if a.state.Task != previous || a.state.Task.LootTargets[0] != "near" {
		t.Fatal("repeated tool call replaced the frozen batch")
	}
}

func TestLootBatchIsBoundedAndToolIsWired(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.latest.NearbyCorpses = nil
	for i := 0; i < maxLootBatchTargets+3; i++ {
		a.latest.NearbyCorpses = append(a.latest.NearbyCorpses, corpseInfo{GUID: newID(), Distance: float64(i + 1)})
	}
	a.applyTool(toolCall{Name: "loot_nearby", Arguments: `{}`})
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "loot_target" || a.state.Task.GoalCount != maxLootBatchTargets || len(a.state.Task.LootTargets) != maxLootBatchTargets {
		t.Fatal("tool dispatch did not create a bounded loot batch")
	}
	registered := false
	for _, tool := range agentTools {
		function := tool["function"].(map[string]any)
		if function["name"] == "loot_nearby" {
			registered = true
		}
	}
	if !registered {
		t.Fatal("loot_nearby missing from model tools")
	}
}

func TestLootBatchStopCancelsChildAndDropsQueue(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.startLootBatchTask(nil)
	_ = nextLoopCommand(t, commands)
	a.applyTool(toolCall{Name: "stop_task", Arguments: `{}`})
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "cancel" {
		t.Fatal("stop did not cancel the child")
	}
	if a.state.Task != nil || a.pendingDecisionReason != "task_cancelled" {
		t.Fatal("stop retained the batch queue")
	}
}

func TestLootBatchRequiresCorpseObservations(t *testing.T) {
	for _, corpses := range [][]corpseInfo{nil, {}} {
		a, _ := lootBatchTestActor(t)
		a.latest.NearbyCorpses = corpses
		a.startLootBatchTask(nil)
		if a.state.Task != nil {
			t.Fatal("missing or empty corpse observations started a batch")
		}
	}
}

func TestLootBatchFailuresRemainBoundedAcrossAcceptance(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.startLootBatchTask(nil)
	for attempt := 0; attempt < 3; attempt++ {
		cmd := nextLoopCommand(t, commands)
		if cmd.Operation == "snapshot" {
			cmd = nextLoopCommand(t, commands)
		}
		a.handleTaskOperation(lootBatchResult(cmd.OperationID, "accepted", "local loop started"))
		a.handleTaskOperation(lootBatchResult(cmd.OperationID, "rejected", "loot rights unavailable"))
		if attempt < 2 {
			if a.state.Task == nil || a.state.Task.Completed != 0 || a.state.Task.Retries != uint32(attempt+1) {
				t.Fatal("failure advanced/reset batch progress")
			}
			a.advanceTask()
		}
	}
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("repeated failures did not block the task")
	}
}

func TestLootBatchDoesNotInferSuccessFromDisappearance(t *testing.T) {
	a, commands := lootBatchTestActor(t)
	a.startLootBatchTask(nil)
	cmd := nextLoopCommand(t, commands)
	a.latest.NearbyCorpses = []corpseInfo{}
	a.advanceTask()
	if a.state.Task.Completed != 0 || a.state.Task.OperationID != cmd.OperationID {
		t.Fatal("snapshot disappearance released or advanced the child")
	}
	a.handleTaskOperation(lootBatchResult(cmd.OperationID, "completed", "operation target disappeared"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("unverified disappearance counted as loot completion")
	}
}

func TestLootBatchPausesBetweenChildrenForCombatAndTrade(t *testing.T) {
	for _, pause := range []string{"combat", "trade"} {
		t.Run(pause, func(t *testing.T) {
			a, commands := lootBatchTestActor(t)
			a.startLootBatchTask(nil)
			cmd := nextLoopCommand(t, commands)
			if pause == "combat" {
				a.latest.Bot.InCombat = true
			} else {
				a.latest.Trade = json.RawMessage(`{"active":true}`)
			}
			a.handleTaskOperation(lootBatchResult(cmd.OperationID, "completed", "corpse loot exhausted"))
			if a.state.Task.Completed != 1 || a.state.Task.OperationID != "" {
				t.Fatal("paused batch started the next child")
			}
			a.latest.Bot.InCombat = false
			a.latest.Trade = nil
			a.advanceTask()
			if next := nextLoopCommand(t, commands); next.Operation != "loot_target" || a.state.Task.TargetGUID != "far" {
				t.Fatal("batch did not resume at the next saved target")
			}
		})
	}
}

func TestLootBatchPersistsAndClonesRemainingQueue(t *testing.T) {
	original := &task{Kind: "loot_batch", LootTargets: []string{"near", "far"}, Phase: "select", GoalCount: 2}
	cloned := cloneTask(original)
	cloned.LootTargets[0] = "changed"
	if original.LootTargets[0] != "near" {
		t.Fatal("decision task clone shares the mutable loot queue")
	}
	encoded, err := json.Marshal(persistedAgent{Task: original})
	if err != nil {
		t.Fatal(err)
	}
	var restored persistedAgent
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	a, commands := lootBatchTestActor(t)
	a.state = restored
	a.advanceTask()
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "loot_target" || a.state.Task.TargetGUID != "near" {
		t.Fatal("restored queue did not resume")
	}
}

func TestLootBatchBlocksOnMapChangeAndDeath(t *testing.T) {
	for _, change := range []string{"map", "death"} {
		t.Run(change, func(t *testing.T) {
			a, _ := lootBatchTestActor(t)
			a.state.Task = &task{Kind: "loot_batch", LootTargets: []string{"near"}, LastProgressUTC: time.Now()}
			if change == "map" {
				a.latest.Bot.MapID = 1
			} else {
				alive := false
				a.latest.Bot.Alive = &alive
			}
			a.advanceTask()
			if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
				t.Fatal("batch ignored map change/death")
			}
		})
	}
}

func TestSnapshotRetainsCorpseObservations(t *testing.T) {
	var state snapshot
	if err := json.Unmarshal([]byte(`{"nearby_corpses":[{"guid":"123","name":"Wolf","distance":4,"position":[1,2,3]}]}`), &state); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip snapshot
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || len(roundTrip.NearbyCorpses) != 1 || roundTrip.NearbyCorpses[0].GUID != "123" {
		t.Fatalf("corpse observations lost on model observation round trip: %s err=%v", encoded, err)
	}
}
