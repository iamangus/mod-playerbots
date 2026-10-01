package main

import (
	"encoding/json"
	"testing"
)

func fishingTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.latest.Professions = map[string]uint32{"fishing": 1}
	return a, commands
}

func fishingResult(operationID, status, reason string) event {
	return event{Payload: mustJSON(map[string]any{"operation_id": operationID,
		"status": status, "reason": reason, "persistent": true})}
}

func TestFishingRunsPersistentCastLoopsUntilCatchCount(t *testing.T) {
	a, commands := fishingTestActor(t)
	a.applyTool(toolCall{Name: "fish_count", Arguments: `{"count":2}`})
	first := nextLoopCommand(t, commands)
	if first.Operation != "fish_once" {
		t.Fatal("fish_count did not start a native fishing cast loop")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "accepted", "cast loop started"))
	if a.state.Task.Completed != 0 || a.state.Task.OperationID != first.OperationID {
		t.Fatal("starting a cast counted as a catch")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "fishing catch acquired"))
	second := nextLoopCommand(t, commands)
	if second.Operation != "fish_once" || a.state.Task.Completed != 1 {
		t.Fatal("verified catch did not start the next cast")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "fishing catch acquired"))
	if a.state.Task.Completed != 1 {
		t.Fatal("duplicate catch event counted twice")
	}
	a.handleTaskOperation(fishingResult(second.OperationID, "completed", "fishing catch acquired"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("catch goal did not finish the fishing task")
	}
}

func TestFishingDoesNotCountClicksOrFailedCasts(t *testing.T) {
	a, commands := fishingTestActor(t)
	a.startFishingTask(map[string]json.RawMessage{"count": json.RawMessage(`1`)})
	for attempt := 0; attempt < 3; attempt++ {
		cmd := nextLoopCommand(t, commands)
		if cmd.Operation == "snapshot" {
			cmd = nextLoopCommand(t, commands)
		}
		a.handleTaskOperation(fishingResult(cmd.OperationID, "accepted", "cast loop started"))
		a.handleTaskOperation(fishingResult(cmd.OperationID, "completed", "bobber clicked"))
		if attempt < 2 {
			if a.state.Task.Completed != 0 || a.state.Task.Retries != uint32(attempt+1) {
				t.Fatal("unverified click counted as a catch or reset retries")
			}
			a.advanceTask()
		}
	}
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("repeated fishing failures were not bounded")
	}
}

func TestFishingRequiresSkillAndBoundedGoal(t *testing.T) {
	for _, count := range []string{`0`, `41`, `null`, `-1`, `"fish"`} {
		a, _ := fishingTestActor(t)
		a.startFishingTask(map[string]json.RawMessage{"count": json.RawMessage(count)})
		if a.state.Task != nil {
			t.Fatal("invalid catch count started a fishing task")
		}
	}
	a, _ := fishingTestActor(t)
	a.latest.Professions = nil
	a.startFishingTask(map[string]json.RawMessage{"count": json.RawMessage(`1`)})
	if a.state.Task != nil {
		t.Fatal("untrained fishing task started")
	}
}

func TestFishingPausesAndResumesWithoutChangingGoal(t *testing.T) {
	a, commands := fishingTestActor(t)
	a.latest.Bot.InCombat = true
	a.startFishingTask(map[string]json.RawMessage{"count": json.RawMessage(`1`)})
	if a.state.Task.OperationID != "" {
		t.Fatal("fishing cast started during combat")
	}
	a.latest.Bot.InCombat = false
	a.latest.Trade = json.RawMessage(`{"active":true}`)
	a.advanceTask()
	if a.state.Task.OperationID != "" {
		t.Fatal("fishing cast started during trade")
	}
	a.latest.Trade = nil
	a.advanceTask()
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "fish_once" || a.state.Task.GoalCount != 1 {
		t.Fatal("fishing failed to resume")
	}
}

func TestFishingStopsOnMapChange(t *testing.T) {
	a, _ := fishingTestActor(t)
	a.state.Task = &task{Kind: "fishing", MapID: 1, GoalCount: 1}
	a.advanceTask()
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("fishing continued after a map change")
	}
}
