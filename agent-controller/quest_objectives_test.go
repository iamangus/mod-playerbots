package main

import (
	"encoding/json"
	"testing"
)

func itemQuestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.latest.Quests = []questInfo{{QuestID: 747, StatusName: "incomplete", Objectives: []questObjective{
		{Index: 0, ItemID: 4739, Count: 0, Required: 7, Sources: []int32{2955}},
		{Index: 1, ItemID: 4740, Count: 0, Required: 7, Sources: []int32{2956}},
	}}}
	a.state.Task = &task{ID: "quest", Kind: "quest", QuestID: 747, Phase: "select"}
	return a, commands
}

func TestQuestItemUsesObservedCreatureSource(t *testing.T) {
	a, commands := itemQuestActor(t)
	_ = json.Unmarshal([]byte(`[{"guid":"unrelated","entry":1},{"guid":"source","entry":2955,"distance":1}]`), &a.latest.NearbyCreatures)
	a.advanceQuestTask(a.state.Task)
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "engage_target" || a.state.Task.TargetGUID != "source" {
		t.Fatalf("operation=%s target=%s", cmd.Operation, a.state.Task.TargetGUID)
	}
	if a.state.Task.ObjectiveItemID != 4739 {
		t.Fatal("item objective identity was lost")
	}
}

func TestQuestItemObjectUsesLootLoopNotCreditInteraction(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Sources = []int32{-171938}
	_ = json.Unmarshal([]byte(`[{"guid":"apple","entry":171938,"can_gather":true}]`), &a.latest.NearbyGameObjects)
	a.advanceQuestTask(a.state.Task)
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "gather_target" || a.state.Task.Phase != "gathering" {
		t.Fatalf("operation=%s phase=%s", cmd.Operation, a.state.Task.Phase)
	}
}

func TestQuestItemSkipsUnavailableObject(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Sources = []int32{-182127}
	_ = json.Unmarshal([]byte(`[{"guid":"flower","entry":182127,"can_gather":false}]`), &a.latest.NearbyGameObjects)
	a.advanceQuestTask(a.state.Task)
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "navigate_to_quest_objective" {
		t.Fatalf("unavailable object dispatched: %s", cmd.Operation)
	}
}

func TestQuestObjectAttemptsSurviveRestartWithoutProgress(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Sources = []int32{-182127}
	_ = json.Unmarshal([]byte(`[{"guid":"flower","entry":182127,"can_gather":true}]`), &a.latest.NearbyGameObjects)
	for attempt := 0; attempt < 3; attempt++ {
		a.advanceQuestTask(a.state.Task)
		cmd := nextLoopCommand(t, commands)
		if cmd.Operation != "gather_target" {
			t.Fatalf("operation=%s", cmd.Operation)
		}
		a.pendingSnapshotID = ""
		a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
			"operation_id": a.state.Task.OperationID, "operation": "gather_target",
			"status": "completed", "persistent": true,
		})})
		nextLoopCommand(t, commands)
		var restored task
		if err := json.Unmarshal(mustJSON(a.state.Task), &restored); err != nil {
			t.Fatal(err)
		}
		a.state.Task = &restored
	}
	a.advanceQuestTask(a.state.Task)
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("object completion without quest progress restarted indefinitely")
	}
}

func TestQuestObjectRejectionsDoNotResetOnReselection(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Sources = []int32{-182127}
	_ = json.Unmarshal([]byte(`[{"guid":"flower","entry":182127,"can_gather":true}]`), &a.latest.NearbyGameObjects)
	for attempt := 0; attempt < 3; attempt++ {
		a.advanceQuestTask(a.state.Task)
		nextLoopCommand(t, commands)
		a.pendingSnapshotID = ""
		a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
			"operation_id": a.state.Task.OperationID, "operation": "gather_target",
			"status": "rejected", "reason": "target is not available to loot or gather",
		})})
		nextLoopCommand(t, commands)
	}
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("reselected rejected object reset failure budget")
	}
}

func TestQuestObjectBudgetResetsOnlyAfterObservedProgress(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Sources = []int32{-182127}
	_ = json.Unmarshal([]byte(`[{"guid":"flower","entry":182127,"can_gather":true}]`), &a.latest.NearbyGameObjects)
	a.state.Task.ObjectiveItemID = 4739
	a.state.Task.QuestObjectAttempts = 3
	a.latest.Quests[0].Objectives[0].Count = 1
	a.advanceQuestTask(a.state.Task)
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "gather_target" || a.state.Task.QuestObjectAttempts != 1 {
		t.Fatal("observed item gain did not reset the object budget")
	}
}

func TestQuestItemNavigationKeepsItemSlotAndProgress(t *testing.T) {
	a, commands := itemQuestActor(t)
	a.latest.Quests[0].Objectives[0].Count = 7
	a.advanceQuestTask(a.state.Task)
	cmd := nextLoopCommand(t, commands)
	var args map[string]uint32
	if err := json.Unmarshal(cmd.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	if cmd.Operation != "navigate_to_quest_objective" || args["objective_index"] != 1 || args["item_id"] != 4740 {
		t.Fatalf("operation=%s args=%v", cmd.Operation, args)
	}
	a.state.Task.OperationID = ""
	a.state.Task.Phase = "select"
	a.state.Task.QuestSearchAttempts = 2
	a.latest.Quests[0].Objectives[1].Count = 1
	a.advanceQuestTask(a.state.Task)
	nextLoopCommand(t, commands)
	if a.state.Task.QuestSearchAttempts != 1 {
		t.Fatal("actual item progress did not reset the source search budget")
	}
}

func TestQuestArrivalsWithoutObjectiveProgressAreBounded(t *testing.T) {
	a, commands := itemQuestActor(t)
	for attempt := 0; attempt < 3; attempt++ {
		a.advanceQuestTask(a.state.Task)
		nextLoopCommand(t, commands)
		a.pendingSnapshotID = "" // Previous requested observation has been delivered.
		a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
			"operation_id": a.state.Task.OperationID, "status": "completed", "persistent": true,
		})})
		nextLoopCommand(t, commands)
	}
	a.advanceQuestTask(a.state.Task)
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("arrival kept restarting the same depleted source search")
	}
}
