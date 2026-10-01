package main

import (
	"encoding/json"
	"testing"
	"time"
)

func acceptTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.latest.AvailableQuests = []availableQuest{{QuestID: 78467, Title: "A Fishy Peril", GiverName: "Marshal Dughan"}}
	return a, commands
}

func TestAcceptQuestTaskValidation(t *testing.T) {
	a, _ := acceptTestActor(t)
	a.startAcceptQuestTask(map[string]json.RawMessage{})
	if a.state.Task != nil {
		t.Fatal("missing quest_id started a task")
	}
	a.startAcceptQuestTask(map[string]json.RawMessage{"quest_id": json.RawMessage(`999`)})
	if a.state.Task != nil {
		t.Fatal("unknown quest started a task")
	}
	a.latest.Quests = []questInfo{{QuestID: 78467, StatusName: "incomplete"}}
	a.startAcceptQuestTask(map[string]json.RawMessage{"quest_id": json.RawMessage(`78467`)})
	if a.state.Task != nil {
		t.Fatal("quest already in the log started a task")
	}
}

func TestAcceptQuestTravelsThenAccepts(t *testing.T) {
	a, commands := acceptTestActor(t)
	a.startAcceptQuestTask(map[string]json.RawMessage{"quest_id": json.RawMessage(`78467`)})
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "navigate_to_quest_giver" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	if a.state.Task.Phase != "travel" || a.state.Task.QuestID != 78467 {
		t.Fatalf("phase=%s quest=%d", a.state.Task.Phase, a.state.Task.QuestID)
	}
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": a.state.Task.OperationID, "status": "completed", "persistent": true,
	})})
	if a.state.Task.Phase != "accept" {
		t.Fatalf("phase=%s", a.state.Task.Phase)
	}
	cmd = nextLoopCommand(t, commands)
	if cmd.Operation != "accept_quest" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	var args struct {
		QuestID uint32 `json:"quest_id"`
	}
	if err := json.Unmarshal(cmd.Arguments, &args); err != nil || args.QuestID != 78467 {
		t.Fatalf("accept arguments quest_id=%d err=%v", args.QuestID, err)
	}
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": a.state.Task.OperationID, "status": "completed", "persistent": true,
	})})
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("accepted quest did not complete the task")
	}
}

func TestAcceptQuestSeesQuestInLogOnSupervision(t *testing.T) {
	a, _ := acceptTestActor(t)
	a.startAcceptQuestTask(map[string]json.RawMessage{"quest_id": json.RawMessage(`78467`)})
	a.latest.Quests = []questInfo{{QuestID: 78467, StatusName: "incomplete"}}
	a.advanceAcceptQuestTask(a.state.Task)
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("quest appearing in the log did not complete the task")
	}
}

func TestAcceptQuestRejectionFallsBackToTravel(t *testing.T) {
	a, commands := acceptTestActor(t)
	a.startAcceptQuestTask(map[string]json.RawMessage{"quest_id": json.RawMessage(`78467`)})
	opID := a.state.Task.OperationID
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": opID, "status": "rejected", "reason": "quest destination unavailable",
	})})
	if a.state.Task == nil || a.state.Task.Phase != "select" {
		t.Fatalf("phase=%s", a.state.Task.Phase)
	}
	a.advanceAcceptQuestTask(a.state.Task)
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "navigate_to_quest_giver" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	// Three repeated navigation failures block the task for the model.
	opID = a.state.Task.OperationID
	for i := 0; i < 3; i++ {
		a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
			"operation_id": opID, "status": "rejected", "reason": "quest destination unavailable",
		})})
		if a.state.Task == nil {
			break
		}
		opID = a.state.Task.OperationID
		a.advanceAcceptQuestTask(a.state.Task)
	}
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("repeated navigation failures did not block the task")
	}
}

func TestAcceptQuestPersistentOperationsRetainOwnership(t *testing.T) {
	for _, tc := range []struct{ kind, phase, operation string }{
		{"accept_quest", "travel", "navigate_to_quest_giver"},
		{"accept_quest", "accept", "accept_quest"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot", ownerToken: "owner"}
			a.state.Task = &task{Kind: tc.kind, Phase: tc.phase, OperationID: "op", QuestID: 78467, LastProgressUTC: time.Now()}
			a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
				"operation_id": "op", "operation": tc.operation, "status": "accepted", "persistent": true,
			})})
			if a.state.Task.OperationID != "op" || a.state.Task.Phase != tc.phase {
				t.Fatal("acceptance completed or released the lower-level loop")
			}
		})
	}
}

func TestSnapshotRetainsNearbyPlayerInGroup(t *testing.T) {
	a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot", online: true, bridgeActive: true}
	a.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(
		`{"schema_version":1,"bot":{"guid":"GUID_Full__0x0000000000000001_Type__Player_Low__1"},"nearby_players":[{"guid":"g1","guid_raw":"2","name":"Peepee","in_group":true}]}`)})
	if len(a.latest.NearbyPlayers) != 1 || !a.latest.NearbyPlayers[0].InGroup {
		t.Fatal("in_group flag was dropped from nearby player info")
	}
}

func TestSnapshotRetainsAvailableQuests(t *testing.T) {
	a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot", online: true, bridgeActive: true}
	a.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(
		`{"schema_version":1,"bot":{"guid":"GUID_Full__0x0000000000000001_Type__Player_Low__1"},"available_quests":[{"quest_id":78467,"title":"A Fishy Peril","giver_name":"Marshal Dughan","giver_guid":"5"}]}`)})
	if len(a.latest.AvailableQuests) != 1 || a.latest.AvailableQuests[0].QuestID != 78467 ||
		a.latest.AvailableQuests[0].GiverName != "Marshal Dughan" {
		t.Fatal("available_quests were dropped from the snapshot")
	}
}
