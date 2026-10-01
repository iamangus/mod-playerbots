package main

import (
	"encoding/json"
	"testing"
)

func TestQuestTurninUsesNativeReceiverWithIncompleteProjection(t *testing.T) {
	a, commands := loopTestActor(t)
	a.latest.Quests = []questInfo{{QuestID: 788, StatusName: "complete"}}
	a.latest.NearbyNPCs = []npcInfo{
		{GUID: "wrong", QuestGiver: true},
		{GUID: "receiver", QuestGiver: true},
	}
	a.state.Task = &task{ID: "quest", Kind: "quest", QuestID: 788, Phase: "turnin_giver"}
	a.advanceQuestTask(a.state.Task)
	cmd := nextLoopCommand(t, commands)
	var args map[string]any
	if err := json.Unmarshal(cmd.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	if cmd.Operation != "interact_quest_giver" || args["target_guid"] != nil {
		t.Fatalf("operation=%s args=%v", cmd.Operation, args)
	}
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": a.state.Task.OperationID, "operation": "interact_quest_giver",
		"status": "rejected", "reason": "quest giver or completed quest unavailable",
	})})
	nextLoopCommand(t, commands)
	// Failure budget must survive serialization, not just this actor tick.
	var restored task
	if err := json.Unmarshal(mustJSON(a.state.Task), &restored); err != nil {
		t.Fatal(err)
	}
	a.state.Task = &restored
	a.advanceQuestTask(a.state.Task)
	cmd = nextLoopCommand(t, commands)
	if cmd.Operation != "interact_quest_giver" || a.state.Task.Retries != 1 {
		t.Fatalf("operation=%s target=%s", cmd.Operation, a.state.Task.TargetGUID)
	}
}

func TestQuestTurninArrivalsAreBoundedAfterRestart(t *testing.T) {
	a, commands := loopTestActor(t)
	a.latest.Quests = []questInfo{{QuestID: 788, StatusName: "complete"}}
	a.latest.NearbyNPCs = []npcInfo{{GUID: "wrong", QuestGiver: true}}
	a.state.Task = &task{ID: "quest", Kind: "quest", QuestID: 788, Phase: "select",
		QuestTurninAttempts: 3}
	var restored task
	if err := json.Unmarshal(mustJSON(a.state.Task), &restored); err != nil {
		t.Fatal(err)
	}
	a.state.Task = &restored
	a.advanceQuestTask(a.state.Task)
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("exhausted receivers restarted a navigation/interaction loop")
	}
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "cancel" {
		t.Fatalf("unexpected operation=%s", cmd.Operation)
	}
}
