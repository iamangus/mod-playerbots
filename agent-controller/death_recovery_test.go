package main

import "testing"

func TestDeathRecoveryAdmissionIsNotResurrection(t *testing.T) {
	a, commands := loopTestActor(t)
	alive := false
	a.latest.Bot.Alive = &alive
	a.startDeathRecoveryTask()
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "recover_death" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": a.state.Task.OperationID, "status": "accepted", "persistent": true,
	})})
	if a.state.Task == nil || a.state.Task.OperationID == "" {
		t.Fatal("command admission claimed resurrection")
	}
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": a.state.Task.OperationID, "status": "completed", "persistent": true,
	})})
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("verified native resurrection did not finish recovery")
	}
}

func TestDeathRecoveryRejectsAliveOrUnknownState(t *testing.T) {
	for _, alive := range []*bool{nil, func() *bool { value := true; return &value }()} {
		a, _ := loopTestActor(t)
		a.latest.Bot.Alive = alive
		a.startDeathRecoveryTask()
		if a.state.Task != nil {
			t.Fatal("alive or unknown bot started corpse recovery")
		}
	}
}

func TestDeathEndsQuestOperationWithoutRetryStorm(t *testing.T) {
	a, _ := itemQuestActor(t)
	a.state.Task.OperationID = "combat"
	a.state.Task.Phase = "combat"
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
		"operation_id": "combat", "status": "rejected", "persistent": true,
		"reason": "bot died or left the operation map",
	})})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("dead quest task remained active for another combat retry")
	}
}
