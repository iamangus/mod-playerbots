package main

import (
	"log"
	"time"
)

const diagnosticRepeatInterval = 30 * time.Second

func eventDeliveryDelay(timestamp int64) int64 {
	if timestamp <= 0 {
		return 0
	}
	elapsed := time.Since(time.UnixMilli(timestamp)).Milliseconds()
	if elapsed < 0 {
		return 0
	}
	return elapsed
}

func (a *actor) logDecisionState(stage, trigger, traceID string) {
	kind, phase, operationID := "none", "none", ""
	if current := a.state.Task; current != nil {
		kind, phase, operationID = current.Kind, current.Phase, current.OperationID
	}
	newEvents := 0
	for _, recent := range a.recent {
		if recent.New {
			newEvents++
		}
	}
	age := int64(0)
	if !a.snapshotUpdatedAt.IsZero() {
		age = time.Since(a.snapshotUpdatedAt).Milliseconds()
	}
	// Metadata only: never print model credentials, prompts, raw arguments,
	// personas, memory, or private chat contents.
	log.Printf("agent decision_%s bot=%s trace_id=%q trigger=%q task_kind=%s phase=%s operation_id=%q new_events=%d snapshot_age_ms=%d combat=%t flight=%t group_size=%d queue_depth=%d",
		stage, a.botGUID, traceID, trigger, kind, phase, operationID, newEvents, age,
		a.latest.Bot.InCombat, a.latest.Bot.InFlight, a.latest.Bot.GroupSize, len(a.owner.modelJobs))
}

func (a *actor) logDecisionBlock(block, trigger string) {
	if a.lastDecisionBlock == block && time.Since(a.lastDecisionBlockAt) < diagnosticRepeatInterval {
		return
	}
	a.lastDecisionBlock, a.lastDecisionBlockAt = block, time.Now()
	log.Printf("agent decision_deferred bot=%s block=%s trigger=%q queue_depth=%d pending_reason=%q",
		a.botGUID, block, trigger, len(a.owner.modelJobs), a.pendingDecisionReason)
}

func (a *actor) logTaskWait(reason string) {
	current := a.state.Task
	if current == nil {
		return
	}
	key := current.ID + ":" + reason
	if a.lastTaskWait == key && time.Since(a.lastTaskWaitAt) < diagnosticRepeatInterval {
		return
	}
	a.lastTaskWait, a.lastTaskWaitAt = key, time.Now()
	age := int64(0)
	if !current.LastProgressUTC.IsZero() {
		age = time.Since(current.LastProgressUTC).Milliseconds()
	}
	log.Printf("agent task_wait bot=%s task_id=%s kind=%s phase=%s block=%s operation_id=%q progress=%d/%d progress_age_ms=%d",
		a.botGUID, current.ID, current.Kind, current.Phase, reason, current.OperationID,
		current.Completed, current.GoalCount, age)
}

func (a *actor) clearTaskWait() {
	if a.lastTaskWait == "" {
		return
	}
	log.Printf("agent task_wait_cleared bot=%s previous=%q", a.botGUID, a.lastTaskWait)
	a.lastTaskWait = ""
}
