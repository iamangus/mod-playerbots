package main

import (
	"encoding/json"
	"time"
)

func (a *actor) startFishingTask(args map[string]json.RawMessage) {
	var count uint32
	if json.Unmarshal(args["count"], &count) != nil || count == 0 || count > 40 {
		a.rejectTool("fish_count", "count must be between 1 and 40 catches")
		return
	}
	if !a.hasSnapshot || a.latest.Professions["fishing"] == 0 {
		a.rejectTool("fish_count", "learn fishing before starting a fishing loop")
		return
	}
	if current := a.state.Task; current != nil && current.Kind == "fishing" && current.GoalCount == count {
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "fishing", GoalCount: count,
		MapID: a.latest.Bot.MapID, Phase: "select", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) advanceFishingTask(current *task) {
	if a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "bot died or left the fishing map")
		return
	}
	if current.Completed >= current.GoalCount {
		a.finishTask("completed", "fishing catch count reached")
		return
	}
	if a.latest.Bot.InCombat {
		return
	}
	current.Phase = "fishing"
	current.OperationID = a.sendPrimitive("fish_once", map[string]any{})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleFishingOperation(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if status == "accepted" {
		a.finishTask("blocked", "fishing requires a persistent native cast/bite/loot loop")
		return
	}
	current.OperationID = ""
	if status != "completed" || reason != "fishing catch acquired" {
		current.Retries++
		if current.Retries >= 3 {
			a.finishTask("blocked", "fishing repeatedly failed: "+reason)
			return
		}
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	}
	current.Completed++
	current.Retries = 0
	current.Phase = "select"
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
	a.advanceTask()
}
