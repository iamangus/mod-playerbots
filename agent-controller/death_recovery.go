package main

import "time"

func (a *actor) startDeathRecoveryTask() {
	if !a.hasSnapshot || a.latest.Bot.Alive == nil || *a.latest.Bot.Alive {
		a.rejectTool("recover_death", "a fresh observation of a dead bot is required")
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "recover_death" {
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "recover_death", Phase: "recovering",
		LastProgressUTC: time.Now().UTC()}
	a.state.Task.OperationID = a.sendPrimitive("recover_death", map[string]any{})
	a.persist()
}
