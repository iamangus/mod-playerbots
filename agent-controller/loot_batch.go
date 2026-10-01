package main

import (
	"encoding/json"
	"math"
	"sort"
	"time"
)

const maxLootBatchTargets = 12

func (a *actor) startLootBatchTask(args map[string]json.RawMessage) {
	radius := 80.0
	if raw, supplied := args["radius"]; supplied {
		if string(raw) == "null" || json.Unmarshal(raw, &radius) != nil ||
			math.IsNaN(radius) || math.IsInf(radius, 0) || radius < 1 || radius > 100 {
			a.rejectTool("loot_nearby", "radius must be between 1 and 100 yards")
			return
		}
	}
	if !a.hasSnapshot || a.latest.NearbyCorpses == nil {
		a.rejectTool("loot_nearby", "corpse observations unavailable; request a fresh snapshot from a supporting core")
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "loot_batch" {
		return // Do not replace the batch or reset progress on a repeated choice.
	}
	candidates := append([]corpseInfo(nil), a.latest.NearbyCorpses...)
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Distance < candidates[j].Distance })
	seen := make(map[string]bool)
	targets := make([]string, 0, maxLootBatchTargets)
	for _, corpse := range candidates {
		if corpse.GUID == "" || seen[corpse.GUID] || math.IsNaN(corpse.Distance) ||
			math.IsInf(corpse.Distance, 0) || corpse.Distance < 0 || corpse.Distance > radius {
			continue
		}
		seen[corpse.GUID] = true
		targets = append(targets, corpse.GUID)
		if len(targets) == maxLootBatchTargets {
			break
		}
	}
	if len(targets) == 0 {
		a.rejectTool("loot_nearby", "no observed lootable corpses within the requested radius")
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "loot_batch", LootTargets: targets,
		GoalCount: uint32(len(targets)), Radius: radius, MapID: a.latest.Bot.MapID,
		Phase: "select", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) advanceLootBatchTask(current *task) {
	if a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "bot died or left the loot batch map")
		return
	}
	if len(current.LootTargets) == 0 {
		a.finishTask("completed", "observed corpse batch processed")
		return
	}
	if a.latest.Bot.InCombat {
		return // Safety behavior handles combat; do not start a loot movement.
	}
	current.TargetGUID = current.LootTargets[0]
	current.Phase = "loot"
	current.OperationID = a.sendPrimitive("loot_target", map[string]any{
		"target_guid": current.TargetGUID, "corpse_only": true,
	})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleLootBatchOperation(operationID, status, reason string, persistent bool) {
	current := a.state.Task
	if current == nil || current.OperationID != operationID {
		return
	}
	if status == "accepted" {
		if !persistent {
			// Older cores cannot enforce corpse-only mode. Do not replay a
			// one-shot acknowledgement as if it were a completed loot loop.
			a.finishTask("blocked", "loot batch requires persistent corpse-only loot support")
			return
		}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	current.OperationID = ""
	if status != "completed" {
		current.Retries++
		if current.Retries >= 3 {
			a.finishTask("blocked", "corpse loot repeatedly failed: "+reason)
			return
		}
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	}
	if reason != "corpse loot exhausted" && reason != "corpse has no remaining loot" {
		a.finishTask("blocked", "corpse loot completion was not verified: "+reason)
		return
	}
	if len(current.LootTargets) == 0 || current.TargetGUID != current.LootTargets[0] {
		a.finishTask("blocked", "loot batch target does not match its saved queue")
		return
	}
	current.LootTargets = current.LootTargets[1:]
	current.Completed++
	current.Retries = 0
	current.TargetGUID = ""
	current.Phase = "select"
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
	// The terminal event, not an LLM/timer decision, drives the next child.
	a.advanceTask()
}
