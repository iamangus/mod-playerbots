package main

import (
	"encoding/json"
	"testing"
	"time"
)

func observedGatheringSite(a *actor, at time.Time) {
	a.observeGatheringSource(event{Timestamp: at.UnixMilli(), Payload: json.RawMessage(`{"source_kind":"gameobject","source_entry":1731,"source_guid":"old-node","map_id":0,"position":[500,0,0],"item_ids":[3]}`)}, at)
}

func TestCraftingTravelsToPreviouslyObservedGatheringSite(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Bot.Position = []float64{0, 0, 0}
	a.latest.Inventory.Items = nil
	observedGatheringSite(a, time.Now())
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	travel := nextLoopCommand(t, commands)
	if travel.Operation != "navigate_to_position" || a.state.Task.Phase != "gather_travel" || a.state.Task.CraftGatherSitesTried[0] != "old-node" {
		t.Fatal("nonlocal source was not realized as a travel child")
	}
	a.handleTaskOperation(fishingResult(travel.OperationID, "completed", "observed position reached"))
	observation := nextLoopCommand(t, commands)
	if observation.Operation != "snapshot" {
		t.Fatal("arrival skipped fresh gathering eligibility")
	}
	state := craftingInventoryEvent(observation.RequestID, 3, 0)
	state.Payload = json.RawMessage(`{"schema_version":1,"bot":{"position":[500,0,0]},"crafting_inventory":[{"item_id":3,"count":0}],"nearby_game_objects":[{"guid":"respawned-node","entry":1731,"gather_skill":186,"can_gather":true,"distance":2}]}`)
	a.handleEvent(state)
	gather := nextLoopCommand(t, commands)
	if gather.Operation != "gather_target" || a.state.Task.CraftGatherTried[0] != "respawned-node" {
		t.Fatal("arrival tried stale source GUID instead of a live eligible node")
	}
}

func TestGatheringArrivalWithoutRespawnNeverInventsSource(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Bot.Position = []float64{0, 0, 0}
	a.latest.Inventory.Items = nil
	observedGatheringSite(a, time.Now())
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	travel := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(travel.OperationID, "completed", "observed position reached"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: observation.RequestID, Payload: json.RawMessage(`{"schema_version":1,"bot":{"position":[500,0,0]},"crafting_inventory":[{"item_id":3,"count":0}]}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("absent node was invented or revisited indefinitely")
	}
}

func TestGatheringSitesExpireAreBoundedAndRejectUnsafeCoordinates(t *testing.T) {
	a, _ := craftingActor(t)
	a.latest.Bot.Position = []float64{0, 0, 0}
	now := time.Now()
	observedGatheringSite(a, now)
	if a.knownGatheringSite(3, nil) == nil {
		t.Fatal("observed site missing")
	}
	if a.knownGatheringSite(3, []string{"old-node"}) != nil {
		t.Fatal("consumed site reused")
	}
	source := a.state.GatheringSources["gameobject:1731"]
	source.Sites[0].MapID = 1
	a.state.GatheringSources["gameobject:1731"] = source
	if a.knownGatheringSite(3, nil) != nil {
		t.Fatal("cross-map coordinate shortcut selected")
	}
	source.Sites[0].MapID = 0
	source.Sites[0].ObservedAt = now.Add(-2 * time.Hour).UnixMilli()
	a.state.GatheringSources["gameobject:1731"] = source
	if a.knownGatheringSite(3, nil) != nil {
		t.Fatal("expired location selected")
	}
	a.observeGatheringSource(event{Timestamp: now.UnixMilli(), Payload: json.RawMessage(`{"source_kind":"gameobject","source_entry":1731,"source_guid":"invalid","map_id":0,"position":[999999,0,0],"item_ids":[3]}`)}, now)
	if a.knownGatheringSite(3, nil) != nil {
		t.Fatal("unsafe position became a navigation goal")
	}
}

func TestGatheringSiteAndTravelProgressPersistWithoutAliasing(t *testing.T) {
	a, _ := craftingActor(t)
	a.latest.Bot.Position = []float64{0, 0, 0}
	observedGatheringSite(a, time.Now())
	var restored persistedAgent
	if json.Unmarshal(mustJSON(a.state), &restored) != nil || len(restored.GatheringSources["gameobject:1731"].Sites) != 1 {
		t.Fatal("gathering site did not survive restart")
	}
	site := a.knownGatheringSite(3, nil)
	site.Position[0] = 999
	if a.state.GatheringSources["gameobject:1731"].Sites[0].Position[0] != 500 {
		t.Fatal("selected site aliases persisted observation")
	}
	current := &task{CraftGatherSitesTried: []string{"old-node"}}
	copy := cloneTask(current)
	copy.CraftGatherSitesTried[0] = "changed"
	if current.CraftGatherSitesTried[0] != "old-node" {
		t.Fatal("decision copy aliases travel attempt budget")
	}
}
