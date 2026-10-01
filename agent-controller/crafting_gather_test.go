package main

import (
	"encoding/json"
	"testing"
	"time"
)

func gatheringCraftActor(t *testing.T) (*actor, <-chan command) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	if err := json.Unmarshal([]byte(`[{"guid":"node-a","entry":1731,"can_gather":true,"gather_skill":186,"distance":10},{"guid":"node-b","entry":1731,"can_gather":true,"gather_skill":186,"distance":20}]`), &a.latest.NearbyGameObjects); err != nil {
		t.Fatal(err)
	}
	a.observeGatheringSource(evidenceSample(1731, []uint32{3}, time.Now()), time.Now())
	return a, commands
}

func craftingInventoryEvent(requestID string, itemID, count uint32) event {
	return event{Type: "snapshot", OwnerToken: "owner", RequestID: requestID,
		Payload: mustJSON(map[string]any{"schema_version": 1, "crafting_inventory": []craftReagent{{ItemID: itemID, Count: count}},
			"nearby_game_objects": []map[string]any{{"guid": "node-b", "entry": 1731, "can_gather": true, "gather_skill": 186, "distance": 20}}})}
}

func TestCraftingGathersObservedMaterialsBeforeSubcraft(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	first := nextLoopCommand(t, commands)
	if first.Operation != "gather_target" || a.state.Task.Phase != "gather_material" || a.state.Task.CraftGatherTried[0] != "node-a" {
		t.Fatal("eligible observed node not selected")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "accepted", "native loop started"))
	if a.state.Task.CraftSupplyIndex != 0 {
		t.Fatal("accepted gathering advanced materials")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "operation target disappeared"))
	observation := nextLoopCommand(t, commands)
	var arguments map[string]uint32
	if json.Unmarshal(observation.Arguments, &arguments) != nil || arguments["craft_item_id"] != 3 || arguments["recipe_item_id"] != 1 {
		t.Fatal("material was not explicitly included in observation")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "operation target disappeared"))
	a.handleEvent(craftingInventoryEvent("unrelated", 3, 1))
	if a.state.Task.Phase != "observe" {
		t.Fatal("unrelated snapshot released material gate")
	}
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 1))
	cast := nextLoopCommand(t, commands)
	if cast.Operation != "craft_once" || a.state.Task.CraftSupplyIndex != 1 || a.state.Task.CraftSteps[0].SpellID != 200 {
		t.Fatal("verified material did not release subcraft")
	}
}

func TestCraftingGatheringPartialGainUsesNextNodeAndRetainsParent(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":2}`})
	first := nextLoopCommand(t, commands)
	id := a.state.Task.ID
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "loot or gather completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 1))
	second := nextLoopCommand(t, commands)
	if second.Operation != "gather_target" || a.state.Task.ID != id || len(a.state.Task.CraftGatherTried) != 2 || a.state.Task.CraftGatherTried[1] != "node-b" || a.state.Task.CraftGatherBefore != 1 {
		t.Fatal("partial gain lost parent or retried consumed source")
	}
}

func TestCraftingGatheringNoRequestedGainBlocksEvenWithOtherLoot(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	first := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "loot or gather completed"))
	observation := nextLoopCommand(t, commands)
	state := craftingInventoryEvent(observation.RequestID, 3, 0)
	state.Payload = json.RawMessage(`{"schema_version":1,"crafting_inventory":[{"item_id":3,"count":0},{"item_id":999,"count":10}]}`)
	a.handleEvent(state)
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("unrelated loot counted as acquired material")
	}
}

func TestCraftingGatheringUnknownExpiredOrIneligibleSourcesDoNotReplaceTask(t *testing.T) {
	for _, scenario := range []string{"unknown", "expired", "ineligible", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			a, _ := gatheringCraftActor(t)
			switch scenario {
			case "unknown":
				a.state.GatheringSources = nil
			case "expired":
				a.observeGatheringSource(evidenceSample(1731, []uint32{3}, time.Now().Add(-2*time.Hour)), time.Now())
				a.state.GatheringSources = map[string]gatheringEvidence{"gameobject:1731": {ItemIDs: []uint32{3}, ObservedAt: time.Now().Add(-2 * time.Hour).UnixMilli()}}
			case "ineligible":
				for i := range a.latest.NearbyGameObjects {
					a.latest.NearbyGameObjects[i].CanGather = false
				}
			case "truncated":
				a.latest.CraftingRecipePage = &craftRecipePage{DependenciesTruncated: true}
			}
			previous := &task{Kind: "follow_player"}
			a.state.Task = previous
			a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
			if a.state.Task != previous {
				t.Fatal("unsafe acquisition displaced active work")
			}
		})
	}
}

func TestCraftingGatheringUncertainChildDoesNotReplay(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	first := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(first.OperationID, "rejected", "watchdog timeout"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain child did not block")
	}
}

func TestCraftingGatheringStatePersistsAndClonesWithoutAliasing(t *testing.T) {
	original := &task{Kind: "crafting", CraftGatherBefore: 2, CraftGatherAwaiting: true, CraftGatherTried: []string{"node-a"}, CraftSupplies: []craftSupply{{Gather: true, ItemID: 3, RequiredCount: 5}}}
	copy := cloneTask(original)
	copy.CraftGatherTried[0] = "changed"
	copy.CraftSupplies[0].RequiredCount = 100
	if original.CraftGatherTried[0] != "node-a" || original.CraftSupplies[0].RequiredCount != 5 {
		t.Fatal("decision copy aliases gathering state")
	}
	var restored task
	if json.Unmarshal(mustJSON(original), &restored) != nil || !restored.CraftGatherAwaiting || restored.CraftGatherBefore != 2 || !restored.CraftSupplies[0].Gather || restored.CraftGatherTried[0] != "node-a" {
		t.Fatal("gathering state not persisted")
	}
}

func TestCraftingMixesVendorAndGatheringWithoutExceedingBudget(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.latest.CraftingRecipes = []craftRecipeInfo{{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 3, Count: 1}, {ItemID: 4, Count: 2}}}}
	a.latest.NearbyNPCs = []npcInfo{{GUID: "vendor", NPCFlags: npcFlagVendor, VendorOffers: []vendorOfferInfo{{ItemID: 4, BundleCount: 2, PriceCopper: 10}}}}
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	first := nextLoopCommand(t, commands)
	if first.Operation != "gather_target" || len(a.state.Task.CraftSupplies) != 2 || a.state.Task.CraftSupplies[1].Budget != 10 {
		t.Fatal("mixed acquisition did not reserve one total budget")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "loot or gather completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 1))
	buy := nextLoopCommand(t, commands)
	var args map[string]json.RawMessage
	var budget uint32
	if json.Unmarshal(buy.Arguments, &args) != nil || json.Unmarshal(args["max_spend_copper"], &budget) != nil || budget != 10 || buy.Operation != "buy_vendor_item" {
		t.Fatal("gathering disturbed reserved spending allowance")
	}
}

func TestCraftingGatheringExhaustionAndAttemptLimitBlock(t *testing.T) {
	for _, exhausted := range []bool{true, false} {
		a, _ := gatheringCraftActor(t)
		tried := []string{"node-a", "node-b"}
		if !exhausted {
			tried = make([]string, maxCraftingCasts)
			for i := range tried {
				tried[i] = "other-node"
			}
		}
		a.state.Task = &task{Kind: "crafting", Phase: "select", MapID: a.latest.Bot.MapID, CraftSupplies: []craftSupply{{Gather: true, ItemID: 3, RequiredCount: 2}}, CraftGatherTried: tried}
		a.advanceTask()
		if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
			t.Fatal("exhausted acquisition loop continued")
		}
	}
}

func TestCraftingGatheringOwnerChangeDoesNotReplay(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	_ = nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "bot_online", OwnerToken: "replacement", Payload: json.RawMessage(`{}`)})
	a.handleEvent(event{Type: "snapshot", OwnerToken: "replacement", Payload: json.RawMessage(`{"schema_version":1,"inventory":{}}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain gather was replayed on replacement owner")
	}
}

func TestCraftingInventoryGateCannotDriftDuringCombatWait(t *testing.T) {
	a, commands := gatheringCraftActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	first := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "loot or gather completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: observation.RequestID,
		Payload: json.RawMessage(`{"schema_version":1,"bot":{"in_combat":true},"crafting_inventory":[{"item_id":3,"count":0}]}`)})
	if a.state.Task == nil || a.state.Task.Phase != "select" {
		t.Fatal("combat did not defer postcondition evaluation")
	}
	var restored task
	if json.Unmarshal(mustJSON(a.state.Task), &restored) != nil || restored.CraftObservedStock == nil {
		t.Fatal("correlated inventory lost across persistence")
	}
	copy := cloneTask(a.state.Task)
	copy.CraftObservedStock[0].Count = 99
	if a.state.Task.CraftObservedStock[0].Count != 0 {
		t.Fatal("decision copy aliases correlated inventory")
	}
	a.handleEvent(craftingInventoryEvent("unrelated", 3, 10))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("later unrelated inventory replaced failed correlated postcondition")
	}
}
