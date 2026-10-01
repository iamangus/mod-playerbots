package main

import (
	"encoding/json"
	"testing"
	"time"
)

func craftingActor(t *testing.T) (*actor, <-chan command) {
	a, commands := loopTestActor(t)
	a.latest.CraftingRecipes = []craftRecipeInfo{
		{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: 1}}},
		{SpellID: 200, ItemID: 2, Yield: 1, Reagents: []craftReagent{{ItemID: 3, Count: 1}}},
	}
	if err := json.Unmarshal([]byte(`{"items":[{"item_id":3,"count":1}]}`), &a.latest.Inventory); err != nil {
		t.Fatal(err)
	}
	return a, commands
}

func TestCraftingSequencesSubcastsAndRequiresCorrelatedInventory(t *testing.T) {
	a, commands := craftingActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	first := nextLoopCommand(t, commands)
	if first.Operation != "craft_once" || a.state.Task.CraftSteps[0].SpellID != 200 {
		t.Fatal("subcraft was not dispatched first")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "accepted", "native loop started"))
	if a.state.Task.CraftStepIndex != 0 || a.state.Task.OperationID != first.OperationID {
		t.Fatal("accepted cast counted as output")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "crafted output acquired"))
	observation := nextLoopCommand(t, commands)
	if observation.Operation != "snapshot" || a.state.Task.Phase != "observe" {
		t.Fatal("did not request fresh inventory between casts")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "crafted output acquired"))
	if a.state.Task.CraftStepIndex != 1 {
		t.Fatal("duplicate completion advanced the plan")
	}
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: "unrelated", Payload: json.RawMessage(`{"schema_version":1,"inventory":{"items":[{"item_id":2,"count":1}]}}`)})
	if a.state.Task.OperationID != "" || a.state.Task.Phase != "observe" {
		t.Fatal("unrelated snapshot released the observation gate")
	}
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: observation.RequestID, Payload: json.RawMessage(`{"schema_version":1,"inventory":{"items":[{"item_id":2,"count":1}]}}`)})
	if a.state.Task == nil || a.state.Task.Phase != "casting" {
		t.Fatalf("correlated observation did not dispatch: task=%+v requested=%s online=%v bridge=%v", a.state.Task, observation.RequestID, a.online, a.bridgeActive)
	}
	second := nextLoopCommand(t, commands)
	if second.Operation != "craft_once" || a.state.Task.CraftStepIndex != 1 {
		t.Fatal("fresh inventory did not start the parent recipe")
	}
	a.handleTaskOperation(fishingResult(second.OperationID, "completed", "crafted output acquired"))
	final := nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: final.RequestID, Payload: json.RawMessage(`{"schema_version":1,"inventory":{"items":[{"item_id":1,"count":1}]}}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("verified output did not finish the parent")
	}
}

func TestCraftingRejectsMissingMaterialsWithoutReplacingTask(t *testing.T) {
	a, _ := craftingActor(t)
	a.latest.Inventory.Items = nil
	previous := &task{Kind: "follow_player"}
	a.state.Task = previous
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	if a.state.Task != previous {
		t.Fatal("unsupported material acquisition displaced an existing task")
	}
}

func TestCraftingUncertainFailureNeverReplaysCast(t *testing.T) {
	a, commands := craftingActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	cmd := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(cmd.OperationID, "rejected", "cast interrupted"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain cast was replayed")
	}
}

func TestCraftingClonesAndPersistsDependencySteps(t *testing.T) {
	original := &task{Kind: "crafting", CraftSteps: []craftingStep{{SpellID: 100, ItemID: 1, Casts: 2}},
		CraftSnapshotID: "snapshot", Phase: "observe", CraftStepCasts: 1,
		CraftSupplies: []craftSupply{{NPCGUID: "vendor", ItemID: 3, Bundles: 1, Budget: 10}}}
	copy := cloneTask(original)
	copy.CraftSteps[0].Casts = 30
	copy.CraftSupplies[0].Budget = 100
	if original.CraftSteps[0].Casts != 2 || original.CraftSupplies[0].Budget != 10 {
		t.Fatal("decision clone aliases the crafting plan")
	}
	data := mustJSON(original)
	var restored task
	if json.Unmarshal(data, &restored) != nil || restored.CraftSnapshotID != "snapshot" || restored.CraftStepCasts != 1 || restored.CraftSteps[0].Casts != 2 {
		t.Fatal("crafting execution state did not survive persistence")
	}
}

func TestCraftingOwnerLossDoesNotReplayInFlightCast(t *testing.T) {
	a, commands := craftingActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	_ = nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "bot_online", OwnerToken: "replacement", Payload: json.RawMessage(`{}`)})
	a.handleEvent(event{Type: "snapshot", OwnerToken: "replacement", Payload: json.RawMessage(`{"schema_version":1,"inventory":{}}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain cast was replayed on a replacement core")
	}
}

func TestCraftingObservationRetriesAreBounded(t *testing.T) {
	a, _ := craftingActor(t)
	a.state.Task = &task{Kind: "crafting", Phase: "observe", MapID: 0, CraftObservationAttempts: 3,
		LastProgressUTC: time.Now().Add(-time.Minute)}
	a.advanceTask()
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("crafting waited indefinitely for inventory observations")
	}
}

func TestCraftingProcuresObservedMaterialsWithinOneTotalBudget(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.NearbyNPCs = []npcInfo{{GUID: "vendor", NPCFlags: npcFlagVendor,
		VendorOffers: []vendorOfferInfo{{ItemID: 3, BundleCount: 5, PriceCopper: 10}}}}
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	buy := nextLoopCommand(t, commands)
	if buy.Operation != "buy_vendor_item" || a.state.Task.Phase != "procure" || a.state.Task.CraftSupplies[0].Budget != 10 {
		t.Fatal("observed vendor supply was not used with a reserved allowance")
	}
	a.handleTaskOperation(fishingResult(buy.OperationID, "completed", "bounded NPC service batch completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: observation.RequestID,
		Payload: json.RawMessage(`{"schema_version":1,"inventory":{"items":[{"item_id":3,"count":5}]}}`)})
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "craft_once" || a.state.Task.CraftSupplyIndex != 1 {
		t.Fatal("verified procurement did not release subcraft execution")
	}
}

func TestCraftingProcurementBudgetAndSourcesAreBounded(t *testing.T) {
	npcs := []npcInfo{{GUID: "vendor", NPCFlags: npcFlagVendor, VendorOffers: []vendorOfferInfo{
		{ItemID: 2, BundleCount: 1, PriceCopper: 10}, {ItemID: 3, BundleCount: 1, PriceCopper: 10},
	}}}
	missing := []craftReagent{{ItemID: 2, Count: 1}, {ItemID: 3, Count: 1}}
	if _, err := craftingSupplies(missing, npcs, 19); err == nil {
		t.Fatal("each material reused the whole total spending allowance")
	}
	supplies, err := craftingSupplies(missing, npcs, 20)
	if err != nil || len(supplies) != 2 || supplies[0].Budget+supplies[1].Budget != 20 {
		t.Fatalf("bad reserved budget: %+v, %v", supplies, err)
	}
	if _, err := craftingSupplies([]craftReagent{{ItemID: 4, Count: 1}}, npcs, 100); err == nil {
		t.Fatal("unknown material source was invented")
	}
	if _, err := craftingSupplies([]craftReagent{{ItemID: 2, Count: 41}}, npcs, 1000); err == nil {
		t.Fatal("unbounded purchase batch")
	}
}

func TestCraftingUsesFocusedInventoryBeyondGenericSnapshotLimit(t *testing.T) {
	var state snapshot
	if err := json.Unmarshal([]byte(`{"inventory":{"items":[{"item_id":1,"count":9}]},"crafting_inventory":[{"item_id":1,"count":0},{"item_id":90000,"count":7}]}`), &state); err != nil {
		t.Fatal(err)
	}
	stock := craftingStock(state)
	if stock[1] != 0 || stock[90000] != 7 {
		t.Fatal("focused crafting counts were lost or overwritten by bounded general inventory")
	}
}
