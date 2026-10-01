package main

import (
	"encoding/json"
	"testing"
)

func negotiatedMaterialTrade() materialTrade {
	var trade materialTrade
	_ = json.Unmarshal([]byte(`{"active":true,"window_open":true,"revision":7,"partner_guid":"seller","bot_offer":{"money_copper":10},"partner_offer":{"accepted":true,"items":[{"slot":0,"item_id":3,"count":2}]}}`), &trade)
	return trade
}

func TestCraftingReceivesNegotiatedTradeAndChecksCorrelatedInventory(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	receive := nextLoopCommand(t, commands)
	if receive.Operation != "receive_trade_material" || a.state.Task.Phase != "trade_material" || a.state.Task.CraftSupplies[0].TradeRevision != 7 {
		t.Fatal("negotiated trade not nested in crafting")
	}
	a.handleTaskOperation(fishingResult(receive.OperationID, "accepted", "native loop started"))
	if a.state.Task.CraftSupplyIndex != 0 {
		t.Fatal("accepted trade counted as transferred items")
	}
	a.handleTaskOperation(fishingResult(receive.OperationID, "completed", "trade material transfer verified"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent("unrelated", 3, 2))
	if a.state.Task.Phase != "observe" {
		t.Fatal("unrelated inventory verified transfer")
	}
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 2))
	cast := nextLoopCommand(t, commands)
	if cast.Operation != "craft_once" || a.state.Task.CraftSupplyIndex != 1 {
		t.Fatal("verified trade did not release crafting")
	}
}

func TestMaterialTradeRejectsNonconsensualOrUnsafeOffers(t *testing.T) {
	for _, scenario := range []string{"not_accepted", "closed", "expensive", "spell", "give_item", "other_item", "nontraded_slot", "too_many", "partner_money"} {
		t.Run(scenario, func(t *testing.T) {
			a, _ := craftingActor(t)
			trade := negotiatedMaterialTrade()
			switch scenario {
			case "not_accepted":
				trade.Theirs.Accepted = false
			case "closed":
				trade.WindowOpen = false
			case "expensive":
				trade.Ours.Money = 11
			case "spell":
				trade.Theirs.SpellID = 1
			case "give_item":
				trade.Ours.Items = trade.Theirs.Items
			case "other_item":
				trade.Theirs.Items[0].ItemID = 999
			case "nontraded_slot":
				trade.Theirs.Items[0].Slot = 6
			case "too_many":
				trade.Theirs.Items[0].Count = 41
			case "partner_money":
				trade.Theirs.Money = 1
			}
			a.latest.Trade = mustJSON(trade)
			if _, err := a.tradeMaterialSupply(craftReagent{ItemID: 3, Count: 1}, 0, 10); err == nil {
				t.Fatal("unsafe offer accepted")
			}
		})
	}
}

func TestMaterialTradeNoBudgetOrUncertainTransferDoesNotReplay(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	previous := &task{Kind: "follow_player"}
	a.state.Task = previous
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1}`})
	if a.state.Task != previous {
		t.Fatal("missing budget authorized a trade")
	}
	a.state.Task = nil
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	receive := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(receive.OperationID, "rejected", "offer changed"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain trade replayed")
	}
}

func TestMaterialTradeChangedRevisionBlocksBeforeDispatch(t *testing.T) {
	a, _ := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	supply, err := a.tradeMaterialSupply(craftReagent{ItemID: 3, Count: 1}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	supply.TradeRevision = 6
	a.state.Task = &task{Kind: "crafting", Phase: "select", CraftSupplies: []craftSupply{supply}, MapID: a.latest.Bot.MapID}
	a.advanceTask()
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("changed negotiated offer dispatched")
	}
}

func TestMaterialTradeOwnerLossNeverReplaysAcceptance(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	_ = nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "bot_online", OwnerToken: "replacement", Payload: json.RawMessage(`{}`)})
	a.handleEvent(event{Type: "snapshot", OwnerToken: "replacement", Payload: json.RawMessage(`{"schema_version":1}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("owner loss replayed trade acceptance")
	}
}

func TestMaterialTradeMissingInventoryDoesNotAdvance(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	receive := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(receive.OperationID, "completed", "trade material transfer verified"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 0))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("missing traded inventory released crafting")
	}
}

func TestOrdinaryCraftingStillPausesDuringTrade(t *testing.T) {
	a, _ := craftingActor(t)
	a.latest.Trade = mustJSON(negotiatedMaterialTrade())
	a.state.Task = &task{Kind: "crafting", Phase: "select", MapID: a.latest.Bot.MapID, CraftSteps: []craftingStep{{SpellID: 200, ItemID: 2, Casts: 1}}}
	a.advanceTask()
	if a.state.Task.OperationID != "" || a.state.Task.Phase != "select" {
		t.Fatal("material-trade exemption unpaused ordinary crafting")
	}
}

func TestCraftingMultiMaterialTradeRequiresEveryReceivedMaterial(t *testing.T) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.CraftingRecipes = []craftRecipeInfo{{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 3, Count: 1}, {ItemID: 4, Count: 1}}}}
	trade := negotiatedMaterialTrade()
	second := trade.Theirs.Items[0]
	second.Slot, second.ItemID, second.Count = 1, 4, 1
	trade.Theirs.Items = append(trade.Theirs.Items, second)
	a.latest.Trade = mustJSON(trade)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	receive := nextLoopCommand(t, commands)
	if receive.Operation != "receive_trade_material" || len(a.state.Task.CraftSupplies[0].TradeMaterials) != 2 {
		t.Fatal("multi-material trade not planned as one atomic native exchange")
	}
	copy := cloneTask(a.state.Task)
	copy.CraftSupplies[0].TradeMaterials[0].Count = 99
	copy.CraftSupplies[0].RequiredMaterials[0].Count = 99
	if a.state.Task.CraftSupplies[0].TradeMaterials[0].Count != 2 || a.state.Task.CraftSupplies[0].RequiredMaterials[0].Count != 2 {
		t.Fatal("decision clone aliases material bundle")
	}
	a.handleTaskOperation(fishingResult(receive.OperationID, "completed", "trade material transfer verified"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 2))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("one material verified an entire exchange")
	}
}
