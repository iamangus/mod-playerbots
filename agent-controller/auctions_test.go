package main

import (
	"encoding/json"
	"testing"
	"time"
)

func auctionActor(t *testing.T) (*actor, <-chan command) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.latest.NearbyNPCs = []npcInfo{{GUID: "auctioneer", NPCFlags: npcFlagAuctioneer}}
	if json.Unmarshal([]byte(`[{"guid":"mailbox"}]`), &a.latest.NearbyMailboxes) != nil {
		t.Fatal("invalid test mailboxes")
	}
	a.state.AuctionObservation = &auctionObservation{NPCGUID: "auctioneer", ItemID: 3, MapID: a.latest.Bot.MapID, ObservedAt: time.Now().UnixMilli(), Offers: []auctionOffer{
		{AuctionID: 10, ItemID: 3, Count: 2, Buyout: 10, ItemGUID: "1000"}, {AuctionID: 11, ItemID: 3, Count: 2, Buyout: 12, ItemGUID: "1001"},
	}}
	return a, commands
}

func auctionReceipt(a *actor, operationID string, supply craftSupply) event {
	return event{Type: "auction_observation", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(auctionObservation{
		OperationID: operationID, NPCGUID: supply.NPCGUID, MapID: a.latest.Bot.MapID, ItemID: supply.ItemID, PurchasedItemGUID: supply.ItemGUID,
	})}
}

func TestAuctionPurchaseRequiresBuyoutMailAndCorrelatedInventory(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "buy_auction", Arguments: `{"item_id":3,"count":1,"max_spend_copper":10}`})
	buy := nextLoopCommand(t, commands)
	if buy.Operation != "buy_auction" || a.state.Task.Kind != "auction_purchase" || a.state.Task.CraftSupplies[0].Budget != 10 {
		t.Fatal("purchase not planned from observed offers")
	}
	supply := a.state.Task.CraftSupplies[0]
	a.handleTaskOperation(fishingResult(buy.OperationID, "accepted", "native loop started"))
	if a.state.Task.Phase != "auction_buy" {
		t.Fatal("accepted buyout counted as items")
	}
	a.handleEvent(auctionReceipt(a, buy.OperationID, supply))
	a.handleTaskOperation(fishingResult(buy.OperationID, "completed", "auction buyout submitted; collect won mail"))
	collect := nextLoopCommand(t, commands)
	var args struct {
		ItemGUID string `json:"collect_item_guid"`
		Target   string `json:"target_guid"`
	}
	if collect.Operation != "collect_mail" || json.Unmarshal(collect.Arguments, &args) != nil || args.ItemGUID != "1000" || args.Target != "mailbox" {
		t.Fatal("buyout did not collect its exact won attachment")
	}
	a.handleTaskOperation(fishingResult(buy.OperationID, "completed", "auction buyout submitted; collect won mail"))
	if a.state.Task.OperationID != collect.OperationID {
		t.Fatal("duplicate buyout changed receipt phase")
	}
	a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "bounded mailbox collection completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent("unrelated", 3, 2))
	if a.state.Task.Phase != "observe" {
		t.Fatal("unrelated snapshot verified receipt")
	}
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 2))
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("collected auction was not completed after verified inventory")
	}
}

func TestCraftingProcuresAuctionsBeforeCasting(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	buy := nextLoopCommand(t, commands)
	if buy.Operation != "buy_auction" || a.state.Task.Kind != "crafting" {
		t.Fatal("auction was not nested in crafting")
	}
	a.handleEvent(auctionReceipt(a, buy.OperationID, a.state.Task.CraftSupplies[0]))
	a.handleTaskOperation(fishingResult(buy.OperationID, "completed", "auction buyout submitted; collect won mail"))
	collect := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "bounded mailbox collection completed"))
	observation := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 2))
	if cast := nextLoopCommand(t, commands); cast.Operation != "craft_once" || a.state.Task.CraftStepIndex != 0 {
		t.Fatal("verified procurement did not release subcraft")
	}
}

func TestAuctionInspectionsRequireCorrelatedBoundedPages(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "inspect_auctions", Arguments: `{"npc_guid":"auctioneer","item_id":3,"cursor":10}`})
	inspection := nextLoopCommand(t, commands)
	page := auctionObservation{OperationID: "unrelated", NPCGUID: "auctioneer", ItemID: 3, Cursor: 10, NextCursor: 11, More: true, Offers: []auctionOffer{{AuctionID: 11, ItemID: 3, Count: 2, Buyout: 12, ItemGUID: "1001"}}}
	previous := a.state.AuctionObservation
	sample := event{Type: "auction_observation", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(page)}
	a.handleEvent(sample)
	if a.state.AuctionObservation != previous {
		t.Fatal("unrelated page replaced current observation")
	}
	page.OperationID = inspection.OperationID
	sample.Payload = mustJSON(page)
	a.handleEvent(sample)
	if a.state.AuctionObservation == previous || a.latest.Auctions.NextCursor != 11 {
		t.Fatal("correlated auction page discarded")
	}
	a.handleTaskOperation(fishingResult(inspection.OperationID, "completed", "bounded auction page observed"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
		t.Fatal("inspection did not complete")
	}
}

func TestAuctionPlanningReservesOneBudgetAndNeverReturnsPartialPlans(t *testing.T) {
	a, _ := auctionActor(t)
	supplies, err := a.auctionSupplies(craftReagent{ItemID: 3, Count: 3}, 5, 22)
	if err != nil || len(supplies) != 2 || supplies[0].Budget+supplies[1].Budget != 22 || supplies[1].RequiredCount != 9 {
		t.Fatal("auction stack rounding or total reservation failed")
	}
	if partial, err := a.auctionSupplies(craftReagent{ItemID: 3, Count: 3}, 0, 21); err == nil || partial != nil {
		t.Fatal("partial purchase plan escaped on insufficient budget")
	}
	if a.state.AuctionObservation.Offers[0].AuctionID != 10 {
		t.Fatal("planning mutated live observation")
	}
}

func TestAuctionUnsafeSourcesDoNotReplaceExistingWork(t *testing.T) {
	for _, scenario := range []string{"expired", "mailbox_missing", "auctioneer_missing", "budget_missing", "budget_low", "invalid_count"} {
		t.Run(scenario, func(t *testing.T) {
			a, _ := auctionActor(t)
			args := `{"item_id":3,"count":1,"max_spend_copper":10}`
			switch scenario {
			case "expired":
				a.state.AuctionObservation.ObservedAt = time.Now().Add(-time.Hour).UnixMilli()
			case "mailbox_missing":
				a.latest.NearbyMailboxes = nil
			case "auctioneer_missing":
				a.latest.NearbyNPCs = nil
			case "budget_missing":
				args = `{"item_id":3,"count":1}`
			case "budget_low":
				args = `{"item_id":3,"count":1,"max_spend_copper":9}`
			case "invalid_count":
				args = `{"item_id":3,"count":0,"max_spend_copper":10}`
			}
			previous := &task{Kind: "follow_player"}
			a.state.Task = previous
			a.applyTool(toolCall{Name: "buy_auction", Arguments: args})
			if a.state.Task != previous {
				t.Fatal("unsafe purchase displaced existing work")
			}
		})
	}
}

func TestAuctionMissingReceiptUncertainSpendOrMissingMailNeverRepurchases(t *testing.T) {
	for _, scenario := range []string{"receipt_missing", "uncertain", "mail_empty", "inventory_missing"} {
		t.Run(scenario, func(t *testing.T) {
			a, commands := auctionActor(t)
			a.applyTool(toolCall{Name: "buy_auction", Arguments: `{"item_id":3,"count":1,"max_spend_copper":10}`})
			buy := nextLoopCommand(t, commands)
			if scenario != "receipt_missing" {
				a.handleEvent(auctionReceipt(a, buy.OperationID, a.state.Task.CraftSupplies[0]))
			}
			if scenario == "uncertain" {
				a.handleTaskOperation(fishingResult(buy.OperationID, "rejected", "timeout"))
			} else {
				a.handleTaskOperation(fishingResult(buy.OperationID, "completed", "auction buyout submitted; collect won mail"))
			}
			if scenario == "mail_empty" || scenario == "inventory_missing" {
				collect := nextLoopCommand(t, commands)
				if scenario == "mail_empty" {
					a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "no delivered non-COD attachments or money remain"))
				} else {
					a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "bounded mailbox collection completed"))
					observation := nextLoopCommand(t, commands)
					a.handleEvent(craftingInventoryEvent(observation.RequestID, 3, 0))
				}
			}
			if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
				t.Fatal("unverified procurement continued")
			}
		})
	}
}

func TestAuctionOwnerChangeBlocksInFlightPurchaseAndInvalidatesListings(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "buy_auction", Arguments: `{"item_id":3,"count":1,"max_spend_copper":10}`})
	_ = nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "bot_online", OwnerToken: "replacement", Payload: json.RawMessage(`{}`)})
	a.handleEvent(event{Type: "snapshot", OwnerToken: "replacement", Payload: json.RawMessage(`{"schema_version":1}`)})
	if a.state.Task != nil || a.state.AuctionObservation != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("owner loss allowed uncertain purchase to replay")
	}
}

func TestAuctionInvalidPagesAndMissingObservationBlockInspection(t *testing.T) {
	for _, scenario := range []string{"stale", "wrong_item", "wrong_npc", "duplicate", "too_many", "backward_cursor", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			a, commands := auctionActor(t)
			a.applyTool(toolCall{Name: "inspect_auctions", Arguments: `{"npc_guid":"auctioneer","item_id":3}`})
			inspection := nextLoopCommand(t, commands)
			page := auctionObservation{OperationID: inspection.OperationID, NPCGUID: "auctioneer", ItemID: 3, NextCursor: 10, Offers: []auctionOffer{{AuctionID: 10, ItemID: 3, Count: 2, Buyout: 10, ItemGUID: "1000"}}}
			at := time.Now()
			switch scenario {
			case "stale":
				at = at.Add(-time.Hour)
			case "wrong_item":
				page.ItemID = 9
			case "wrong_npc":
				page.NPCGUID = "other"
			case "duplicate":
				page.Offers = append(page.Offers, page.Offers[0])
			case "too_many":
				page.Offers = make([]auctionOffer, 41)
			case "backward_cursor":
				page.More = true
				page.NextCursor = 0
			}
			previous := a.state.AuctionObservation
			if scenario != "missing" {
				a.handleEvent(event{Type: "auction_observation", OwnerToken: "owner", Timestamp: at.UnixMilli(), Payload: mustJSON(page)})
			}
			if a.state.AuctionObservation != previous {
				t.Fatal("invalid page entered planning state")
			}
			a.handleTaskOperation(fishingResult(inspection.OperationID, "completed", "bounded auction page observed"))
			if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
				t.Fatal("missing page reported successful inspection")
			}
		})
	}
}

func TestAuctionObservedBaselineSurvivesInventoryDisplayTruncation(t *testing.T) {
	a, commands := auctionActor(t)
	a.state.AuctionObservation.InventoryCount = 7
	a.applyTool(toolCall{Name: "buy_auction", Arguments: `{"item_id":3,"count":1,"max_spend_copper":10}`})
	_ = nextLoopCommand(t, commands)
	if a.state.Task.CraftInitialCount != 7 || a.state.Task.CraftSupplies[0].RequiredCount != 9 {
		t.Fatal("hidden existing stock was counted as newly purchased inventory")
	}
	var restored task
	if json.Unmarshal(mustJSON(a.state.Task), &restored) != nil || restored.CraftSupplies[0].ItemGUID != "1000" || restored.CraftSupplies[0].MailboxGUID != "mailbox" || restored.CraftInitialCount != 7 {
		t.Fatal("auction plan not persisted")
	}
	copy := cloneTask(a.state.Task)
	copy.CraftSupplies[0].AuctionID = 999
	if a.state.Task.CraftSupplies[0].AuctionID != 10 {
		t.Fatal("model task copy aliases reserved auctions")
	}
}
