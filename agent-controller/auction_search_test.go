package main

import (
	"encoding/json"
	"testing"
	"time"
)

func searchPage(a *actor, cmd command, itemID, cursor, next uint32, more bool, offers []auctionOffer) event {
	return event{Type: "auction_observation", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(auctionObservation{
		OperationID: cmd.OperationID, NPCGUID: "auctioneer", ItemID: itemID, MapID: a.latest.Bot.MapID, Cursor: cursor, NextCursor: next, More: more, Offers: offers,
	})}
}

func TestAuctionSearchPagesAndItemsNestWithoutModelCalls(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "search_auctions", Arguments: `{"npc_guid":"auctioneer","item_ids":[3,4]}`})
	first := nextLoopCommand(t, commands)
	id := a.state.Task.ID
	a.handleEvent(searchPage(a, first, 3, 0, 10, true, []auctionOffer{{AuctionID: 10, ItemID: 3, Count: 1, Buyout: 10, ItemGUID: "1000"}}))
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "bounded auction page observed"))
	second := nextLoopCommand(t, commands)
	if second.Operation != "inspect_auctions" || a.state.Task.ID != id || a.state.Task.AuctionSearchCursor != 10 || a.pendingDecisionReason == "task_completed" {
		t.Fatal("page continuation lost parent or asked model to pace it")
	}
	a.handleEvent(searchPage(a, second, 3, 10, 11, false, []auctionOffer{{AuctionID: 11, ItemID: 3, Count: 1, Buyout: 12, ItemGUID: "1001"}}))
	a.handleTaskOperation(fishingResult(second.OperationID, "completed", "bounded auction page observed"))
	third := nextLoopCommand(t, commands)
	if a.state.Task.AuctionSearchIndex != 1 || a.state.Task.AuctionSearchCursor != 0 {
		t.Fatal("did not start second material at first page")
	}
	a.handleEvent(searchPage(a, third, 4, 0, 20, false, []auctionOffer{{AuctionID: 20, ItemID: 4, Count: 2, Buyout: 8, ItemGUID: "2000"}}))
	a.handleTaskOperation(fishingResult(third.OperationID, "completed", "bounded auction page observed"))
	if a.state.Task != nil || len(a.cachedAuctionPage(3).Offers) != 2 || len(a.cachedAuctionPage(4).Offers) != 1 || a.pendingDecisionReason != "task_completed" {
		t.Fatal("multi-item evidence not retained")
	}
	supplies, err := a.craftingMaterialSupplies([]craftReagent{{ItemID: 3, Count: 2}, {ItemID: 4, Count: 2}}, map[uint32]uint32{}, 30, true)
	if err != nil || len(supplies) != 3 {
		t.Fatalf("multi-material auction planning failed: %v %+v", err, supplies)
	}
	spent := uint32(0)
	for _, supply := range supplies {
		spent += supply.Budget
	}
	if spent != 30 {
		t.Fatal("material plans did not share one total budget")
	}
}

func TestCraftingAutomaticallySearchesUnknownAuctionSupply(t *testing.T) {
	a, commands := auctionActor(t)
	a.state.AuctionObservation = nil
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"max_spend_copper":10}`})
	inspection := nextLoopCommand(t, commands)
	if inspection.Operation != "inspect_auctions" || a.state.Task.Kind != "crafting" || !a.state.Task.CraftNeedsSupplies {
		t.Fatal("unknown auction supply was not realized as a search child")
	}
	a.handleEvent(searchPage(a, inspection, 3, 0, 10, false, []auctionOffer{{AuctionID: 10, ItemID: 3, Count: 1, Buyout: 10, ItemGUID: "1000"}}))
	a.handleTaskOperation(fishingResult(inspection.OperationID, "completed", "bounded auction page observed"))
	observation := nextLoopCommand(t, commands)
	if observation.Operation != "snapshot" || a.state.Task.Phase != "observe" {
		t.Fatal("searched acquisition skipped fresh inventory")
	}
	inventory := craftingInventoryEvent(observation.RequestID, 3, 0)
	inventory.Payload = json.RawMessage(`{"schema_version":1,"crafting_inventory":[{"item_id":3,"count":0}],"nearby_npcs":[{"guid":"auctioneer","npc_flags":2097152}],"nearby_mailboxes":[{"guid":"mailbox"}]}`)
	a.handleEvent(inventory)
	buy := nextLoopCommand(t, commands)
	if buy.Operation != "buy_auction" || a.state.Task.CraftNeedsSupplies {
		t.Fatal("correlated search did not release budgeted procurement")
	}
}

func TestAuctionSearchNeverAdvancesFromMissingOrDuplicatePage(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "search_auctions", Arguments: `{"npc_guid":"auctioneer","item_ids":[3]}`})
	first := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "bounded auction page observed"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("missing page advanced search")
	}
	a, commands = auctionActor(t)
	a.applyTool(toolCall{Name: "search_auctions", Arguments: `{"npc_guid":"auctioneer","item_ids":[3]}`})
	first = nextLoopCommand(t, commands)
	page := searchPage(a, first, 3, 0, 10, true, nil)
	a.handleEvent(page)
	a.handleEvent(page)
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "bounded auction page observed"))
	_ = nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "bounded auction page observed"))
	if a.state.Task.AuctionSearchPages != 1 {
		t.Fatal("duplicate terminal event consumed page budget")
	}
}

func TestAuctionCatalogIsBoundedAndDoesNotRefreshOldOffers(t *testing.T) {
	a, _ := auctionActor(t)
	now := time.Now()
	old := now.Add(-auctionObservationAge - time.Second).UnixMilli()
	a.cacheAuctionPage(auctionObservation{NPCGUID: "auctioneer", ItemID: 3, ObservedAt: now.UnixMilli(), Offers: []auctionOffer{{AuctionID: 10, ItemID: 3, Count: 1, Buyout: 1, ItemGUID: "1000", ObservedAt: old}}})
	a.cacheAuctionPage(auctionObservation{NPCGUID: "auctioneer", ItemID: 3, Cursor: 10, ObservedAt: now.UnixMilli(), Offers: []auctionOffer{{AuctionID: 11, ItemID: 3, Count: 1, Buyout: 1, ItemGUID: "1001", ObservedAt: now.UnixMilli()}}})
	if len(a.cachedAuctionPage(3).Offers) != 1 || a.cachedAuctionPage(3).Offers[0].AuctionID != 11 {
		t.Fatal("new page refreshed old offer evidence")
	}
	for itemID := uint32(1); itemID <= maxAuctionSearchItems+1; itemID++ {
		a.cacheAuctionPage(auctionObservation{NPCGUID: "auctioneer", ItemID: itemID, ObservedAt: now.UnixMilli()})
	}
	if len(a.state.AuctionCatalog) != maxAuctionSearchItems {
		t.Fatal("unbounded material catalog")
	}
}

func TestAuctionStackPlannerAvoidsGreedyBudgetTrap(t *testing.T) {
	a, _ := auctionActor(t)
	a.state.AuctionObservation.Offers = []auctionOffer{{AuctionID: 10, ItemID: 3, Count: 2, Buyout: 6, ItemGUID: "1000"}, {AuctionID: 11, ItemID: 3, Count: 3, Buyout: 10, ItemGUID: "1001"}}
	supplies, err := a.auctionSupplies(craftReagent{ItemID: 3, Count: 3}, 0, 10)
	if err != nil || len(supplies) != 1 || supplies[0].AuctionID != 11 {
		t.Fatal("feasible smaller total budget rejected by unit-price greed")
	}
}

func TestAuctionSearchPageLimitAndStateClone(t *testing.T) {
	a, commands := auctionActor(t)
	a.applyTool(toolCall{Name: "search_auctions", Arguments: `{"npc_guid":"auctioneer","item_ids":[3]}`})
	first := nextLoopCommand(t, commands)
	a.state.Task.AuctionSearchPages = maxAuctionSearchPages - 1
	a.handleEvent(searchPage(a, first, 3, 0, 10, true, nil))
	copy := cloneTask(a.state.Task)
	copy.AuctionSearchItems[0] = 999
	if a.state.Task.AuctionSearchItems[0] != 3 {
		t.Fatal("decision clone aliases search state")
	}
	var restored task
	if json.Unmarshal(mustJSON(a.state.Task), &restored) != nil || restored.AuctionSearchReceipt != first.OperationID {
		t.Fatal("search progress not persisted")
	}
	a.handleTaskOperation(fishingResult(first.OperationID, "completed", "bounded auction page observed"))
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("search exceeded page budget")
	}
}
