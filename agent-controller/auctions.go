package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const npcFlagAuctioneer = 0x200000
const auctionObservationAge = 2 * time.Minute

type auctionOffer struct {
	ObservedAt int64  `json:"observed_at,omitempty"`
	MinimumBid uint32 `json:"minimum_bid_copper,omitempty"`
	ExpiresAt  int64  `json:"expires_at,omitempty"`
	AuctionID  uint32 `json:"auction_id"`
	ItemID     uint32 `json:"item_id"`
	Count      uint32 `json:"count"`
	Buyout     uint32 `json:"buyout_copper"`
	ItemGUID   string `json:"item_guid"`
}

type auctionObservation struct {
	InventoryCount    uint32         `json:"inventory_count"`
	OperationID       string         `json:"operation_id"`
	NPCGUID           string         `json:"npc_guid"`
	MapID             uint32         `json:"map_id"`
	ItemID            uint32         `json:"item_id"`
	Cursor            uint32         `json:"cursor"`
	NextCursor        uint32         `json:"next_cursor"`
	More              bool           `json:"more"`
	PurchasedItemGUID string         `json:"purchased_item_guid"`
	Offers            []auctionOffer `json:"offers"`
	ObservedAt        int64          `json:"observed_at"`
	CacheTruncated    bool           `json:"cache_truncated,omitempty"`
	AuctionID         uint32         `json:"auction_id,omitempty"`
	BidCopper         uint32         `json:"bid_copper,omitempty"`
}

func (a *actor) observeAuctions(incoming event) {
	var bidReceipt auctionObservation
	if json.Unmarshal(incoming.Payload, &bidReceipt) == nil && a.observeAuctionBidReceipt(incoming, bidReceipt) {
		return
	}
	current := a.state.Task
	if current == nil || current.OperationID == "" {
		return
	}
	var observation auctionObservation
	if json.Unmarshal(incoming.Payload, &observation) != nil || observation.OperationID != current.OperationID ||
		observation.ItemID == 0 || observation.MapID != current.MapID || incoming.Timestamp <= 0 ||
		incoming.Timestamp > time.Now().Add(time.Minute).UnixMilli() || time.Since(time.UnixMilli(incoming.Timestamp)) > auctionObservationAge {
		return
	}
	if itemID, cursor, err := auctionSearchArguments(current); err == nil {
		if observation.NPCGUID != current.TargetGUID ||
			observation.ItemID != itemID || observation.Cursor != cursor || len(observation.Offers) > 40 ||
			(observation.More && observation.NextCursor <= observation.Cursor) {
			return
		}
		seen := make(map[uint32]bool)
		for _, offer := range observation.Offers {
			if offer.ItemID != observation.ItemID || offer.AuctionID <= observation.Cursor || offer.AuctionID > observation.NextCursor ||
				offer.Count == 0 || (offer.Buyout == 0 && offer.MinimumBid == 0) || offer.ItemGUID == "" || offer.ItemGUID == "0" || seen[offer.AuctionID] {
				return
			}
			seen[offer.AuctionID] = true
		}
		observation.ObservedAt = incoming.Timestamp
		if observation.ObservedAt > time.Now().UnixMilli() {
			observation.ObservedAt = time.Now().UnixMilli()
		}
		for index := range observation.Offers {
			observation.Offers[index].ObservedAt = observation.ObservedAt
		}
		a.cacheAuctionPage(observation)
		if current.Phase == "auction_search" {
			current.AuctionSearchReceipt = current.OperationID
			current.AuctionSearchCursor = observation.NextCursor
			current.AuctionSearchMore = observation.More
		}
		a.state.AuctionObservation = &observation
		a.latest.Auctions = &observation
		a.persist()
		return
	}
	if (current.Kind == "crafting" || current.Kind == "auction_purchase") && current.Phase == "auction_buy" && current.CraftSupplyIndex < uint32(len(current.CraftSupplies)) {
		supply := current.CraftSupplies[current.CraftSupplyIndex]
		if supply.AuctionID != 0 && observation.NPCGUID == supply.NPCGUID && observation.ItemID == supply.ItemID && observation.PurchasedItemGUID == supply.ItemGUID {
			current.CraftAuctionReceipt = observation.PurchasedItemGUID
			a.persist()
		}
	}
}

func (a *actor) auctionSupplies(material craftReagent, stock uint32, budget uint32) ([]craftSupply, error) {
	page := a.cachedAuctionPage(material.ItemID)
	if page == nil || page.ItemID != material.ItemID || page.MapID != a.latest.Bot.MapID || page.ObservedAt > time.Now().UnixMilli() || time.Since(time.UnixMilli(page.ObservedAt)) > auctionObservationAge {
		return nil, fmt.Errorf("fresh observed auction offers required")
	}
	eligible := false
	for _, npc := range a.latest.NearbyNPCs {
		eligible = eligible || (npc.GUID == page.NPCGUID && npc.NPCFlags&npcFlagAuctioneer != 0)
	}
	mailbox := ""
	for _, box := range a.latest.NearbyMailboxes {
		if box.GUID != "" && (mailbox == "" || box.GUID < mailbox) {
			mailbox = box.GUID
		}
	}
	if !eligible || mailbox == "" {
		return nil, fmt.Errorf("observed auctioneer and mailbox required")
	}
	if material.Count == 0 || material.Count > maxCraftingCasts*40 || len(page.Offers) > maxAuctionCachedOffers {
		return nil, fmt.Errorf("auction material goal exceeds bounded planning limit")
	}
	offers := make([]auctionOffer, 0, len(page.Offers))
	seen := make(map[uint32]bool)
	for _, offer := range page.Offers {
		at := offer.ObservedAt
		if at == 0 {
			at = page.ObservedAt
		}
		if seen[offer.AuctionID] || offer.AuctionID == 0 || offer.Count == 0 || offer.Count > 40 || offer.ItemID != material.ItemID || offer.Buyout == 0 || offer.Buyout > budget || offer.ItemGUID == "" || offer.ItemGUID == "0" || at > time.Now().UnixMilli() || time.Since(time.UnixMilli(at)) > auctionObservationAge {
			continue
		}
		seen[offer.AuctionID] = true
		offers = append(offers, offer)
	}
	sort.Slice(offers, func(i, j int) bool { return offers[i].AuctionID < offers[j].AuctionID })
	// Bounded 0/1 stack selection minimizes total spend, not unit price. Greedy
	// unit pricing can reject a feasible smaller stack after reserving a large one.
	costs := make([]uint64, int(material.Count)+1)
	paths := make([][]int, len(costs))
	for index := 1; index < len(costs); index++ {
		costs[index] = ^uint64(0)
	}
	for index, offer := range offers {
		for quantity := int(material.Count) - 1; quantity >= 0; quantity-- {
			if costs[quantity] == ^uint64(0) {
				continue
			}
			next := quantity + int(offer.Count)
			if next > int(material.Count) {
				next = int(material.Count)
			}
			cost := costs[quantity] + uint64(offer.Buyout)
			if cost <= uint64(budget) && (cost < costs[next] || (cost == costs[next] && len(paths[quantity])+1 < len(paths[next]))) {
				costs[next] = cost
				paths[next] = append(append([]int(nil), paths[quantity]...), index)
			}
		}
	}
	if costs[material.Count] == ^uint64(0) {
		return nil, fmt.Errorf("observed auction offers cannot fill material goal within budget")
	}
	var supplies []craftSupply
	acquired := uint64(0)
	for _, index := range paths[material.Count] {
		offer := offers[index]
		acquired += uint64(offer.Count)
		required := uint64(stock) + acquired
		if required > uint64(^uint32(0)) {
			return nil, fmt.Errorf("auction inventory goal overflow")
		}
		supplies = append(supplies, craftSupply{NPCGUID: page.NPCGUID, ItemID: material.ItemID, Bundles: offer.Count, Budget: offer.Buyout, AuctionID: offer.AuctionID, ItemGUID: offer.ItemGUID, MailboxGUID: mailbox, RequiredCount: uint32(required)})
	}
	if acquired < uint64(material.Count) || len(supplies) > maxCraftingCasts {
		return nil, fmt.Errorf("observed auction page cannot fill material goal within budget")
	}
	return supplies, nil
}

func (a *actor) inspectAuctions(args map[string]json.RawMessage) {
	var npcGUID string
	var itemID, cursor uint32
	if json.Unmarshal(args["npc_guid"], &npcGUID) != nil || npcGUID == "" || json.Unmarshal(args["item_id"], &itemID) != nil || itemID == 0 {
		a.rejectTool("inspect_auctions", "observed npc_guid and item_id required")
		return
	}
	if raw, provided := args["cursor"]; provided && (string(raw) == "null" || json.Unmarshal(raw, &cursor) != nil) {
		a.rejectTool("inspect_auctions", "cursor must be a nonnegative integer")
		return
	}
	found := false
	for _, npc := range a.latest.NearbyNPCs {
		found = found || (npc.GUID == npcGUID && npc.NPCFlags&npcFlagAuctioneer != 0)
	}
	if !a.hasSnapshot || !found || !a.bridgeActive {
		a.rejectTool("inspect_auctions", "observed auctioneer and active native bridge required")
		return
	}
	if current := a.state.Task; current != nil && (current.Kind == "crafting" || current.Kind == "auction_purchase") {
		a.rejectTool("inspect_auctions", "finish or explicitly cancel procurement before browsing")
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "npc_service", ServiceOperation: "inspect_auctions", TargetGUID: npcGUID, MapID: a.latest.Bot.MapID,
		ServiceArguments: mustJSON(map[string]any{"target_guid": npcGUID, "item_id": itemID, "cursor": cursor, "count": 1}), Phase: "service", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) buyAuction(args map[string]json.RawMessage) {
	var itemID, count, budget uint32
	if json.Unmarshal(args["item_id"], &itemID) != nil || itemID == 0 || json.Unmarshal(args["count"], &count) != nil || count == 0 || count > 40 || string(args["max_spend_copper"]) == "null" || json.Unmarshal(args["max_spend_copper"], &budget) != nil {
		a.rejectTool("buy_auction", "item_id, count 1..40 and explicit total copper budget required")
		return
	}
	if current := a.state.Task; current != nil && (current.Kind == "crafting" || current.Kind == "auction_purchase") {
		a.rejectTool("buy_auction", "procurement already active; await or explicitly cancel it")
		return
	}
	if !a.hasSnapshot || !a.bridgeActive {
		a.rejectTool("buy_auction", "fresh state and active native bridge required")
		return
	}
	stock := craftingStock(a.latest)
	if page := a.cachedAuctionPage(itemID); page != nil {
		if _, observed := stock[itemID]; !observed {
			stock[itemID] = page.InventoryCount
		}
	}
	supplies, err := a.auctionSupplies(craftReagent{ItemID: itemID, Count: count}, stock[itemID], budget)
	if err != nil {
		a.rejectTool("buy_auction", err.Error())
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "auction_purchase", MapID: a.latest.Bot.MapID, CraftItemID: itemID, CraftInitialCount: stock[itemID], GoalCount: count,
		CraftSupplies: supplies, CraftObservedStock: craftingObservedStock(a.latest), Phase: "select", LastProgressUTC: time.Now().UTC()}
	a.state.Task.CraftObservedStock = append(a.state.Task.CraftObservedStock, craftReagent{ItemID: itemID, Count: stock[itemID]})
	a.persist()
	a.advanceTask()
}
