package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const (
	maxAuctionSearchItems  = 8
	maxAuctionSearchPages  = 16
	maxAuctionCachedOffers = 160
)

func (a *actor) cachedAuctionPage(itemID uint32) *auctionObservation {
	if page := a.state.AuctionCatalog[itemID]; page != nil {
		return page
	}
	if page := a.state.AuctionObservation; page != nil && page.ItemID == itemID {
		return page
	}
	return nil
}

func (a *actor) cacheAuctionPage(page auctionObservation) {
	now := time.Now()
	if a.state.AuctionCatalog == nil {
		a.state.AuctionCatalog = make(map[uint32]*auctionObservation)
	}
	for itemID, cached := range a.state.AuctionCatalog {
		if cached == nil || time.Since(time.UnixMilli(cached.ObservedAt)) > auctionObservationAge || cached.MapID != page.MapID || cached.NPCGUID != page.NPCGUID {
			delete(a.state.AuctionCatalog, itemID)
		}
	}
	previous := a.state.AuctionCatalog[page.ItemID]
	if previous != nil && page.Cursor != 0 {
		seen := make(map[uint32]bool)
		for _, offer := range page.Offers {
			seen[offer.AuctionID] = true
		}
		for _, offer := range previous.Offers {
			if len(page.Offers) >= maxAuctionCachedOffers {
				page.CacheTruncated = true
				break
			}
			if !seen[offer.AuctionID] && offer.ObservedAt <= now.UnixMilli() && time.Since(time.UnixMilli(offer.ObservedAt)) <= auctionObservationAge {
				page.Offers = append(page.Offers, offer)
			}
		}
		page.CacheTruncated = page.CacheTruncated || previous.CacheTruncated
	}
	if _, exists := a.state.AuctionCatalog[page.ItemID]; !exists && len(a.state.AuctionCatalog) >= maxAuctionSearchItems {
		oldestID := uint32(0)
		oldest := int64(0)
		for itemID, cached := range a.state.AuctionCatalog {
			if oldestID == 0 || cached.ObservedAt < oldest || (cached.ObservedAt == oldest && itemID < oldestID) {
				oldestID, oldest = itemID, cached.ObservedAt
			}
		}
		delete(a.state.AuctionCatalog, oldestID)
	}
	sort.Slice(page.Offers, func(i, j int) bool { return page.Offers[i].AuctionID < page.Offers[j].AuctionID })
	a.state.AuctionCatalog[page.ItemID] = &page
	a.latest.CachedAuctionItems = a.auctionCatalogSummary(page.MapID)
}

type auctionCatalogSummary struct {
	ItemID  uint32 `json:"item_id"`
	Offers  int    `json:"offers"`
	Partial bool   `json:"partial"`
}

func (a *actor) auctionCatalogSummary(mapID uint32) []auctionCatalogSummary {
	var summaries []auctionCatalogSummary
	for itemID, page := range a.state.AuctionCatalog {
		if page == nil || page.MapID != mapID || page.ObservedAt > time.Now().UnixMilli() || time.Since(time.UnixMilli(page.ObservedAt)) > auctionObservationAge {
			continue
		}
		count := 0
		for _, offer := range page.Offers {
			if offer.ObservedAt <= time.Now().UnixMilli() && time.Since(time.UnixMilli(offer.ObservedAt)) <= auctionObservationAge {
				count++
			}
		}
		summaries = append(summaries, auctionCatalogSummary{ItemID: itemID, Offers: count, Partial: page.More || page.CacheTruncated})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].ItemID < summaries[j].ItemID })
	if len(summaries) > maxAuctionSearchItems {
		summaries = summaries[:maxAuctionSearchItems]
	}
	return summaries
}

func (a *actor) observedAuctioneer() string {
	npcGUID := ""
	for _, npc := range a.latest.NearbyNPCs {
		if npc.NPCFlags&npcFlagAuctioneer != 0 && npc.GUID != "" && (npcGUID == "" || npc.GUID < npcGUID) {
			npcGUID = npc.GUID
		}
	}
	return npcGUID
}

func (a *actor) searchAuctions(args map[string]json.RawMessage) {
	var npcGUID string
	var items []uint32
	if json.Unmarshal(args["npc_guid"], &npcGUID) != nil || npcGUID == "" || json.Unmarshal(args["item_ids"], &items) != nil || len(items) == 0 || len(items) > maxAuctionSearchItems {
		a.rejectTool("search_auctions", "observed npc_guid and 1..8 distinct item_ids required")
		return
	}
	seen := make(map[uint32]bool)
	for _, itemID := range items {
		if itemID == 0 || seen[itemID] {
			a.rejectTool("search_auctions", "item_ids must be positive and distinct")
			return
		}
		seen[itemID] = true
	}
	found := false
	for _, npc := range a.latest.NearbyNPCs {
		found = found || (npc.GUID == npcGUID && npc.NPCFlags&npcFlagAuctioneer != 0)
	}
	if !a.hasSnapshot || !a.bridgeActive || !found {
		a.rejectTool("search_auctions", "observed auctioneer and active native bridge required")
		return
	}
	if current := a.state.Task; current != nil && (current.Kind == "crafting" || current.Kind == "auction_purchase") {
		a.rejectTool("search_auctions", "procurement already active")
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "auction_search", MapID: a.latest.Bot.MapID, TargetGUID: npcGUID, AuctionSearchItems: items, Phase: "auction_search", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) advanceAuctionSearch(current *task) {
	if a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) || current.Retries != 0 {
		a.finishTask("blocked", "auction search interrupted; start a fresh bounded search")
		return
	}
	if a.latest.Bot.InCombat {
		return
	}
	if len(current.AuctionSearchItems) == 0 || len(current.AuctionSearchItems) > maxAuctionSearchItems || current.AuctionSearchIndex >= uint32(len(current.AuctionSearchItems)) || current.AuctionSearchPages >= maxAuctionSearchPages || current.AuctionSearchItems[current.AuctionSearchIndex] == 0 || current.TargetGUID == "" {
		a.finishTask("blocked", "invalid or exhausted persisted auction search")
		return
	}
	current.Phase = "auction_search"
	current.AuctionSearchReceipt = ""
	current.OperationID = a.sendPrimitive("inspect_auctions", map[string]any{"target_guid": current.TargetGUID, "item_id": current.AuctionSearchItems[current.AuctionSearchIndex], "cursor": current.AuctionSearchCursor, "count": 1})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleAuctionSearch(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if status != "completed" || reason != "bounded auction page observed" || current.AuctionSearchReceipt != current.OperationID {
		a.finishTask("blocked", "correlated auction search page unavailable")
		return
	}
	current.OperationID = ""
	current.AuctionSearchPages++
	if current.AuctionSearchMore {
		if current.AuctionSearchPages >= maxAuctionSearchPages {
			a.finishTask("blocked", "auction search page limit reached; observed offers remain partial")
			return
		}
	} else {
		current.AuctionSearchIndex++
		current.AuctionSearchCursor, current.AuctionSearchPages = 0, 0
	}
	if current.AuctionSearchIndex >= uint32(len(current.AuctionSearchItems)) {
		if current.Kind == "crafting" {
			a.requestCraftingObservation(current)
			return
		}
		a.finishTask("completed", "bounded multi-item auction search completed")
		return
	}
	a.persist()
	a.advanceAuctionSearch(current)
}

func auctionSearchArguments(current *task) (uint32, uint32, error) {
	if current.Kind == "npc_service" && current.ServiceOperation == "inspect_auctions" {
		var args struct {
			ItemID uint32 `json:"item_id"`
			Cursor uint32 `json:"cursor"`
		}
		if json.Unmarshal(current.ServiceArguments, &args) != nil {
			return 0, 0, fmt.Errorf("invalid search arguments")
		}
		return args.ItemID, args.Cursor, nil
	}
	if (current.Kind == "auction_search" || current.Kind == "crafting") && current.Phase == "auction_search" && current.AuctionSearchIndex < uint32(len(current.AuctionSearchItems)) {
		return current.AuctionSearchItems[current.AuctionSearchIndex], current.AuctionSearchCursor, nil
	}
	return 0, 0, fmt.Errorf("no matching auction search")
}
