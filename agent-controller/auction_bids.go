package main

import (
	"encoding/json"
	"sort"
	"time"
)

const maxPendingAuctionBids = 8

type auctionBid struct {
	AuctionID      uint32    `json:"auction_id"`
	ItemID         uint32    `json:"item_id"`
	ItemGUID       string    `json:"item_guid"`
	Count          uint32    `json:"count"`
	Price          uint32    `json:"bid_copper"`
	NPCGUID        string    `json:"npc_guid"`
	MapID          uint32    `json:"map_id"`
	OperationID    string    `json:"operation_id"`
	Status         string    `json:"status"`
	EscrowVerified bool      `json:"escrow_verified"`
	NextCheckUTC   time.Time `json:"next_check_utc"`
	StartedAtUTC   time.Time `json:"started_at_utc"`
}

func (a *actor) bidAuction(args map[string]json.RawMessage) {
	var auctionID, itemID, price, budget uint32
	if json.Unmarshal(args["auction_id"], &auctionID) != nil || auctionID == 0 || json.Unmarshal(args["item_id"], &itemID) != nil || itemID == 0 ||
		json.Unmarshal(args["bid_copper"], &price) != nil || price == 0 || json.Unmarshal(args["max_spend_copper"], &budget) != nil || price > budget {
		a.rejectTool("bid_auction", "observed auction_id/item_id, positive bid_copper and explicit sufficient budget required")
		return
	}
	if !a.hasSnapshot || !a.bridgeActive || len(a.state.PendingAuctionBids) >= maxPendingAuctionBids || a.state.PendingAuctionBids[auctionID] != nil {
		a.rejectTool("bid_auction", "native state unavailable, pending bid already exists, or escrow journal is full")
		return
	}
	if current := a.state.Task; current != nil && (current.Kind == "crafting" || current.Kind == "auction_purchase" || current.Kind == "auction_bid") {
		a.rejectTool("bid_auction", "finish or cancel resource procurement first")
		return
	}
	page := a.cachedAuctionPage(itemID)
	if page == nil || page.MapID != a.latest.Bot.MapID || page.ObservedAt > time.Now().UnixMilli() || time.Since(time.UnixMilli(page.ObservedAt)) > auctionObservationAge {
		a.rejectTool("bid_auction", "fresh observed auction page required")
		return
	}
	eligible := false
	for _, npc := range a.latest.NearbyNPCs {
		eligible = eligible || (npc.GUID == page.NPCGUID && npc.NPCFlags&npcFlagAuctioneer != 0)
	}
	var selected *auctionOffer
	for index := range page.Offers {
		offer := &page.Offers[index]
		at := offer.ObservedAt
		if at == 0 {
			at = page.ObservedAt
		}
		if offer.AuctionID == auctionID && offer.ItemID == itemID && offer.Count > 0 && offer.Count <= 40 && offer.ItemGUID != "" && offer.ItemGUID != "0" && offer.MinimumBid > 0 &&
			price >= offer.MinimumBid && (offer.Buyout == 0 || price < offer.Buyout) && offer.ExpiresAt > time.Now().Unix() && at <= time.Now().UnixMilli() && time.Since(time.UnixMilli(at)) <= auctionObservationAge {
			selected = offer
			break
		}
	}
	if !eligible || selected == nil {
		a.rejectTool("bid_auction", "listing, minimum bid, expiry or non-buyout price no longer valid")
		return
	}
	if a.state.PendingAuctionBids == nil {
		a.state.PendingAuctionBids = make(map[uint32]*auctionBid)
	}
	a.cancelTaskPrimitive()
	a.state.PendingAuctionBids[auctionID] = &auctionBid{AuctionID: auctionID, ItemID: itemID, ItemGUID: selected.ItemGUID, Count: selected.Count, Price: price, NPCGUID: page.NPCGUID, MapID: page.MapID, Status: "submitting", NextCheckUTC: time.Now().Add(time.Minute), StartedAtUTC: time.Now().UTC()}
	a.state.Task = &task{ID: newID(), Kind: "auction_bid", MapID: page.MapID, BidAuctionID: auctionID, Phase: "bid", LastProgressUTC: time.Now().UTC()}
	a.latest.PendingAuctionBids = a.pendingBidSummary()
	if !a.persist() {
		a.state.Task = nil
		delete(a.state.PendingAuctionBids, auctionID)
		a.latest.PendingAuctionBids = a.pendingBidSummary()
		a.rejectTool("bid_auction", "escrow journal could not be persisted; no bid dispatched")
		return
	}
	a.advanceTask()
}

func (a *actor) advanceAuctionBid(current *task) {
	bid := a.state.PendingAuctionBids[current.BidAuctionID]
	if bid == nil || bid.OperationID != "" || current.Retries != 0 || a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "bid submission uncertain or interrupted; inspect escrow and mail rather than rebidding")
		return
	}
	if a.latest.Bot.InCombat {
		return
	}
	current.OperationID = newID()
	bid.OperationID = current.OperationID
	current.LastProgressUTC = time.Now().UTC()
	// Persist the no-replay marker before publishing an irreversible escrow command.
	if !a.persist() {
		bid.Status = "uncertain"
		a.finishTask("blocked", "bid dispatch journal unavailable; no bid dispatched")
		return
	}
	a.sendCommandWithID(newID(), current.OperationID, "bid_auction", map[string]any{"target_guid": bid.NPCGUID, "auction_id": bid.AuctionID, "item_id": bid.ItemID, "item_guid": bid.ItemGUID, "count": bid.Count, "buyout_copper": bid.Price, "max_spend_copper": bid.Price})
}

func (a *actor) observeAuctionBidReceipt(incoming event, observation auctionObservation) bool {
	bid := a.state.PendingAuctionBids[observation.AuctionID]
	if bid == nil || bid.OperationID == "" || bid.OperationID != observation.OperationID {
		return false
	}
	if observation.ItemID == bid.ItemID && observation.PurchasedItemGUID == bid.ItemGUID && observation.BidCopper == bid.Price && observation.NPCGUID == bid.NPCGUID && observation.MapID == bid.MapID &&
		incoming.Timestamp > 0 && incoming.Timestamp <= time.Now().Add(time.Minute).UnixMilli() && time.Since(time.UnixMilli(incoming.Timestamp)) <= auctionObservationAge {
		if bid.Status == "submitting" || bid.Status == "uncertain" {
			bid.Status = "pending"
		}
		bid.EscrowVerified = true
		a.latest.PendingAuctionBids = a.pendingBidSummary()
		a.persist()
	}
	return true
}

func (a *actor) handleAuctionBid(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	bid := a.state.PendingAuctionBids[current.BidAuctionID]
	if status != "completed" || reason != "auction bid verified; outcome pending" || bid == nil || !bid.EscrowVerified {
		if bid != nil && !bid.EscrowVerified {
			bid.Status = "uncertain"
		}
		a.finishTask("blocked", "auction bid receipt unverified; escrow journal retained without rebidding")
		return
	}
	a.finishTask("completed", "auction bid placed; won item or refund remains pending in escrow journal")
}

func (a *actor) pendingBidSummary() []auctionBid {
	var bids []auctionBid
	for _, bid := range a.state.PendingAuctionBids {
		if bid != nil {
			bids = append(bids, *bid)
		}
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].AuctionID < bids[j].AuctionID })
	return bids
}

func (a *actor) observeAuctionBidNotification(incoming event) {
	var notification struct {
		AuctionID uint32 `json:"auction_id"`
		ItemID    uint32 `json:"item_id"`
		Price     uint32 `json:"bid_copper"`
		Own       bool   `json:"own_bidder"`
	}
	if json.Unmarshal(incoming.Payload, &notification) != nil {
		return
	}
	bid := a.state.PendingAuctionBids[notification.AuctionID]
	if bid == nil || bid.ItemID != notification.ItemID || incoming.Timestamp <= 0 || time.UnixMilli(incoming.Timestamp).Before(bid.StartedAtUTC.Add(-time.Second)) || incoming.Timestamp > time.Now().Add(time.Minute).UnixMilli() || time.Since(time.UnixMilli(incoming.Timestamp)) > auctionObservationAge {
		return
	}
	status := ""
	if notification.Own && notification.Price == 0 {
		status = "won_mail_pending"
	}
	if !notification.Own && notification.Price > bid.Price {
		status = "outbid_refund_pending"
	}
	if status == "" || status == bid.Status {
		return
	}
	bid.Status = status
	a.latest.PendingAuctionBids = a.pendingBidSummary()
	a.rememberEvent(incoming)
	a.persist()
	a.decide("auction_bid_status")
}

func (a *actor) collectAuctionProceeds(args map[string]json.RawMessage) {
	var auctionID uint32
	var mailbox string
	if json.Unmarshal(args["auction_id"], &auctionID) != nil || json.Unmarshal(args["mailbox_guid"], &mailbox) != nil || !a.startAuctionProceeds(auctionID, mailbox, false) {
		a.rejectTool("collect_auction_proceeds", "pending auction_id and observed mailbox_guid required; procurement must be idle")
	}
}

func (a *actor) startAuctionProceeds(auctionID uint32, mailbox string, passive bool) bool {
	bid := a.state.PendingAuctionBids[auctionID]
	if bid == nil || bid.Status == "won" || bid.Status == "refunded" || bid.Status == "receipt_uncertain" || !a.hasSnapshot || !a.bridgeActive || a.state.Task != nil {
		return false
	}
	found := false
	for _, box := range a.latest.NearbyMailboxes {
		found = found || (box.GUID == mailbox && mailbox != "")
	}
	if !found {
		return false
	}
	bid.NextCheckUTC = time.Now().Add(time.Minute)
	if passive {
		a.revision++
	}
	a.state.Task = &task{ID: newID(), Kind: "auction_proceeds", MapID: a.latest.Bot.MapID, TargetGUID: mailbox, BidAuctionID: auctionID, CraftItemID: bid.ItemID, AuctionProceedsPassive: passive, LastProgressUTC: time.Now().UTC()}
	a.requestCraftingObservation(a.state.Task)
	return true
}

func (a *actor) startPassiveAuctionProceeds() bool {
	if a.state.Task != nil || len(a.latest.NearbyMailboxes) == 0 || a.latest.Bot.InCombat || !a.online {
		return false
	}
	for _, bid := range a.pendingBidSummary() {
		if time.Now().Before(bid.NextCheckUTC) {
			continue
		}
		if a.startAuctionProceeds(bid.AuctionID, a.latest.NearbyMailboxes[0].GUID, true) {
			return true
		}
	}
	return false
}

func (a *actor) advanceAuctionProceeds(current *task) {
	if current.Retries != 0 || a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "auction proceeds collection uncertain; inspect mail and inventory")
		return
	}
	if current.Phase == "observe" {
		if time.Since(current.LastProgressUTC) >= 10*time.Second {
			a.requestCraftingObservation(current)
		}
		return
	}
	if a.latest.Bot.InCombat {
		return
	}
	bid := a.state.PendingAuctionBids[current.BidAuctionID]
	if bid == nil {
		a.finishTask("blocked", "pending auction record missing")
		return
	}
	stock := make(map[uint32]uint32)
	for _, item := range current.CraftObservedStock {
		stock[item.ItemID] = item.Count
	}
	if current.AuctionProceedsOutcome != 0 {
		if current.AuctionProceedsOutcome == 1 && uint64(stock[bid.ItemID]) < uint64(current.CraftInitialCount)+uint64(bid.Count) {
			bid.Status = "receipt_uncertain"
			a.finishTask("blocked", "auction-won inventory postcondition missing")
			return
		}
		if current.AuctionProceedsOutcome == 1 {
			bid.Status = "won"
		} else {
			bid.Status = "refunded"
		}
		delete(a.state.PendingAuctionBids, bid.AuctionID)
		a.latest.PendingAuctionBids = a.pendingBidSummary()
		a.finishTask("completed", "auction proceeds verified: "+bid.Status)
		return
	}
	if !current.AuctionProceedsBaselineReady {
		current.CraftInitialCount = stock[bid.ItemID]
		current.AuctionProceedsBaselineReady = true
	}
	current.Phase = "collect_proceeds"
	current.OperationID = a.sendPrimitive("collect_mail", map[string]any{"target_guid": current.TargetGUID, "count": 1, "collect_item_guid": bid.ItemGUID, "collect_auction_id": bid.AuctionID, "item_id": bid.ItemID, "item_count": bid.Count, "bid_copper": bid.Price})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) observeAuctionProceeds(incoming event) {
	current := a.state.Task
	if current == nil || current.Kind != "auction_proceeds" || current.Phase != "collect_proceeds" {
		return
	}
	var result struct {
		OperationID string `json:"operation_id"`
		AuctionID   uint32 `json:"auction_id"`
		ItemID      uint32 `json:"item_id"`
		Count       uint32 `json:"count"`
		Price       uint32 `json:"bid_copper"`
		Outcome     uint32 `json:"outcome"`
	}
	bid := a.state.PendingAuctionBids[current.BidAuctionID]
	if json.Unmarshal(incoming.Payload, &result) != nil || bid == nil || result.OperationID != current.OperationID || result.AuctionID != bid.AuctionID || result.ItemID != bid.ItemID || result.Count != bid.Count || result.Price != bid.Price || (result.Outcome != 1 && result.Outcome != 2) || incoming.Timestamp <= 0 || incoming.Timestamp > time.Now().Add(time.Minute).UnixMilli() || time.Since(time.UnixMilli(incoming.Timestamp)) > auctionObservationAge {
		return
	}
	current.AuctionProceedsOutcome = result.Outcome
	a.persist()
}

func (a *actor) handleAuctionProceeds(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if status == "completed" && reason == "no delivered non-COD attachments or money remain" && current.AuctionProceedsOutcome == 0 {
		if current.AuctionProceedsPassive {
			a.state.Task = nil
			a.revision++
			a.persist()
		} else {
			a.finishTask("blocked", "auction proceeds not delivered yet; escrow remains pending")
		}
		return
	}
	if status != "completed" || reason != "auction proceeds collected" || current.AuctionProceedsOutcome == 0 {
		if bid := a.state.PendingAuctionBids[current.BidAuctionID]; bid != nil {
			bid.Status = "receipt_uncertain"
		}
		a.finishTask("blocked", "auction proceeds receipt uncertain; do not assume a win or refund")
		return
	}
	current.OperationID = ""
	a.requestCraftingObservation(current)
}
