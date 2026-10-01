package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestAuctionBidNotificationsPreserveOutcomeWhenReceiptArrivesLater(t *testing.T) {
	a, commands := bidActor(t)
	a.applyTool(toolCall{Name: "bid_auction", Arguments: `{"auction_id":10,"item_id":3,"bid_copper":4,"max_spend_copper":4}`})
	cmd := nextLoopCommand(t, commands)
	notice := event{Type: "auction_bid_notification", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(map[string]any{"auction_id": 10, "item_id": 3, "bid_copper": 5, "own_bidder": false})}
	a.handleEvent(notice)
	if a.state.PendingAuctionBids[10].Status != "outbid_refund_pending" {
		t.Fatal("outbid event ignored")
	}
	before := len(a.recent)
	a.handleEvent(notice)
	if len(a.recent) != before {
		t.Fatal("duplicate notice retriggered decision")
	}
	a.handleEvent(event{Type: "auction_observation", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(auctionObservation{OperationID: cmd.OperationID, AuctionID: 10, ItemID: 3, PurchasedItemGUID: "1000", BidCopper: 4, NPCGUID: "auctioneer", MapID: a.latest.Bot.MapID})})
	if a.state.PendingAuctionBids[10].Status != "outbid_refund_pending" || !a.state.PendingAuctionBids[10].EscrowVerified {
		t.Fatal("late receipt overwrote known outcome")
	}
	var restored persistedAgent
	if json.Unmarshal(mustJSON(a.state), &restored) != nil || restored.PendingAuctionBids[10].OperationID != cmd.OperationID || restored.PendingAuctionBids[10].Status != "outbid_refund_pending" {
		t.Fatal("escrow journal lost restart identity")
	}
}

func TestAuctionBidNotificationRejectsOtherOwnerAndOldEvents(t *testing.T) {
	a, commands := bidActor(t)
	placeTestBid(t, a, commands)
	notice := event{Type: "auction_bid_notification", OwnerToken: "other", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(map[string]any{"auction_id": 10, "item_id": 3, "bid_copper": 0, "own_bidder": true})}
	a.handleEvent(notice)
	notice.OwnerToken = "owner"
	notice.Timestamp = time.Now().Add(-time.Hour).UnixMilli()
	a.handleEvent(notice)
	if a.state.PendingAuctionBids[10].Status != "pending" {
		t.Fatal("untrusted or old notification changed escrow")
	}
}

func bidActor(t *testing.T) (*actor, <-chan command) {
	a, commands := auctionActor(t)
	a.state.AuctionObservation.Offers[0].MinimumBid = 4
	a.state.AuctionObservation.Offers[0].ExpiresAt = time.Now().Add(time.Hour).Unix()
	return a, commands
}

func placeTestBid(t *testing.T, a *actor, commands <-chan command) command {
	a.applyTool(toolCall{Name: "bid_auction", Arguments: `{"auction_id":10,"item_id":3,"bid_copper":4,"max_spend_copper":4}`})
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "bid_auction" {
		t.Fatal("bid not dispatched")
	}
	a.handleEvent(event{Type: "auction_observation", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(auctionObservation{OperationID: cmd.OperationID, AuctionID: 10, ItemID: 3, PurchasedItemGUID: "1000", BidCopper: 4, NPCGUID: "auctioneer", MapID: a.latest.Bot.MapID})})
	a.handleTaskOperation(fishingResult(cmd.OperationID, "completed", "auction bid verified; outcome pending"))
	if cancel := nextLoopCommand(t, commands); cancel.Operation != "cancel" {
		t.Fatal("finished bid did not release native operation")
	}
	if snapshot := nextLoopCommand(t, commands); snapshot.Operation != "snapshot" {
		t.Fatal("finished bid did not request updated state")
	}
	return cmd
}

func TestAuctionBidKeepsEscrowWithoutClaimingInventoryOrRebidding(t *testing.T) {
	a, commands := bidActor(t)
	placeTestBid(t, a, commands)
	if a.state.Task != nil || !a.state.PendingAuctionBids[10].EscrowVerified {
		t.Fatal("verified escrow missing")
	}
	a.applyTool(toolCall{Name: "bid_auction", Arguments: `{"auction_id":10,"item_id":3,"bid_copper":5,"max_spend_copper":5}`})
	if a.state.Task != nil || a.state.PendingAuctionBids[10].Price != 4 {
		t.Fatal("outstanding bid resubmitted")
	}
}

func TestAuctionBidMissingReceiptRetainsUncertainJournal(t *testing.T) {
	a, commands := bidActor(t)
	a.applyTool(toolCall{Name: "bid_auction", Arguments: `{"auction_id":10,"item_id":3,"bid_copper":4,"max_spend_copper":4}`})
	cmd := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(cmd.OperationID, "completed", "auction bid verified; outcome pending"))
	if a.state.Task != nil || a.state.PendingAuctionBids[10].Status != "uncertain" {
		t.Fatal("uncertain bid discarded or treated as verified")
	}
}

func TestAuctionBidRejectsBuyoutAndInsufficientBudget(t *testing.T) {
	for _, arguments := range []string{`{"auction_id":10,"item_id":3,"bid_copper":10,"max_spend_copper":10}`, `{"auction_id":10,"item_id":3,"bid_copper":4,"max_spend_copper":3}`} {
		a, _ := bidActor(t)
		a.applyTool(toolCall{Name: "bid_auction", Arguments: arguments})
		if a.state.Task != nil || len(a.state.PendingAuctionBids) != 0 {
			t.Fatal("invalid bid reserved escrow")
		}
	}
}

func TestAuctionProceedsRequireReceiptAndCorrelatedInventory(t *testing.T) {
	for _, outcome := range []uint32{1, 2} {
		a, commands := bidActor(t)
		placeTestBid(t, a, commands)
		a.applyTool(toolCall{Name: "collect_auction_proceeds", Arguments: `{"auction_id":10,"mailbox_guid":"mailbox"}`})
		baseline := nextLoopCommand(t, commands)
		a.handleEvent(craftingInventoryEvent(baseline.RequestID, 3, 0))
		collect := nextLoopCommand(t, commands)
		if collect.Operation != "collect_mail" {
			t.Fatal("proceeds did not nest mailbox collection")
		}
		a.handleEvent(event{Type: "auction_proceeds", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(map[string]any{"operation_id": collect.OperationID, "auction_id": 10, "item_id": 3, "count": 2, "bid_copper": 4, "outcome": outcome})})
		a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "auction proceeds collected"))
		observed := nextLoopCommand(t, commands)
		a.handleEvent(craftingInventoryEvent("unrelated", 3, 2))
		if a.state.Task == nil || a.state.PendingAuctionBids[10] == nil {
			t.Fatal("unrelated inventory closed escrow")
		}
		count := uint32(0)
		if outcome == 1 {
			count = 2
		}
		a.handleEvent(craftingInventoryEvent(observed.RequestID, 3, count))
		if a.state.Task != nil || a.state.PendingAuctionBids[10] != nil {
			t.Fatal("verified proceeds did not close journal")
		}
	}
}

func TestAuctionProceedsEmptyMailboxKeepsPendingEscrow(t *testing.T) {
	a, commands := bidActor(t)
	placeTestBid(t, a, commands)
	a.state.PendingAuctionBids[10].NextCheckUTC = time.Now().Add(-time.Second)
	a.advanceTask()
	baseline := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(baseline.RequestID, 3, 0))
	collect := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "no delivered non-COD attachments or money remain"))
	if a.state.Task != nil || a.state.PendingAuctionBids[10] == nil || !time.Now().Before(a.state.PendingAuctionBids[10].NextCheckUTC) {
		t.Fatal("empty mailbox lost escrow or busy-polled")
	}
}

func TestAuctionProceedsWonWithoutInventoryBlocks(t *testing.T) {
	a, commands := bidActor(t)
	placeTestBid(t, a, commands)
	a.applyTool(toolCall{Name: "collect_auction_proceeds", Arguments: `{"auction_id":10,"mailbox_guid":"mailbox"}`})
	baseline := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(baseline.RequestID, 3, 0))
	collect := nextLoopCommand(t, commands)
	a.handleEvent(event{Type: "auction_proceeds", OwnerToken: "owner", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(map[string]any{"operation_id": collect.OperationID, "auction_id": 10, "item_id": 3, "count": 2, "bid_copper": 4, "outcome": 1})})
	a.handleTaskOperation(fishingResult(collect.OperationID, "completed", "auction proceeds collected"))
	observed := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(observed.RequestID, 3, 0))
	if a.state.Task != nil || a.state.PendingAuctionBids[10].Status != "receipt_uncertain" {
		t.Fatal("missing won inventory treated as success")
	}
}
