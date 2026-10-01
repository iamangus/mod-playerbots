package main

import (
	"encoding/json"
	"testing"
	"time"
)

func discoveryActor(t *testing.T) (*actor, <-chan command) {
	a, commands := craftingActor(t)
	a.latest.Inventory.Items = nil
	a.applyTool(toolCall{Name: "craft_item", Arguments: `{"item_id":1,"count":1,"procurement":"trade","max_spend_copper":10,"trade_request_channel":"trade","trade_request_message":"Looking to buy material 3, please whisper a price."}`})
	advert := nextLoopCommand(t, commands)
	if advert.Operation != "send_chat" || a.state.Task == nil || a.state.Task.CraftTradeStage != "discover" {
		t.Fatal("crafting did not start bounded public seller discovery")
	}
	a.latest.NearbyPlayers = []playerInfo{{Name: "Seller", GUIDRaw: "seller", MapID: a.latest.Bot.MapID, Distance: 3}}
	a.rememberEvent(event{Type: "chat_received", Timestamp: time.Now().UnixMilli(), Payload: json.RawMessage(`{"sender_name":"Seller","message":"I can sell one for 5 copper."}`)})
	return a, commands
}

func negotiateTestSeller(t *testing.T, a *actor) {
	t.Helper()
	a.applyTool(toolCall{Name: "negotiate_crafting_trade", Arguments: `{"player_name":"Seller","price_copper":5,"message":"Agreed, 5 copper. Please open trade when we meet."}`})
	if a.state.Task == nil || a.state.Task.CraftTradeStage != "wait_invite" {
		t.Fatal("observed nearby seller was not selected")
	}
}

func TestTradeDiscoveryMeetsSellerAndNestsVerifiedAcquisition(t *testing.T) {
	a, commands := discoveryActor(t)
	parentID := a.state.Task.ID
	negotiateTestSeller(t, a)
	if whisper := nextLoopCommand(t, commands); whisper.Operation != "send_chat" {
		t.Fatal("agreement not communicated")
	}
	trade := negotiatedMaterialTrade()
	trade.PartnerGUID, trade.WindowOpen, trade.Ours.Money, trade.Theirs.Accepted = "seller", false, 0, false
	a.latest.Trade = mustJSON(trade)
	a.advanceTask()
	open := nextLoopCommand(t, commands)
	if open.Operation != "begin_trade" {
		t.Fatal("pending seller trade not opened")
	}
	a.handleTaskOperation(fishingResult(open.OperationID, "accepted", "trade operation queued; verify live trade state"))
	if a.state.Task.OperationID != open.OperationID {
		t.Fatal("queue acceptance counted as trade opening")
	}
	a.handleTaskOperation(fishingResult(open.OperationID, "completed", "trade state updated"))
	openedSnapshot := nextLoopCommand(t, commands)
	trade.WindowOpen, trade.Revision = true, trade.Revision+1
	a.latest.Trade = mustJSON(trade)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: openedSnapshot.RequestID, Payload: mustJSON(a.latest)})
	price := nextLoopCommand(t, commands)
	if price.Operation != "offer_trade_money" {
		t.Fatal("agreed price not offered")
	}
	a.handleTaskOperation(fishingResult(price.OperationID, "accepted", "trade operation queued; verify live trade state"))
	a.handleTaskOperation(fishingResult(price.OperationID, "completed", "trade state updated"))
	pricedSnapshot := nextLoopCommand(t, commands)
	trade.Ours.Money, trade.Theirs.Accepted, trade.Revision = 5, true, trade.Revision+1
	a.latest.Trade = mustJSON(trade)
	a.handleEvent(event{Type: "snapshot", OwnerToken: "owner", RequestID: pricedSnapshot.RequestID, Payload: mustJSON(a.latest)})
	baseline := nextLoopCommand(t, commands)
	if baseline.Operation != "snapshot" || a.state.Task.ID != parentID {
		t.Fatal("negotiation replaced crafting or skipped inventory gate")
	}
	state := craftingInventoryEvent(baseline.RequestID, 3, 0)
	var payload map[string]any
	_ = json.Unmarshal(state.Payload, &payload)
	payload["trade"] = trade
	state.Payload = mustJSON(payload)
	a.handleEvent(state)
	receive := nextLoopCommand(t, commands)
	if receive.Operation != "receive_trade_material" || a.state.Task.CraftSupplies[0].Budget != 5 {
		t.Fatal("negotiated offer did not realize a bounded atomic material child")
	}
	a.handleTaskOperation(fishingResult(receive.OperationID, "completed", "trade material transfer verified"))
	postTransfer := nextLoopCommand(t, commands)
	a.handleEvent(craftingInventoryEvent(postTransfer.RequestID, 3, 2))
	if cast := nextLoopCommand(t, commands); cast.Operation != "craft_once" {
		t.Fatal("verified negotiated material did not release the subcraft")
	}
}

func TestTradeDiscoveryMeetsDistantObservedSellerWithoutFollowing(t *testing.T) {
	a, commands := discoveryActor(t)
	a.latest.NearbyPlayers[0].Distance = 30
	a.applyTool(toolCall{Name: "negotiate_crafting_trade", Arguments: `{"player_name":"Seller","price_copper":5,"message":"Agreed. Please open trade when we meet."}`})
	_ = nextLoopCommand(t, commands)
	meeting := nextLoopCommand(t, commands)
	if meeting.Operation != "navigate_to_player" {
		t.Fatal("negotiation did not realize an ordinary meeting child")
	}
	a.handleTaskOperation(fishingResult(meeting.OperationID, "completed", "arrived at target"))
	_ = nextLoopCommand(t, commands)
	if a.state.Task.Phase != "trade_negotiate" || a.state.Task.CraftTradeStage != "wait_invite" {
		t.Fatal("arrival did not end movement and wait for seller trade")
	}
	for range 3 {
		a.advanceTask()
	}
	select {
	case cmd := <-commands:
		t.Fatalf("meeting turned into continuous follow/replay: %s", cmd.Operation)
	default:
	}
}

func TestTradeDiscoveryRequiresContactAndBoundedPrice(t *testing.T) {
	for _, scenario := range []string{"no_contact", "old_contact", "over_budget", "unobserved"} {
		t.Run(scenario, func(t *testing.T) {
			a, _ := discoveryActor(t)
			arguments := `{"player_name":"Seller","price_copper":5,"message":"Agreed, please open trade."}`
			switch scenario {
			case "no_contact":
				a.recent = nil
			case "old_contact":
				a.recent[0].Timestamp = time.Now().Add(-time.Hour).UnixMilli()
			case "over_budget":
				arguments = `{"player_name":"Seller","price_copper":11,"message":"Agreed."}`
			case "unobserved":
				a.latest.NearbyPlayers = nil
			}
			a.applyTool(toolCall{Name: "negotiate_crafting_trade", Arguments: arguments})
			if a.state.Task.CraftTradeStage != "discover" {
				t.Fatal("unverified seller or unaffordable quote displaced discovery")
			}
		})
	}
}

func TestTradeDiscoveryDeadlineAndManualControls(t *testing.T) {
	a, _ := discoveryActor(t)
	a.applyTool(toolCall{Name: "accept_trade", Arguments: `{"trade_revision":1}`})
	if a.state.Task.CraftTradeStage != "discover" {
		t.Fatal("manual acceptance displaced negotiated owner")
	}
	a.state.Task.CraftTradeDeadline = time.Now().Add(-time.Second)
	a.advanceTask()
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("seller discovery waited forever")
	}
}

func TestTradeDiscoveryNeverRepeatsUnobservedMoneyOffer(t *testing.T) {
	a, commands := discoveryActor(t)
	negotiateTestSeller(t, a)
	_ = nextLoopCommand(t, commands)
	trade := negotiatedMaterialTrade()
	trade.PartnerGUID, trade.Ours.Money, trade.Theirs.Accepted = "seller", 0, false
	a.latest.Trade = mustJSON(trade)
	a.advanceTask()
	price := nextLoopCommand(t, commands)
	a.handleTaskOperation(fishingResult(price.OperationID, "accepted", "trade operation queued; verify live trade state"))
	a.handleTaskOperation(fishingResult(price.OperationID, "completed", "trade state updated"))
	_ = nextLoopCommand(t, commands)
	for range 3 {
		a.advanceTask()
	}
	select {
	case cmd := <-commands:
		t.Fatalf("unobserved price was replayed: %s", cmd.Operation)
	default:
	}
	var restored task
	if json.Unmarshal(mustJSON(a.state.Task), &restored) != nil || !restored.CraftTradePriced || restored.CraftTradePrice != 5 {
		t.Fatal("price no-replay marker not persisted")
	}
}

func TestTradeDiscoveryRejectsDifferentPartnerAndWrongAcceptedMaterials(t *testing.T) {
	for _, wrongPartner := range []bool{true, false} {
		a, commands := discoveryActor(t)
		negotiateTestSeller(t, a)
		_ = nextLoopCommand(t, commands)
		trade := negotiatedMaterialTrade()
		trade.PartnerGUID, trade.Ours.Money = "seller", 5
		if wrongPartner {
			trade.PartnerGUID = "other"
		} else {
			trade.Theirs.Items[0].ItemID = 999
		}
		a.latest.Trade = mustJSON(trade)
		a.advanceTask()
		if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
			t.Fatal("wrong partner/material accepted")
		}
	}
}
