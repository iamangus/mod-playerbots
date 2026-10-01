package main

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"
)

const tradeDiscoveryTimeout = 10 * time.Minute

func (a *actor) negotiateCraftingTrade(args map[string]json.RawMessage) {
	current := a.state.Task
	var name, message string
	var price uint32
	if current == nil || current.Kind != "crafting" || current.CraftTradeStage != "discover" ||
		json.Unmarshal(args["player_name"], &name) != nil || name == "" ||
		json.Unmarshal(args["price_copper"], &price) != nil || string(args["price_copper"]) == "null" || price > current.CraftAcquisitionBudget ||
		json.Unmarshal(args["message"], &message) != nil || !validTradeMessage(message) {
		a.rejectTool("negotiate_crafting_trade", "active trade-discovery parent, seller, agreed price within budget, and negotiation message required")
		return
	}
	if !a.bridgeActive || !a.hasSnapshot || time.Now().After(current.CraftTradeDeadline) {
		a.rejectTool("negotiate_crafting_trade", "live native observations required before discovery expires")
		return
	}
	// The LLM interprets the conversation; this guard requires actual recent contact,
	// not an invented seller or access to somebody else's inventory.
	contact := false
	for _, recent := range a.recent {
		var chat struct {
			Name string `json:"sender_name"`
		}
		if recent.Type == "chat_received" && recent.Timestamp > 0 && recent.Timestamp <= time.Now().UnixMilli() &&
			time.Since(time.UnixMilli(recent.Timestamp)) <= tradeDiscoveryTimeout && json.Unmarshal(recent.Payload, &chat) == nil && strings.EqualFold(chat.Name, name) {
			contact = true
		}
	}
	target := navigationPlayer(a.latest, name, "")
	if !contact || target == nil {
		a.rejectTool("negotiate_crafting_trade", "seller must have recent chat contact and be observed on this map; arrange a meeting through ordinary chat first")
		return
	}
	guid := target.GUIDRaw
	if guid == "" {
		guid = target.GUID
	}
	if guid == "" || guid == "0" {
		a.rejectTool("negotiate_crafting_trade", "seller identity unavailable")
		return
	}
	current.TargetName, current.TargetGUID = target.Name, guid
	current.CraftTradePrice, current.CraftTradeStage = price, "meet"
	current.Phase, current.LastProgressUTC = "trade_negotiate", time.Now().UTC()
	if !a.persist() {
		a.finishTask("blocked", "trade agreement could not be persisted; no negotiation dispatched")
		return
	}
	a.sendPrimitive("send_chat", map[string]any{"channel": "whisper", "recipient": target.Name, "message": message})
	a.advanceTask()
}

func validTradeMessage(message string) bool {
	return utf8.ValidString(message) && strings.TrimSpace(message) != "" && len(message) <= 255 && !strings.ContainsAny(message, "\x00\r\n")
}

func (a *actor) advanceCraftingTrade(current *task) {
	if time.Now().After(current.CraftTradeDeadline) {
		a.finishTask("blocked", "material trade discovery or negotiation expired; no automatic resource replay")
		return
	}
	if current.CraftTradeStage == "discover" || a.latest.Bot.InCombat {
		return
	}
	var trade materialTrade
	_ = json.Unmarshal(a.latest.Trade, &trade)
	if trade.Active && trade.PartnerGUID != current.TargetGUID {
		a.finishTask("blocked", "a different trade partner interrupted negotiated acquisition")
		return
	}
	if !trade.Active {
		if current.CraftTradeOpened || current.CraftTradePriced {
			a.finishTask("blocked", "negotiated trade closed before verified material transfer")
			return
		}
		target := navigationPlayer(a.latest, current.TargetName, current.TargetGUID)
		if target == nil {
			a.finishTask("blocked", "negotiated seller is no longer observed; arrange a fresh meeting")
			return
		}
		if target.Distance <= 5 {
			current.CraftTradeStage = "wait_invite"
			a.persist()
			return
		}
		if current.CraftTradeStage == "wait_invite" {
			return
		}
		current.Phase = "trade_meet"
		current.OperationID = a.sendPrimitive("navigate_to_player", map[string]any{"player_name": current.TargetName, "target_guid": current.TargetGUID, "distance": 5})
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if len(trade.Ours.Items) != 0 || trade.Ours.SpellID != 0 || trade.Theirs.SpellID != 0 || trade.Theirs.Money != 0 {
		a.finishTask("blocked", "negotiated material trade contains outgoing items or services/currency")
		return
	}
	if !trade.WindowOpen {
		if current.CraftTradeOpened {
			return
		}
		current.CraftTradeOpened = true
		a.dispatchCraftingTradeControl(current, "begin_trade", map[string]any{"trade_revision": trade.Revision})
		return
	}
	current.CraftTradeOpened = true
	if trade.Ours.Money != current.CraftTradePrice {
		if current.CraftTradePriced {
			return
		}
		current.CraftTradePriced = true
		a.dispatchCraftingTradeControl(current, "offer_trade_money", map[string]any{"trade_revision": trade.Revision, "money_copper": current.CraftTradePrice})
		return
	}
	current.CraftTradePriced = true
	if !trade.Theirs.Accepted {
		return
	}
	stock := craftingStock(a.latest)
	var missing []craftReagent
	for _, goal := range current.CraftMaterialGoals {
		if stock[goal.ItemID] < goal.Count {
			missing = append(missing, craftReagent{ItemID: goal.ItemID, Count: goal.Count - stock[goal.ItemID]})
		}
	}
	if len(missing) == 0 {
		a.finishTask("blocked", "material goals changed during negotiation; inspect inventory and cancel the unused trade")
		return
	}
	supply, err := a.tradeMaterialBatch(missing, stock, current.CraftTradePrice)
	if err != nil {
		a.finishTask("blocked", "accepted seller offer does not match negotiated materials: "+err.Error())
		return
	}
	current.CraftSupplies, current.CraftNeedsSupplies = []craftSupply{supply}, false
	current.CraftTradeStage = "acquire"
	a.requestCraftingObservation(current)
}

func (a *actor) dispatchCraftingTradeControl(current *task, operation string, arguments map[string]any) {
	current.Phase, current.OperationID = "trade_control", newID()
	current.LastProgressUTC = time.Now().UTC()
	if !a.persist() {
		a.finishTask("blocked", "trade control journal unavailable; no control dispatched")
		return
	}
	a.sendCommandWithID(newID(), current.OperationID, operation, arguments)
}

func (a *actor) handleCraftingTradeControl(status, reason string) {
	current := a.state.Task
	if status == "accepted" {
		return
	}
	if current.Phase == "trade_meet" {
		if status != "completed" || reason != "arrived at target" {
			a.finishTask("blocked", "negotiated trade meeting failed")
			return
		}
		current.CraftTradeStage = "wait_invite"
	} else if status != "completed" || reason != "trade state updated" {
		a.finishTask("blocked", "negotiated trade control unavailable; inspect the offer before retrying")
		return
	}
	current.OperationID, current.Phase = "", "trade_negotiate"
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
	a.requestSnapshot()
}
