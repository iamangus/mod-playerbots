package main

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	npcFlagTrainer      = 0x10
	npcFlagVendor       = 0x80
	npcFlagRepair       = 0x1000
	npcFlagFlightMaster = 0x2000
)

func (a *actor) startNPCServiceTask(tool string, args map[string]json.RawMessage) {
	var target, service string
	var budget, itemID uint32
	limit := uint32(12)
	targetKey := "npc_guid"
	if tool == "collect_mail" || tool == "send_mail" {
		targetKey = "mailbox_guid"
	}
	if json.Unmarshal(args[targetKey], &target) != nil || target == "" {
		a.rejectTool(tool, "npc_guid must identify an observed nearby NPC")
		return
	}
	operation, flag := "train_class_spells", uint32(npcFlagTrainer)
	if tool == "collect_mail" || tool == "send_mail" {
		operation, flag = tool, 0
	}
	if tool == "discover_flight_path" || tool == "take_flight" {
		operation, flag = tool, npcFlagFlightMaster
		if tool == "take_flight" && (json.Unmarshal(args["destination_node"], &itemID) != nil || itemID == 0) {
			a.rejectTool(tool, "destination_node must identify a known direct taxi destination")
			return
		}
	}
	if tool == "visit_vendor" {
		_ = json.Unmarshal(args["service"], &service)
		switch service {
		case "repair":
			operation, flag = "repair_equipment", npcFlagRepair
		case "sell_junk":
			operation, flag = "sell_junk", npcFlagVendor
		case "buy":
			operation, flag = "buy_vendor_item", npcFlagVendor
			if json.Unmarshal(args["item_id"], &itemID) != nil || itemID == 0 {
				a.rejectTool(tool, "buy requires a valid item_id")
				return
			}
		default:
			a.rejectTool(tool, "service must be repair, sell_junk, or buy")
			return
		}
	}
	if raw, provided := args["count"]; provided {
		if string(raw) == "null" || json.Unmarshal(raw, &limit) != nil || limit == 0 || limit > 40 {
			a.rejectTool(tool, "count must be between 1 and 40")
			return
		}
	}
	if operation != "sell_junk" && operation != "collect_mail" && operation != "discover_flight_path" {
		if raw := args["max_spend_copper"]; string(raw) == "null" || json.Unmarshal(raw, &budget) != nil {
			a.rejectTool(tool, "an explicit max_spend_copper budget is required")
			return
		}
	}
	serviceArgs := map[string]any{"target_guid": target, "max_spend_copper": budget,
		"item_id": itemID, "destination_node": itemID, "count": limit}
	if tool == "send_mail" {
		if current := a.state.Task; current != nil && current.Kind == "npc_service" && current.ServiceOperation == "send_mail" {
			a.rejectTool(tool, "a mail submission is already in flight; await its result instead of duplicating it")
			return
		}
		var recipient, subject, body, itemGUID string
		var money uint32
		if json.Unmarshal(args["recipient"], &recipient) != nil || strings.TrimSpace(recipient) == "" || len(recipient) > 48 ||
			json.Unmarshal(args["subject"], &subject) != nil || len(subject) == 0 || len(subject) > 128 {
			a.rejectTool(tool, "recipient and a subject of at most 128 bytes are required")
			return
		}
		if raw, provided := args["body"]; provided && json.Unmarshal(raw, &body) != nil {
			a.rejectTool(tool, "body must be text")
			return
		}
		if raw, provided := args["money_copper"]; provided && (string(raw) == "null" || json.Unmarshal(raw, &money) != nil) {
			a.rejectTool(tool, "money_copper must be a nonnegative integer")
			return
		}
		if raw, provided := args["item_guid"]; provided && (string(raw) == "null" || json.Unmarshal(raw, &itemGUID) != nil || itemGUID == "") {
			a.rejectTool(tool, "item_guid must identify an observed transferable stack")
			return
		}
		if len(body) > 4096 || strings.ContainsRune(recipient+subject+body, '\x00') || uint64(money)+30 > uint64(budget) {
			a.rejectTool(tool, "invalid mail text or budget below gift plus 30 copper postage")
			return
		}
		if itemGUID != "" {
			var stacks []struct {
				GUID string `json:"item_guid"`
			}
			_ = json.Unmarshal(a.latest.Inventory.TradeableStacks, &stacks)
			observed := false
			for _, stack := range stacks {
				observed = observed || stack.GUID == itemGUID
			}
			if !observed {
				a.rejectTool(tool, "attachment must come from observed tradeable_stacks; the whole stack is mailed")
				return
			}
		}
		serviceArgs["recipient"], serviceArgs["subject"], serviceArgs["body"] = recipient, subject, body
		serviceArgs["item_guid"], serviceArgs["money_copper"], serviceArgs["count"] = itemGUID, money, uint32(1)
	}
	var selected *npcInfo
	if tool == "collect_mail" || tool == "send_mail" {
		for _, mailbox := range a.latest.NearbyMailboxes {
			if mailbox.GUID == target {
				selected = &npcInfo{GUID: mailbox.GUID, Name: mailbox.Name}
				break
			}
		}
	}
	for index := range a.latest.NearbyNPCs {
		npc := &a.latest.NearbyNPCs[index]
		if flag != 0 && npc.GUID == target && npc.NPCFlags&flag != 0 {
			selected = npc
			break
		}
	}
	if !a.hasSnapshot || selected == nil {
		a.rejectTool(tool, "NPC is not observed with the required service flags")
		return
	}
	if tool == "train_class_spells" && !selected.ClassTrainer {
		a.rejectTool(tool, "select an observed compatible class trainer")
		return
	}
	if tool == "take_flight" {
		known := false
		for _, route := range selected.TaxiRoutes {
			known = known || route.ToNode == itemID
		}
		if !known {
			a.rejectTool(tool, "select a known direct destination from this flight master's taxi_routes")
			return
		}
	}
	if current := a.state.Task; tool != "send_mail" && current != nil && current.Kind == "npc_service" &&
		current.TargetGUID == target && current.ServiceOperation == operation {
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "npc_service", ServiceOperation: operation,
		ServiceArguments: mustJSON(serviceArgs), TargetGUID: target, TargetName: selected.Name,
		MapID: a.latest.Bot.MapID, Phase: "service", GoalCount: 1, LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) advanceNPCServiceTask(current *task) {
	if a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "bot died or left the NPC service map")
		return
	}
	if a.latest.Bot.InCombat {
		return
	}
	// Economic operations are deliberately not replayed after an uncertain
	// timeout; buying or selling twice is not equivalent to movement retries.
	if current.Retries > 0 {
		a.finishTask("blocked", "NPC service timed out; inspect a fresh snapshot before trying again")
		return
	}
	current.Phase = "service"
	current.OperationID = a.sendPrimitive(current.ServiceOperation, current.ServiceArguments)
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleNPCServiceOperation(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if status == "accepted" {
		a.finishTask("blocked", "NPC services require a persistent native operation")
		return
	}
	if current.ServiceOperation == "inspect_auctions" && status == "completed" &&
		(reason != "bounded auction page observed" || a.state.AuctionObservation == nil || a.state.AuctionObservation.OperationID != current.OperationID) {
		a.finishTask("blocked", "correlated auction page unavailable")
		return
	}
	current.OperationID = ""
	if status != "completed" {
		a.finishTask("blocked", "NPC service failed: "+reason)
		return
	}
	current.Completed = 1
	a.finishTask("completed", reason)
}
