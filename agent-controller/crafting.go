package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type craftSupply struct {
	NPCGUID           string         `json:"npc_guid"`
	ItemID            uint32         `json:"item_id"`
	Bundles           uint32         `json:"bundles"`
	Budget            uint32         `json:"budget"`
	Gather            bool           `json:"gather,omitempty"`
	RequiredCount     uint32         `json:"required_count,omitempty"`
	AuctionID         uint32         `json:"auction_id,omitempty"`
	ItemGUID          string         `json:"item_guid,omitempty"`
	MailboxGUID       string         `json:"mailbox_guid,omitempty"`
	TradePartnerGUID  string         `json:"trade_partner_guid,omitempty"`
	TradeRevision     uint64         `json:"trade_revision,omitempty"`
	TradeMaterials    []craftReagent `json:"trade_materials,omitempty"`
	RequiredMaterials []craftReagent `json:"required_materials,omitempty"`
}

func (a *actor) craftingMaterialSupplies(missing []craftReagent, stock map[uint32]uint32, budget uint32, vendorsAllowed bool) ([]craftSupply, error) {
	if len(missing) > maxCraftingCasts {
		return nil, fmt.Errorf("too many external material types")
	}
	var supplies []craftSupply
	remaining := budget
	if vendorsAllowed && len(missing) != 0 {
		trade, err := a.tradeMaterialBatch(missing, stock, remaining)
		if err == nil {
			return []craftSupply{trade}, nil
		}
	}
	for _, material := range missing {
		if vendorsAllowed {
			vendor, err := craftingSupplies([]craftReagent{material}, a.latest.NearbyNPCs, remaining)
			if err == nil {
				supplies = append(supplies, vendor[0])
				remaining -= vendor[0].Budget
				continue
			}
		}
		if a.craftingGatherTarget(material.ItemID, nil) == "" && a.knownGatheringSite(material.ItemID, nil) == nil {
			if vendorsAllowed {
				auctions, err := a.auctionSupplies(material, stock[material.ItemID], remaining)
				if err == nil {
					for _, supply := range auctions {
						remaining -= supply.Budget
					}
					supplies = append(supplies, auctions...)
					if len(supplies) > maxCraftingCasts {
						return nil, fmt.Errorf("too many acquisition children")
					}
					continue
				}
			}
			return nil, fmt.Errorf("material %d has no eligible observed source within the explicit budget", material.ItemID)
		}
		required := uint64(stock[material.ItemID]) + uint64(material.Count)
		if required > uint64(^uint32(0)) {
			return nil, fmt.Errorf("material inventory goal overflow")
		}
		supplies = append(supplies, craftSupply{ItemID: material.ItemID, Gather: true, RequiredCount: uint32(required)})
	}
	return supplies, nil
}

func (a *actor) craftingGatherTarget(itemID uint32, tried []string) string {
	best := ""
	bestDistance := 0.0
	for _, object := range a.latest.NearbyGameObjects {
		if !object.CanGather || object.GatherSkill == 0 || object.GUID == "" || object.Distance < 0 {
			continue
		}
		skipped := false
		for _, guid := range tried {
			if guid == object.GUID {
				skipped = true
				break
			}
		}
		if skipped {
			continue
		}
		// Recheck expiry from persisted evidence, not a potentially old decorated snapshot.
		evidence := a.state.GatheringSources[gatheringSourceKey("gameobject", object.Entry)]
		if evidence.ObservedAt > time.Now().UnixMilli() || time.Since(time.UnixMilli(evidence.ObservedAt)) > gatheringEvidenceAge {
			continue
		}
		for _, observed := range evidence.freshItems(time.Now()) {
			if observed == itemID && (best == "" || object.Distance < bestDistance || (object.Distance == bestDistance && object.GUID < best)) {
				best, bestDistance = object.GUID, object.Distance
			}
		}
	}
	return best
}

func craftingSupplies(missing []craftReagent, npcs []npcInfo, budget uint32) ([]craftSupply, error) {
	if len(missing) > maxCraftingCasts {
		return nil, fmt.Errorf("too many external material types")
	}
	var supplies []craftSupply
	remaining := uint64(budget)
	for _, material := range missing {
		var best *craftSupply
		for _, npc := range npcs {
			if npc.GUID == "" || npc.NPCFlags&npcFlagVendor == 0 {
				continue
			}
			for _, offer := range npc.VendorOffers {
				if offer.ItemID != material.ItemID || offer.BundleCount == 0 {
					continue
				}
				bundles := (uint64(material.Count) + uint64(offer.BundleCount) - 1) / uint64(offer.BundleCount)
				price := bundles * uint64(offer.PriceCopper)
				if bundles == 0 || bundles > maxCraftingCasts || price > remaining {
					continue
				}
				candidate := craftSupply{NPCGUID: npc.GUID, ItemID: material.ItemID, Bundles: uint32(bundles), Budget: uint32(price)}
				if best == nil || candidate.Budget < best.Budget || (candidate.Budget == best.Budget && candidate.NPCGUID < best.NPCGUID) {
					best = &candidate
				}
			}
		}
		if best == nil {
			return nil, fmt.Errorf("material %d x%d has no observed vendor supply within the remaining budget", material.ItemID, material.Count)
		}
		remaining -= uint64(best.Budget)
		supplies = append(supplies, *best)
	}
	return supplies, nil
}

func craftingStock(state snapshot) map[uint32]uint32 {
	stock := make(map[uint32]uint32)
	for _, item := range state.Inventory.Items {
		stock[item.ItemID] = item.Count
	}
	for _, item := range state.CraftingInventory {
		stock[item.ItemID] = item.Count // Relevant zero counts are authoritative too.
	}
	return stock
}

func craftingObservedStock(state snapshot) []craftReagent {
	stock := craftingStock(state)
	items := make([]craftReagent, 0, len(stock))
	for itemID, count := range stock {
		items = append(items, craftReagent{ItemID: itemID, Count: count})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ItemID < items[j].ItemID })
	return items
}

func (a *actor) startCraftingTask(args map[string]json.RawMessage) {
	var itemID, count uint32
	var budget uint32
	var procurement, requestMessage, requestChannel string
	if raw, ok := args["procurement"]; ok && (json.Unmarshal(raw, &procurement) != nil || (procurement != "auto" && procurement != "trade")) {
		a.rejectTool("craft_item", "procurement must be auto or trade")
		return
	}
	budgetRaw, budgetProvided := args["max_spend_copper"]
	if budgetProvided && (string(budgetRaw) == "null" || json.Unmarshal(budgetRaw, &budget) != nil) {
		a.rejectTool("craft_item", "max_spend_copper must be a nonnegative integer")
		return
	}
	if !a.hasSnapshot || json.Unmarshal(args["item_id"], &itemID) != nil || itemID == 0 ||
		json.Unmarshal(args["count"], &count) != nil {
		a.rejectTool("craft_item", "observed recipe item_id and a count between 1 and 40 are required")
		return
	}
	if current := a.state.Task; current != nil && (current.Kind == "crafting" || current.Kind == "auction_purchase") {
		a.rejectTool("craft_item", "a crafting parent is already active; await or explicitly cancel it")
		return
	}
	stock := craftingStock(a.latest)
	if page := a.latest.CraftingRecipePage; page != nil && page.DependenciesTruncated {
		a.rejectTool("craft_item", "focused recipe dependencies are truncated; a complete bounded plan is required")
		return
	}
	plan, err := planCrafting(itemID, count, stock, a.latest.CraftingRecipes)
	if err != nil {
		a.rejectTool("craft_item", err.Error())
		return
	}
	var supplies []craftSupply
	searchNPC := ""
	if procurement == "trade" {
		if !budgetProvided || !a.bridgeActive || len(plan.Missing) == 0 || len(plan.Missing) > 6 ||
			json.Unmarshal(args["trade_request_message"], &requestMessage) != nil || !validTradeMessage(requestMessage) ||
			json.Unmarshal(args["trade_request_channel"], &requestChannel) != nil || (requestChannel != "trade" && requestChannel != "say" && requestChannel != "world") {
			a.rejectTool("craft_item", "trade procurement requires 1..6 missing materials, explicit budget, request message and trade/say/world channel")
			return
		}
		total := uint64(0)
		for _, material := range plan.Missing {
			total += uint64(material.Count)
		}
		if total > 40 {
			a.rejectTool("craft_item", "trade bundle exceeds 40 total missing items")
			return
		}
	}
	if len(plan.Missing) != 0 && procurement != "trade" {
		supplies, err = a.craftingMaterialSupplies(plan.Missing, stock, budget, budgetProvided)
		if err != nil {
			searchNPC = a.observedAuctioneer()
			if !budgetProvided || searchNPC == "" || len(a.latest.NearbyMailboxes) == 0 || len(plan.Missing) > maxAuctionSearchItems {
				a.rejectTool("craft_item", err.Error())
				return
			}
		}
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "crafting", MapID: a.latest.Bot.MapID,
		CraftItemID: itemID, CraftInitialCount: stock[itemID], CraftSteps: plan.Steps,
		CraftSupplies:      supplies,
		CraftObservedStock: craftingObservedStock(a.latest),
		GoalCount:          count, Phase: "select", LastProgressUTC: time.Now().UTC()}
	if searchNPC != "" || procurement == "trade" {
		current := a.state.Task
		current.TargetGUID, current.Phase, current.CraftNeedsSupplies, current.CraftAcquisitionBudget = searchNPC, "auction_search", true, budget
		if procurement == "trade" {
			current.Phase, current.CraftTradeStage = "trade_negotiate", "discover"
			current.CraftTradeDeadline = time.Now().Add(tradeDiscoveryTimeout)
		}
		for _, material := range plan.Missing {
			required := uint64(stock[material.ItemID]) + uint64(material.Count)
			if required > uint64(^uint32(0)) {
				a.finishTask("blocked", "material acquisition goal overflow")
				return
			}
			current.AuctionSearchItems = append(current.AuctionSearchItems, material.ItemID)
			current.CraftMaterialGoals = append(current.CraftMaterialGoals, craftReagent{ItemID: material.ItemID, Count: uint32(required)})
		}
	}
	if !a.persist() {
		a.finishTask("blocked", "crafting plan journal unavailable; no acquisition dispatched")
		return
	}
	if procurement == "trade" {
		a.sendPrimitive("send_chat", map[string]any{"channel": requestChannel, "message": requestMessage})
	}
	a.advanceTask()
}

func (a *actor) requestCraftingObservation(current *task) {
	if current.CraftObservationAttempts >= 3 {
		a.finishTask("blocked", "fresh crafting inventory observation unavailable")
		return
	}
	current.CraftObservationAttempts++
	current.Phase = "observe"
	current.CraftSnapshotID = newID()
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
	focus := current.CraftItemID
	if current.CraftSupplyIndex < uint32(len(current.CraftSupplies)) {
		focus = current.CraftSupplies[current.CraftSupplyIndex].ItemID
	}
	a.sendCommand(current.CraftSnapshotID, "snapshot", map[string]any{"craft_item_id": focus, "recipe_item_id": current.CraftItemID})
}

func (a *actor) advanceCraftingTask(current *task) {
	if a.latest.Bot.MapID != current.MapID || (a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive) {
		a.finishTask("blocked", "bot died or left the crafting map")
		return
	}
	if current.Retries != 0 {
		a.finishTask("blocked", "crafting result is uncertain; inspect inventory before starting another plan")
		return
	}
	if current.Phase == "auction_search" {
		a.advanceAuctionSearch(current)
		return
	}
	if current.Phase == "trade_negotiate" {
		a.advanceCraftingTrade(current)
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
	stock := craftingStock(a.latest)
	if current.CraftObservedStock != nil {
		stock = make(map[uint32]uint32, len(current.CraftObservedStock))
		for _, item := range current.CraftObservedStock {
			stock[item.ItemID] = item.Count
		}
	}
	if current.CraftNeedsSupplies {
		var missing []craftReagent
		for _, goal := range current.CraftMaterialGoals {
			if stock[goal.ItemID] < goal.Count {
				missing = append(missing, craftReagent{ItemID: goal.ItemID, Count: goal.Count - stock[goal.ItemID]})
			}
		}
		supplies, err := a.craftingMaterialSupplies(missing, stock, current.CraftAcquisitionBudget, true)
		if err != nil {
			a.finishTask("blocked", "bounded material search could not supply the plan: "+err.Error())
			return
		}
		current.CraftSupplies, current.CraftNeedsSupplies = supplies, false
		a.persist()
	}
	if current.CraftSupplyIndex < uint32(len(current.CraftSupplies)) {
		supply := current.CraftSupplies[current.CraftSupplyIndex]
		if supply.TradePartnerGUID != "" {
			if current.CraftTradeReceived {
				goals := supply.RequiredMaterials
				if len(goals) == 0 {
					goals = []craftReagent{{ItemID: supply.ItemID, Count: supply.RequiredCount}}
				}
				for _, goal := range goals {
					if goal.ItemID == 0 || goal.Count == 0 || stock[goal.ItemID] < goal.Count {
						a.finishTask("blocked", "traded material missing from correlated inventory")
						return
					}
				}
				current.CraftTradeReceived = false
				current.CraftSupplyIndex++
				a.persist()
				a.advanceCraftingTask(current)
				return
			}
			materials := supply.TradeMaterials
			if len(materials) == 0 {
				materials = []craftReagent{{ItemID: supply.ItemID, Count: supply.Bundles}}
			}
			fresh, err := a.tradeMaterialBatch(materials, stock, supply.Budget)
			if err != nil || fresh.TradePartnerGUID != supply.TradePartnerGUID || fresh.TradeRevision != supply.TradeRevision || fresh.Budget != supply.Budget || fresh.Bundles != supply.Bundles || len(current.CraftSupplies) != 1 {
				a.finishTask("blocked", "negotiated material offer changed; inspect trade before trying again")
				return
			}
			current.Phase = "trade_material"
			for index, material := range materials {
				if index >= len(fresh.TradeMaterials) || material != fresh.TradeMaterials[index] {
					a.finishTask("blocked", "material trade composition changed")
					return
				}
			}
			current.OperationID = newID()
			current.LastProgressUTC = time.Now().UTC()
			if !a.persist() {
				a.finishTask("blocked", "material transfer journal unavailable; no acceptance dispatched")
				return
			}
			a.sendCommandWithID(newID(), current.OperationID, "receive_trade_material", map[string]any{"target_guid": supply.TradePartnerGUID, "trade_revision": supply.TradeRevision, "item_id": supply.ItemID, "count": supply.Bundles, "materials": materials, "max_spend_copper": supply.Budget})
			return
		}
		if supply.AuctionID != 0 {
			if supply.ItemID == 0 || supply.ItemGUID == "" || supply.MailboxGUID == "" || supply.NPCGUID == "" || supply.Budget == 0 || supply.Bundles == 0 || supply.Bundles > 40 || supply.RequiredCount == 0 || len(current.CraftSupplies) > maxCraftingCasts {
				a.finishTask("blocked", "invalid persisted auction procurement")
				return
			}
			if current.CraftAuctionCollect {
				if stock[supply.ItemID] < supply.RequiredCount {
					a.finishTask("blocked", "auction mail did not yield the purchased material in correlated inventory")
					return
				}
				current.CraftAuctionCollect = false
				current.CraftAuctionReceipt = ""
				current.CraftSupplyIndex++
				a.persist()
				a.advanceCraftingTask(current)
				return
			}
			if current.CraftAuctionReceipt != "" {
				current.Phase = "auction_collect"
				current.OperationID = a.sendPrimitive("collect_mail", map[string]any{"target_guid": supply.MailboxGUID, "collect_item_guid": supply.ItemGUID, "count": 1})
			} else {
				current.Phase = "auction_buy"
				current.OperationID = a.sendPrimitive("buy_auction", map[string]any{"target_guid": supply.NPCGUID, "auction_id": supply.AuctionID, "item_id": supply.ItemID, "item_guid": supply.ItemGUID, "count": supply.Bundles, "buyout_copper": supply.Budget, "max_spend_copper": supply.Budget})
			}
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
		if supply.Gather {
			if supply.ItemID == 0 || supply.RequiredCount == 0 || len(current.CraftSupplies) > maxCraftingCasts {
				a.finishTask("blocked", "invalid persisted gathering procurement")
				return
			}
			quantity := stock[supply.ItemID]
			if current.CraftGatherAwaiting {
				current.CraftGatherAwaiting = false
				if quantity <= current.CraftGatherBefore {
					a.finishTask("blocked", "gathering did not increase the requested material in fresh inventory")
					return
				}
			}
			if quantity >= supply.RequiredCount {
				current.CraftSupplyIndex++
				a.persist()
				a.advanceCraftingTask(current)
				return
			}
			target := a.craftingGatherTarget(supply.ItemID, current.CraftGatherTried)
			if target == "" && len(current.CraftGatherTried) < maxCraftingCasts {
				if site := a.knownGatheringSite(supply.ItemID, current.CraftGatherSitesTried); site != nil {
					current.CraftGatherSitesTried = append(current.CraftGatherSitesTried, site.GUID)
					current.Phase = "gather_travel"
					current.OperationID = a.sendPrimitive("navigate_to_position", map[string]any{"map_id": site.MapID, "x": site.Position[0], "y": site.Position[1], "z": site.Position[2]})
					current.LastProgressUTC = time.Now().UTC()
					a.persist()
					return
				}
			}
			if target == "" || len(current.CraftGatherTried) >= maxCraftingCasts {
				a.finishTask("blocked", "observed gathering sources exhausted before material goal")
				return
			}
			current.CraftGatherBefore = quantity
			current.CraftGatherTried = append(current.CraftGatherTried, target)
			current.Phase = "gather_material"
			current.OperationID = a.sendPrimitive("gather_target", map[string]any{"target_guid": target})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
		if len(current.CraftSupplies) > maxCraftingCasts || supply.NPCGUID == "" || supply.ItemID == 0 || supply.Bundles == 0 || supply.Bundles > maxCraftingCasts {
			a.finishTask("blocked", "invalid persisted material procurement")
			return
		}
		current.Phase = "procure"
		current.OperationID = a.sendPrimitive("buy_vendor_item", map[string]any{
			"target_guid": supply.NPCGUID, "item_id": supply.ItemID, "count": supply.Bundles, "max_spend_copper": supply.Budget,
		})
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if quantity := stock[current.CraftItemID]; quantity >= current.CraftInitialCount &&
		uint64(quantity)-uint64(current.CraftInitialCount) >= uint64(current.GoalCount) {
		current.Completed = current.GoalCount
		if current.Kind == "auction_purchase" {
			a.finishTask("completed", "auction purchase and mailbox receipt verified in fresh inventory")
		} else {
			a.finishTask("completed", "crafted output goal verified in fresh inventory")
		}
		return
	}
	if current.CraftStepIndex >= uint32(len(current.CraftSteps)) {
		a.finishTask("blocked", "crafting plan ended without the requested inventory increase")
		return
	}
	step := current.CraftSteps[current.CraftStepIndex]
	totalCasts := uint64(0)
	for _, planned := range current.CraftSteps {
		totalCasts += uint64(planned.Casts)
	}
	if len(current.CraftSteps) > maxCraftingCasts || totalCasts > maxCraftingCasts {
		a.finishTask("blocked", "persisted crafting plan exceeds its work budget")
		return
	}
	if step.SpellID == 0 || step.ItemID == 0 || step.Casts == 0 || step.Casts > maxCraftingCasts {
		a.finishTask("blocked", "invalid persisted crafting step")
		return
	}
	current.Phase = "casting"
	current.OperationID = a.sendPrimitive("craft_once", map[string]any{"spell_id": step.SpellID, "item_id": step.ItemID})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleCraftingOperation(status, reason string, persistent bool) {
	if current := a.state.Task; current.Phase == "trade_meet" || current.Phase == "trade_control" {
		a.handleCraftingTradeControl(status, reason)
		return
	}
	current := a.state.Task
	if status == "accepted" && persistent {
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if current.Phase == "trade_material" {
		if status != "completed" || reason != "trade material transfer verified" {
			a.finishTask("blocked", "material trade not verified; inspect inventory before any retry")
			return
		}
		current.OperationID = ""
		current.CraftTradeReceived = true
		a.requestCraftingObservation(current)
		return
	}
	if current.Phase == "gather_travel" {
		if status != "completed" || reason != "observed position reached" {
			a.finishTask("blocked", "known gathering site could not be reached")
			return
		}
		current.OperationID = ""
		a.requestCraftingObservation(current)
		return
	}
	if current.Phase == "auction_buy" {
		if status != "completed" || reason != "auction buyout submitted; collect won mail" || current.CraftAuctionReceipt == "" {
			a.finishTask("blocked", "auction buyout receipt unavailable or uncertain; do not repurchase")
			return
		}
		current.OperationID = ""
		current.Phase = "select"
		a.persist()
		a.advanceCraftingTask(current)
		return
	}
	if current.Phase == "auction_collect" {
		if status != "completed" || reason != "bounded mailbox collection completed" {
			a.finishTask("blocked", "purchased auction mail not collected; inspect mailbox before retrying")
			return
		}
		current.OperationID = ""
		current.CraftAuctionCollect = true
		a.requestCraftingObservation(current)
		return
	}
	if current.Phase == "gather_material" {
		if status != "completed" || (reason != "loot or gather completed" && reason != "operation target disappeared") {
			a.finishTask("blocked", "material gathering was not verified: "+reason)
			return
		}
		current.OperationID = ""
		current.CraftGatherAwaiting = true
		a.requestCraftingObservation(current)
		return
	}
	if current.Phase == "procure" {
		if status != "completed" || reason != "bounded NPC service batch completed" {
			a.finishTask("blocked", "material purchase was not verified: "+reason)
			return
		}
		current.OperationID = ""
		current.CraftSupplyIndex++
		a.requestCraftingObservation(current)
		return
	}
	if status != "completed" || reason != "crafted output acquired" {
		a.finishTask("blocked", "crafting child did not verify its output: "+reason)
		return
	}
	current.OperationID = ""
	if current.CraftStepIndex >= uint32(len(current.CraftSteps)) {
		a.finishTask("blocked", "unexpected crafting child result")
		return
	}
	current.CraftStepCasts++
	if current.CraftStepCasts >= current.CraftSteps[current.CraftStepIndex].Casts {
		current.CraftStepIndex++
		current.CraftStepCasts = 0
	}
	a.requestCraftingObservation(current)
}
