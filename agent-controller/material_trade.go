package main

import (
	"encoding/json"
	"fmt"
	"sort"
)

type materialTradeOffer struct {
	Money    uint32 `json:"money_copper"`
	SpellID  uint32 `json:"spell_id"`
	Accepted bool   `json:"accepted"`
	Items    []struct {
		Slot   uint32 `json:"slot"`
		ItemID uint32 `json:"item_id"`
		Count  uint32 `json:"count"`
	} `json:"items"`
}

type materialTrade struct {
	Active      bool               `json:"active"`
	WindowOpen  bool               `json:"window_open"`
	Revision    uint64             `json:"revision"`
	PartnerGUID string             `json:"partner_guid"`
	Ours        materialTradeOffer `json:"bot_offer"`
	Theirs      materialTradeOffer `json:"partner_offer"`
}

func (a *actor) tradeMaterialSupply(material craftReagent, stock, budget uint32) (craftSupply, error) {
	return a.tradeMaterialBatch([]craftReagent{material}, map[uint32]uint32{material.ItemID: stock}, budget)
}

func (a *actor) tradeMaterialBatch(materials []craftReagent, stock map[uint32]uint32, budget uint32) (craftSupply, error) {
	var trade materialTrade
	if json.Unmarshal(a.latest.Trade, &trade) != nil || !trade.Active || !trade.WindowOpen || trade.PartnerGUID == "" ||
		!trade.Theirs.Accepted || trade.Ours.Money > budget || len(trade.Ours.Items) != 0 || trade.Ours.SpellID != 0 || trade.Theirs.SpellID != 0 || trade.Theirs.Money != 0 || len(trade.Theirs.Items) > 6 {
		return craftSupply{}, fmt.Errorf("already negotiated, partner-accepted material trade required")
	}
	if len(materials) == 0 || len(materials) > 6 {
		return craftSupply{}, fmt.Errorf("trade material types exceed six slots")
	}
	wanted := make(map[uint32]uint32)
	for _, material := range materials {
		if material.ItemID == 0 || material.Count == 0 || wanted[material.ItemID] != 0 {
			return craftSupply{}, fmt.Errorf("invalid trade material goal")
		}
		wanted[material.ItemID] = material.Count
	}
	counts := make(map[uint32]uint64)
	total := uint64(0)
	seen := make(map[uint32]bool)
	for _, item := range trade.Theirs.Items {
		if item.Slot >= 6 || seen[item.Slot] || wanted[item.ItemID] == 0 || item.Count == 0 {
			return craftSupply{}, fmt.Errorf("trade must contain only requested material stacks")
		}
		seen[item.Slot] = true
		counts[item.ItemID] += uint64(item.Count)
		total += uint64(item.Count)
	}
	if total == 0 || total > 40 {
		return craftSupply{}, fmt.Errorf("trade quantity outside bounded material goal")
	}
	supply := craftSupply{Bundles: uint32(total), Budget: trade.Ours.Money, TradePartnerGUID: trade.PartnerGUID, TradeRevision: trade.Revision}
	for itemID, goal := range wanted {
		quantity := counts[itemID]
		if quantity < uint64(goal) || uint64(stock[itemID])+quantity > uint64(^uint32(0)) {
			return craftSupply{}, fmt.Errorf("trade does not fill every bounded material goal")
		}
		supply.TradeMaterials = append(supply.TradeMaterials, craftReagent{ItemID: itemID, Count: uint32(quantity)})
		supply.RequiredMaterials = append(supply.RequiredMaterials, craftReagent{ItemID: itemID, Count: uint32(uint64(stock[itemID]) + quantity)})
	}
	sort.Slice(supply.TradeMaterials, func(i, j int) bool { return supply.TradeMaterials[i].ItemID < supply.TradeMaterials[j].ItemID })
	sort.Slice(supply.RequiredMaterials, func(i, j int) bool { return supply.RequiredMaterials[i].ItemID < supply.RequiredMaterials[j].ItemID })
	supply.ItemID, supply.RequiredCount = supply.TradeMaterials[0].ItemID, supply.RequiredMaterials[0].Count
	return supply, nil
}

func materialTradeTask(current *task) bool {
	return current != nil && current.Kind == "crafting" && ((current.CraftTradeStage != "" && current.CraftTradeStage != "acquire") ||
		(current.CraftSupplyIndex < uint32(len(current.CraftSupplies)) && current.CraftSupplies[current.CraftSupplyIndex].TradePartnerGUID != ""))
}
