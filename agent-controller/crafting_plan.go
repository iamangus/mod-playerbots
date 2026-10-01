package main

import (
	"fmt"
	"sort"
)

const (
	maxCraftingDepth = 8
	maxCraftingCasts = 40
)

type craftReagent struct {
	ItemID uint32 `json:"item_id"`
	Count  uint32 `json:"count"`
}

type craftRecipeInfo struct {
	SpellID  uint32         `json:"spell_id"`
	ItemID   uint32         `json:"item_id"`
	Name     string         `json:"name"`
	Yield    uint32         `json:"yield"`
	Reagents []craftReagent `json:"reagents"`
}

type craftingStep struct {
	SpellID uint32 `json:"spell_id"`
	ItemID  uint32 `json:"item_id"`
	Casts   uint32 `json:"casts"`
}

type craftingPlan struct {
	Steps   []craftingStep
	Missing []craftReagent
}

// planCrafting models the material dependency tree without touching the game.
// additional is newly produced output, not an absolute inventory target. Stock
// is consumed once across shared dependencies, and surplus subcraft output is
// retained. The executor must still revalidate real inventory before each cast.
func planCrafting(itemID, additional uint32, stock map[uint32]uint32, recipes []craftRecipeInfo) (craftingPlan, error) {
	var plan craftingPlan
	if itemID == 0 || additional == 0 || additional > maxCraftingCasts {
		return plan, fmt.Errorf("crafting output goal must be between 1 and %d", maxCraftingCasts)
	}
	byItem := make(map[uint32]craftRecipeInfo)
	for _, recipe := range recipes {
		if recipe.SpellID == 0 || recipe.ItemID == 0 || recipe.Yield == 0 {
			return plan, fmt.Errorf("invalid recipe observation")
		}
		for _, reagent := range recipe.Reagents {
			if reagent.ItemID == 0 || reagent.Count == 0 {
				return plan, fmt.Errorf("invalid reagent observation")
			}
		}
		// Stable selection when two known recipes produce the same item. A later
		// policy may choose by cost; map iteration must never decide the recipe.
		if previous, exists := byItem[recipe.ItemID]; !exists || recipe.SpellID < previous.SpellID {
			byItem[recipe.ItemID] = recipe
		}
	}
	if _, known := byItem[itemID]; !known {
		return plan, fmt.Errorf("requested output has no observed recipe")
	}
	available := make(map[uint32]uint64, len(stock))
	for item, count := range stock {
		available[item] = uint64(count)
	}
	missing := make(map[uint32]uint64)
	visiting := make(map[uint32]bool)
	casts := uint64(0)
	var ensure func(uint32, uint64, int) error
	ensure = func(item uint32, needed uint64, depth int) error {
		if available[item] >= needed {
			return nil
		}
		deficit := needed - available[item]
		recipe, known := byItem[item]
		if !known {
			missing[item] += deficit
			if missing[item] > uint64(^uint32(0)) {
				return fmt.Errorf("material requirement overflow")
			}
			available[item] += deficit // Reserved external acquisition, not real inventory.
			return nil
		}
		if depth >= maxCraftingDepth || visiting[item] {
			return fmt.Errorf("cyclic or excessively deep crafting dependencies")
		}
		iterations := (deficit + uint64(recipe.Yield) - 1) / uint64(recipe.Yield)
		casts += iterations
		if casts > maxCraftingCasts {
			return fmt.Errorf("crafting dependency plan exceeds %d casts", maxCraftingCasts)
		}
		visiting[item] = true
		for _, reagent := range recipe.Reagents {
			quantity := uint64(reagent.Count) * iterations
			if err := ensure(reagent.ItemID, quantity, depth+1); err != nil {
				return err
			}
			available[reagent.ItemID] -= quantity
		}
		delete(visiting, item)
		plan.Steps = append(plan.Steps, craftingStep{SpellID: recipe.SpellID, ItemID: item, Casts: uint32(iterations)})
		available[item] += iterations * uint64(recipe.Yield)
		return nil
	}
	if err := ensure(itemID, uint64(stock[itemID])+uint64(additional), 0); err != nil {
		return craftingPlan{}, err // Never expose a partially validated plan.
	}
	if available[itemID] < uint64(stock[itemID])+uint64(additional) {
		return craftingPlan{}, fmt.Errorf("dependency plan consumes the requested output instead of increasing it")
	}
	for item, count := range missing {
		plan.Missing = append(plan.Missing, craftReagent{ItemID: item, Count: uint32(count)})
	}
	sort.Slice(plan.Missing, func(i, j int) bool { return plan.Missing[i].ItemID < plan.Missing[j].ItemID })
	return plan, nil
}
