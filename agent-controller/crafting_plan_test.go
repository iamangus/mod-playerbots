package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCraftRecipeObservationsSurviveSnapshotRoundTrip(t *testing.T) {
	var state snapshot
	if err := json.Unmarshal([]byte(`{"crafting_recipes":[{"spell_id":100,"item_id":1,"name":"Crafted output","yield":2,"reagents":[{"item_id":2,"count":3}]}]}`), &state); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored snapshot
	if err := json.Unmarshal(encoded, &restored); err != nil || !reflect.DeepEqual(state.CraftingRecipes, restored.CraftingRecipes) || len(restored.CraftingRecipes) != 1 {
		t.Fatal("recipe output/reagent observations were lost before model input")
	}
	plan, err := planCrafting(1, 3, nil, restored.CraftingRecipes)
	if err != nil || plan.Steps[0].Casts != 2 || plan.Missing[0].Count != 6 {
		t.Fatalf("observed recipe cannot drive material planning: %+v, %v", plan, err)
	}
}

func TestCraftPlanOrdersDependenciesAndKeepsBatchSurplus(t *testing.T) {
	recipes := []craftRecipeInfo{
		{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: 3}}},
		{SpellID: 200, ItemID: 2, Yield: 2, Reagents: []craftReagent{{ItemID: 3, Count: 1}}},
	}
	stock := map[uint32]uint32{1: 10, 2: 1, 3: 2}
	plan, err := planCrafting(1, 2, stock, recipes)
	if err != nil || len(plan.Missing) != 1 || plan.Missing[0] != (craftReagent{ItemID: 3, Count: 1}) {
		t.Fatalf("wrong material deficit: %+v, %v", plan, err)
	}
	expected := []craftingStep{{SpellID: 200, ItemID: 2, Casts: 3}, {SpellID: 100, ItemID: 1, Casts: 2}}
	if !reflect.DeepEqual(plan.Steps, expected) || stock[1] != 10 || stock[2] != 1 || stock[3] != 2 {
		t.Fatalf("bad dependency order or mutated input stock: %+v, %v", plan, stock)
	}
}

func TestCraftPlanSharedMaterialsAreConsumedOnlyOnce(t *testing.T) {
	recipes := []craftRecipeInfo{
		{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: 1}, {ItemID: 3, Count: 1}}},
		{SpellID: 200, ItemID: 2, Yield: 1, Reagents: []craftReagent{{ItemID: 4, Count: 2}}},
		{SpellID: 300, ItemID: 3, Yield: 1, Reagents: []craftReagent{{ItemID: 4, Count: 2}}},
	}
	plan, err := planCrafting(1, 1, map[uint32]uint32{4: 3}, recipes)
	if err != nil || !reflect.DeepEqual(plan.Missing, []craftReagent{{ItemID: 4, Count: 1}}) || len(plan.Steps) != 3 {
		t.Fatalf("shared reagent stock was reused: %+v, %v", plan, err)
	}
}

func TestCraftPlanBoundsCyclesDepthAndCastCount(t *testing.T) {
	cycle := []craftRecipeInfo{
		{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: 1}}},
		{SpellID: 200, ItemID: 2, Yield: 1, Reagents: []craftReagent{{ItemID: 1, Count: 1}}},
	}
	if plan, err := planCrafting(1, 1, nil, cycle); err == nil || len(plan.Steps) != 0 {
		t.Fatal("cyclic recipe graph produced a runnable partial plan")
	}
	if _, err := planCrafting(1, 1, map[uint32]uint32{1: 1}, cycle); err == nil {
		t.Fatal("seed inventory hid a zero-net-output recipe cycle")
	}
	var deep []craftRecipeInfo
	for item := uint32(1); item <= maxCraftingDepth+1; item++ {
		deep = append(deep, craftRecipeInfo{SpellID: item, ItemID: item, Yield: 1, Reagents: []craftReagent{{ItemID: item + 1, Count: 1}}})
	}
	if _, err := planCrafting(1, 1, nil, deep); err == nil {
		t.Fatal("unbounded dependency depth")
	}
	large := []craftRecipeInfo{
		{SpellID: 1, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: 40}}},
		{SpellID: 2, ItemID: 2, Yield: 1},
	}
	if _, err := planCrafting(1, 1, nil, large); err == nil {
		t.Fatal("dependency work exceeded the total cast budget")
	}
}

func TestCraftPlanRejectsInvalidGoalsRecipesAndOverflow(t *testing.T) {
	for _, goal := range []uint32{0, 41} {
		if _, err := planCrafting(1, goal, nil, nil); err == nil {
			t.Fatal("invalid output goal accepted")
		}
	}
	for _, recipe := range []craftRecipeInfo{
		{SpellID: 1, ItemID: 1},
		{SpellID: 1, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2}}},
		{SpellID: 1, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 2, Count: ^uint32(0)}}},
	} {
		if _, err := planCrafting(1, 2, nil, []craftRecipeInfo{recipe}); err == nil {
			t.Fatal("invalid recipe or material overflow accepted")
		}
	}
}

func TestCraftPlanRecipeChoiceAndMissingOrderAreDeterministic(t *testing.T) {
	recipes := []craftRecipeInfo{
		{SpellID: 200, ItemID: 1, Yield: 1},
		{SpellID: 100, ItemID: 1, Yield: 1, Reagents: []craftReagent{{ItemID: 3, Count: 1}, {ItemID: 2, Count: 1}}},
	}
	plan, err := planCrafting(1, 1, nil, recipes)
	if err != nil || plan.Steps[0].SpellID != 100 || !reflect.DeepEqual(plan.Missing, []craftReagent{{ItemID: 2, Count: 1}, {ItemID: 3, Count: 1}}) {
		t.Fatalf("nondeterministic recipe/material plan: %+v, %v", plan, err)
	}
}
