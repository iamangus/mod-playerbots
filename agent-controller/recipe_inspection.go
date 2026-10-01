package main

import "encoding/json"

const maxRecipeInspectionOffset = 4096

type recipeInspection struct {
	Offset uint32 `json:"offset"`
	ItemID uint32 `json:"item_id"`
}

type craftRecipePage struct {
	Offset                uint32  `json:"offset"`
	Total                 uint32  `json:"total"`
	ItemID                uint32  `json:"item_id"`
	NextOffset            *uint32 `json:"next_offset"`
	DependenciesTruncated bool    `json:"dependencies_truncated"`
}

func (a *actor) inspectCraftingRecipes(args map[string]json.RawMessage) {
	var query recipeInspection
	if raw, supplied := args["offset"]; supplied && (string(raw) == "null" || json.Unmarshal(raw, &query.Offset) != nil || query.Offset > maxRecipeInspectionOffset) {
		a.rejectTool("inspect_crafting_recipes", "offset must be an integer between 0 and 4096")
		return
	}
	if raw, supplied := args["item_id"]; supplied && (string(raw) == "null" || json.Unmarshal(raw, &query.ItemID) != nil || query.ItemID == 0) {
		a.rejectTool("inspect_crafting_recipes", "item_id must identify the desired recipe output")
		return
	}
	if query.ItemID != 0 && query.Offset != 0 {
		a.rejectTool("inspect_crafting_recipes", "choose either a recipe page offset or an output item_id")
		return
	}
	if !a.bridgeActive {
		a.rejectTool("inspect_crafting_recipes", "native bridge is inactive")
		return
	}
	// Persist the query, not a stale recipe cache. Requested snapshots revalidate
	// learned recipes each time; the current task and its private gate are untouched.
	a.state.RecipeInspection = &query
	a.pendingDecisionReason = "recipe_inspection"
	a.pendingSnapshotID = ""
	a.persist()
	a.requestSnapshot()
}
