package main

import (
	"encoding/json"
	"testing"
)

func TestRecipeInspectionPagesDoNotReplaceCurrentTask(t *testing.T) {
	a, commands := loopTestActor(t)
	current := &task{ID: "keep", Kind: "crafting", Phase: "observe", CraftSnapshotID: "private-observation"}
	a.state.Task = current
	a.pendingSnapshotID = "old-request"
	a.applyTool(toolCall{Name: "inspect_crafting_recipes", Arguments: `{"offset":40}`})
	cmd := nextLoopCommand(t, commands)
	var args map[string]uint32
	if json.Unmarshal(cmd.Arguments, &args) != nil || cmd.Operation != "snapshot" || args["recipe_offset"] != 40 {
		t.Fatalf("recipe page was not forwarded: %+v", cmd)
	}
	if a.state.Task != current || current.CraftSnapshotID != "private-observation" || current.Phase != "observe" || cmd.RequestID == "old-request" {
		t.Fatal("inspection replaced task execution or reused an obsolete ordinary request")
	}
	if a.pendingDecisionReason != "recipe_inspection" || a.pendingSnapshotID != cmd.RequestID {
		t.Fatal("inspection response cannot trigger fresh deliberation")
	}
}

func TestRecipeInspectionFocusPersistsAcrossRequestedSnapshots(t *testing.T) {
	a, commands := loopTestActor(t)
	a.applyTool(toolCall{Name: "inspect_crafting_recipes", Arguments: `{"item_id":90000}`})
	_ = nextLoopCommand(t, commands)
	data := mustJSON(a.state)
	var restored persistedAgent
	if json.Unmarshal(data, &restored) != nil || restored.RecipeInspection == nil || restored.RecipeInspection.ItemID != 90000 {
		t.Fatal("recipe query disappeared across persistence")
	}
	a.state = restored
	a.pendingSnapshotID = ""
	a.requestSnapshot()
	cmd := nextLoopCommand(t, commands)
	var args map[string]uint32
	if json.Unmarshal(cmd.Arguments, &args) != nil || args["recipe_item_id"] != 90000 || args["recipe_offset"] != 0 {
		t.Fatal("requested observations reverted to the first page instead of refreshing the query")
	}
}

func TestRecipeInspectionRejectsUnsafeRangesWithoutChangingQuery(t *testing.T) {
	for _, args := range []string{`{"offset":-1}`, `{"offset":4097}`, `{"offset":null}`, `{"offset":1.5}`,
		`{"item_id":0}`, `{"item_id":null}`, `{"item_id":-1}`, `{"offset":40,"item_id":1}`} {
		t.Run(args, func(t *testing.T) {
			a, _ := loopTestActor(t)
			previous := &recipeInspection{Offset: 80}
			a.state.RecipeInspection = previous
			a.applyTool(toolCall{Name: "inspect_crafting_recipes", Arguments: args})
			if a.state.RecipeInspection != previous {
				t.Fatal("invalid query displaced a valid inspection")
			}
		})
	}
}

func TestRecipeInspectionCanReturnToFirstGeneralPage(t *testing.T) {
	a, commands := loopTestActor(t)
	a.state.RecipeInspection = &recipeInspection{ItemID: 90000}
	a.applyTool(toolCall{Name: "inspect_crafting_recipes", Arguments: `{}`})
	_ = nextLoopCommand(t, commands)
	if a.state.RecipeInspection.ItemID != 0 || a.state.RecipeInspection.Offset != 0 {
		t.Fatal("empty inspection arguments did not clear the focused view")
	}
}

func TestRecipePageMetadataSurvivesModelInputRoundTrip(t *testing.T) {
	for _, input := range []string{
		`{"crafting_recipe_page":{"offset":40,"total":100,"item_id":0,"next_offset":80,"dependencies_truncated":false}}`,
		`{"crafting_recipe_page":{"offset":0,"total":100,"item_id":90000,"next_offset":null,"dependencies_truncated":true}}`,
	} {
		var state snapshot
		if json.Unmarshal([]byte(input), &state) != nil || state.CraftingRecipePage == nil {
			t.Fatal("recipe pagination metadata was discarded")
		}
		var restored snapshot
		if json.Unmarshal(mustJSON(state), &restored) != nil || restored.CraftingRecipePage == nil {
			t.Fatal("model round trip lost recipe page metadata")
		}
		page := restored.CraftingRecipePage
		if page.Total != 100 || (page.ItemID == 0 && (page.NextOffset == nil || *page.NextOffset != 80)) ||
			(page.ItemID != 0 && (page.NextOffset != nil || !page.DependenciesTruncated)) {
			t.Fatal("terminal page or truncated dependencies were misrepresented")
		}
	}
}

func TestRecipeInspectionRequiresAnActiveBridge(t *testing.T) {
	a, _ := loopTestActor(t)
	a.bridgeActive = false
	a.applyTool(toolCall{Name: "inspect_crafting_recipes", Arguments: `{}`})
	if a.state.RecipeInspection != nil {
		t.Fatal("inactive native bridge accepted an inspection")
	}
}
