package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestChatContextRelevanceBoundsAndNoStateMutation(t *testing.T) {
	a, _ := chatTestSetup(t)
	var items []map[string]any
	var recipes []map[string]any
	var quests []map[string]any
	for i := 0; i < 30; i++ {
		items = append(items, map[string]any{"item_id": i + 1, "count": 100 - i, "name": fmt.Sprintf("普通 Linen %02d", i)})
		quests = append(quests, map[string]any{"quest_id": i + 1, "title": fmt.Sprintf("Long unrelated quest title %02d", i)})
		recipes = append(recipes, map[string]any{"name": fmt.Sprintf("Recipe %02d", i), "yield": 1})
	}
	items = append(items, map[string]any{"item_id": 99, "count": 1, "name": "Malachite"})
	recipes = append(recipes, map[string]any{"name": "Malachite Pendant", "yield": 2,
		"reagents": []map[string]any{{"item_id": 99, "name": "Malachite", "count": 3}}})
	quests = append(quests, map[string]any{"quest_id": 99, "title": "Malachite Delivery"})
	if err := json.Unmarshal(mustJSON(map[string]any{"inventory": map[string]any{"items": items},
		"crafting_recipes": recipes, "quests": quests}), &a.latest); err != nil {
		t.Fatal(err)
	}
	before := string(mustJSON(a.latest))
	context := a.buildChatContext("Do you have Malachite for a pendant?")
	for name, group := range map[string]string{"possessions": context.Possessions, "abilities": context.Abilities, "quests": context.Quests} {
		if !strings.Contains(group, "Malachite") || !strings.Contains(group, "partial") || len(group) > chatContextSectionBytes || !utf8.ValidString(group) {
			t.Fatalf("%s relevance or bound failed: %q", name, group)
		}
	}
	if !strings.Contains(context.Abilities, "yields 2, Malachite x3") {
		t.Fatalf("recipe details lost: %s", context.Abilities)
	}
	if string(mustJSON(a.latest)) != before {
		t.Fatal("chat context mutated gameplay snapshot")
	}
	if len(a.latest.Quests) != 31 {
		t.Fatal("quest log truncated in place")
	}
	for _, group := range []string{context.Character, context.Activity, context.Possessions, context.Abilities,
		context.Quests, context.Surroundings, context.ActivityDetail, context.Social, context.Economy} {
		if len(group) > chatContextSectionBytes {
			t.Fatal("writer context budget exceeded")
		}
	}
}

func TestChatContextNativeAvailabilityAndSurroundings(t *testing.T) {
	a, _ := chatTestSetup(t)
	if err := json.Unmarshal([]byte(`{"bot":{"area_name":"Northshire Abbey","zone_name":"Elwynn Forest"},
		"known_abilities":["Lesser Heal","Smite"],"known_abilities_total":2,
		"bag_slots_free":0,"unread_mail_count":0,"mail_count":0,
		"crafting_recipes":[{"name":"Copper Chain","yield":1,"reagents":[{"item_id":2840,"name":"Copper Bar","count":2}]}],
		"available_quests":[{"title":"Kobold Cleanup","giver_name":"Marshal McBride"}],
		"nearby_npcs":[{"name":"Far NPC","distance":90},{"name":"Near NPC","distance":2}],
		"nearby_players":[{"name":"Peepee","distance":4}]}`), &a.latest); err != nil {
		t.Fatal(err)
	}
	context := a.buildChatContext()
	if !strings.Contains(context.Possessions, "0 free bag slots") || !strings.Contains(context.Economy, "0 unread letters") {
		t.Fatal("known zero counts lost")
	}
	if !strings.Contains(context.Abilities, "known spell Lesser Heal") || !strings.Contains(context.Abilities, "Copper Bar x2") {
		t.Fatalf("native ability/recipe facts missing: %s", context.Abilities)
	}
	if !strings.Contains(context.Quests, "available quest Kobold Cleanup from Marshal McBride") {
		t.Fatal("available quests missing")
	}
	if !strings.Contains(context.Surroundings, "Northshire Abbey, Elwynn Forest") || !strings.Contains(context.Surroundings, "player Peepee") ||
		strings.Index(context.Surroundings, "Near NPC") > strings.Index(context.Surroundings, "Far NPC") {
		t.Fatal("location or nearest ordering wrong")
	}
	a.latest.MailCount, a.latest.UnreadMailCount = nil, nil
	if got := a.buildChatContext().Economy; !strings.Contains(got, "mail counts unavailable") || strings.Contains(got, "0 unread") {
		t.Fatalf("unknown mail became zero: %s", got)
	}
}

func TestChatAuctionFactsFreshnessAndReceiptUncertainty(t *testing.T) {
	now := time.Now()
	state := persistedAgent{AuctionCatalog: map[uint32]*auctionObservation{
		1: {MapID: 0, ItemID: 1, ObservedAt: now.UnixMilli(), More: true,
			Offers: []auctionOffer{{ItemID: 1, Count: 3, Buyout: 12, ExpiresAt: now.Add(time.Minute).Unix()}}},
		2: {MapID: 0, ItemID: 2, ObservedAt: now.Add(-time.Hour).UnixMilli(), Offers: []auctionOffer{{ItemID: 2}}},
		3: {MapID: 1, ItemID: 3, ObservedAt: now.UnixMilli(), Offers: []auctionOffer{{ItemID: 3}}},
		4: {MapID: 0, ItemID: 4, ObservedAt: now.Add(time.Minute).UnixMilli(), Offers: []auctionOffer{{ItemID: 4}}},
	}, PendingAuctionBids: map[uint32]*auctionBid{9: {ItemID: 1, Status: "receipt_uncertain", Price: 5}}}
	got := strings.Join(auctionChatParts(state, snapshot{}, now), "; ")
	if !strings.Contains(got, "item 1 x3") || !strings.Contains(got, "partial catalog") || !strings.Contains(got, "receipt_uncertain") ||
		!strings.Contains(got, "escrow verified=false") {
		t.Fatalf("auction facts missing: %s", got)
	}
	for _, invalid := range []string{"item 2", "item 3", "item 4"} {
		if strings.Contains(got, invalid) {
			t.Fatalf("invalid observation leaked: %s", got)
		}
	}
}
