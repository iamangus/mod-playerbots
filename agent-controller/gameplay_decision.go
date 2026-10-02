package main

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Jev chooses both the high-level operation and its arguments. Arguments are
// selections over local, typed values, never generated JSON or writer output.
// Runtime task/native guards remain authoritative after selection.
type gameplayPlanner struct{ evaluator chatEvaluator }

type gameplayValue struct {
	Value  any    `json:"value"`
	Source string `json:"source"`
}

type gameplayTool struct {
	Name        string
	Description string
	Properties  map[string]any
	Required    map[string]bool
}

func gameplayTools() []gameplayTool {
	var result []gameplayTool
	for _, definition := range agentTools {
		f := definition["function"].(map[string]any)
		name := f["name"].(string)
		if name == "send_chat" { // communication has its own Jev battery
			continue
		}
		p := f["parameters"].(map[string]any)
		t := gameplayTool{Name: name, Description: f["description"].(string),
			Properties: p["properties"].(map[string]any), Required: map[string]bool{}}
		if required, ok := p["required"].([]string); ok {
			for _, key := range required {
				t.Required[key] = true
			}
		}
		result = append(result, t)
	}
	return result
}

func (p *gameplayPlanner) decide(job decisionJob) (*toolCall, error) {
	if p == nil || p.evaluator == nil {
		return nil, fmt.Errorf("Jev gameplay evaluator is unavailable")
	}
	observation := map[string]any{"schema_version": 1, "trigger": job.trigger,
		"state": job.state, "events": job.events, "current_task": job.task,
		"memory": job.memories, "profile": job.profile}
	criteria := map[string]any{"continue": "Keep the current lower-level loops running, or wait if idle. Do not replace useful work merely because a timer fired."}
	domains := map[string]map[string][]gameplayValue{}
	registry := map[string]gameplayTool{}
	for _, tool := range gameplayTools() {
		if job.state.Bot.Alive != nil && !*job.state.Bot.Alive && tool.Name != "recover_death" && tool.Name != "stop_task" && tool.Name != "remember" && tool.Name != "set_identity" {
			continue
		}
		d := gameplayDomains(tool, job)
		possible := true
		for key := range tool.Required {
			if len(d[key]) == 0 {
				possible = false
			}
		}
		if !possible {
			continue
		}
		domains[tool.Name], registry[tool.Name] = d, tool
		criteria[tool.Name] = map[string]any{"purpose": tool.Description, "arguments": d}
	}
	answers, err := p.evaluator.evaluate(observation, map[string]typesafeQuestion{"action": {
		Type: "choice", Instructions: map[string]any{"question": "Which ONE high-level gameplay operation should this character perform now?",
			"rules": systemPrompt(job.profile, true) + " Choose continue when the current task is appropriate. Choose only an offered operation. Communication is independently decided by Jev; no OpenRouter gameplay fallback exists."}, Criteria: criteria}})
	if err != nil {
		return nil, err
	}
	a, ok := answers["action"]
	// Choice confidence is relative to the offered alternatives. Applying the
	// chat-recipient 0.6 floor to 40 legal quantity/tool alternatives rejects
	// valid winners. Exact allowlists and runtime effect guards provide safety;
	// malformed/zero/non-finite confidence remains a protocol failure.
	if !ok || a.Type != "choice" || math.IsNaN(a.Confidence) || a.Confidence <= 0 || a.Confidence > 1 {
		return nil, fmt.Errorf("invalid or low-confidence Jev gameplay action: type=%q choice=%q confidence=%g", a.Type, a.Choice, a.Confidence)
	}
	if a.Choice == "continue" {
		return nil, nil
	}
	tool, ok := registry[a.Choice]
	if !ok {
		return nil, fmt.Errorf("Jev selected unknown/unavailable gameplay action %q", a.Choice)
	}
	observation["selected_action"] = tool.Name
	d := domains[tool.Name]
	questions := map[string]typesafeQuestion{}
	keys := make([]string, 0, len(tool.Properties))
	for key := range tool.Properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(d[key]) == 0 {
			continue
		}
		options := map[string]any{}
		if !tool.Required[key] {
			options["omit"] = "Do not supply this optional argument."
		}
		for i, value := range d[key] {
			options[fmt.Sprintf("v%d", i)] = value
		}
		questions[key] = typesafeQuestion{Type: "choice", Instructions: map[string]any{
			"question": "Select the exact " + key + " argument for " + tool.Name + ".",
			"rules":    "Use state, recent events and the current task. Select only an offered typed value. Do not invent identity, money, inventory or success. Spending is a deliberate budget, not a market-value estimate. Optional arguments may be omitted.",
			"schema":   tool.Properties[key]}, Criteria: options}
	}
	args := map[string]any{}
	if len(questions) > 0 {
		answers, err = p.evaluator.evaluate(observation, questions)
		if err != nil {
			return nil, err
		}
		for key := range questions {
			a, ok := answers[key]
			if !ok || a.Type != "choice" || math.IsNaN(a.Confidence) || a.Confidence <= 0 || a.Confidence > 1 {
				return nil, fmt.Errorf("invalid Jev argument %s", key)
			}
			if a.Choice == "omit" && !tool.Required[key] {
				continue
			}
			index, parseErr := strconv.Atoi(strings.TrimPrefix(a.Choice, "v"))
			if parseErr != nil || a.Choice != fmt.Sprintf("v%d", index) || index < 0 || index >= len(d[key]) {
				return nil, fmt.Errorf("unoffered Jev argument %s", key)
			}
			args[key] = d[key][index].Value
		}
	}
	for key := range tool.Required {
		if _, ok := args[key]; !ok {
			return nil, fmt.Errorf("missing required Jev argument %s", key)
		}
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return &toolCall{Name: tool.Name, Arguments: string(encoded)}, nil
}

var gameplayNumber = regexp.MustCompile(`\b[0-9]{1,10}\b`)

// Domains expose parameter families for EVERY registered gameplay operation.
// No policy silently chooses a quest, player or task: Jev picks the operation
// and all supplied parameters. Candidates disclose their evidence source.
func gameplayDomains(tool gameplayTool, job decisionJob) map[string][]gameplayValue {
	s := job.state
	d := map[string][]gameplayValue{}
	add := func(key string, value any, source string) {
		if _, exists := tool.Properties[key]; !exists {
			return
		}
		d[key] = append(d[key], gameplayValue{value, source})
	}
	for _, q := range s.Quests {
		if tool.Name != "accept_quest" {
			add("quest_id", q.QuestID, q.Title)
		}
	}
	if tool.Name == "accept_quest" {
		for _, q := range s.AvailableQuests {
			add("quest_id", q.QuestID, q.Title+" offered by "+q.GiverName)
		}
	}
	for _, creature := range s.NearbyCreatures {
		add("creature_entry", creature.Entry, creature.Name)
	}
	for _, player := range s.NearbyPlayers {
		add("player_name", player.Name, "nearby player")
		add("recipient", player.Name, "nearby player")
	}
	for _, member := range s.Bot.GroupMembers {
		add("player_name", member.Name, "group member")
		add("recipient", member.Name, "group member")
	}
	for _, mailbox := range s.NearbyMailboxes {
		add("mailbox_guid", mailbox.GUID, mailbox.Name)
	}
	for _, npc := range s.NearbyNPCs {
		add("npc_guid", npc.GUID, npc.Name)
		for _, route := range npc.TaxiRoutes {
			add("destination_node", route.ToNode, route.Name)
			add("max_spend_copper", route.PriceCopper, "observed taxi fare")
		}
		for _, offer := range npc.VendorOffers {
			add("item_id", offer.ItemID, offer.Name)
			add("max_spend_copper", offer.PriceCopper, "observed vendor bundle price")
		}
	}
	if s.TravelTarget != nil {
		add("destination", s.TravelTarget.DestinationName, "native travel destination")
	}
	if job.task != nil {
		add("destination", job.task.TargetName, "current task target")
	}
	for _, recipe := range s.CraftingRecipes {
		add("item_id", recipe.ItemID, "known recipe")
		for _, reagent := range recipe.Reagents {
			add("item_id", reagent.ItemID, "known recipe reagent")
		}
	}
	for _, item := range s.Inventory.Items {
		add("item_id", item.ItemID, item.Name)
	}
	for _, item := range s.CraftingInventory {
		add("item_id", item.ItemID, "crafting inventory")
	}
	if s.CraftingRecipePage != nil {
		add("offset", s.CraftingRecipePage.NextOffset, "next native recipe page")
	}
	if s.Auctions != nil {
		add("cursor", s.Auctions.NextCursor, "next native auction page")
		for _, offer := range s.Auctions.Offers {
			add("auction_id", offer.AuctionID, "observed auction")
			add("item_id", offer.ItemID, "observed auction item")
			add("bid_copper", offer.MinimumBid, "observed minimum bid")
			add("max_spend_copper", offer.Buyout, "observed buyout")
		}
	}
	for _, bid := range s.PendingAuctionBids {
		add("auction_id", bid.AuctionID, "persisted pending bid")
	}
	var trade map[string]any
	_ = json.Unmarshal(s.Trade, &trade)
	if revision, ok := trade["revision"]; ok {
		add("trade_revision", revision, "observed trade revision")
	}
	var stacks []map[string]any
	_ = json.Unmarshal(s.Inventory.TradeableStacks, &stacks)
	for _, stack := range stacks {
		if guid, ok := stack["item_guid"]; ok {
			add("item_guid", guid, "observed tradeable stack")
		}
	}
	add("offset", 0, "first recipe page")
	add("cursor", 0, "first auction page")
	for i := 1; i <= 40; i++ {
		add("count", i, "bounded work quantity")
	}
	for _, radius := range []int{5, 10, 20, 30, 50, 75, 100} {
		add("radius", radius, "bounded local search radius")
	}
	for i := 0; i <= 5; i++ {
		add("trade_slot", i, "ordinary trade slot")
	}
	for _, amount := range []uint32{0, 30, 100, 500, 1000, 10000, s.Inventory.MoneyCopper / 10, s.Inventory.MoneyCopper / 2, s.Inventory.MoneyCopper} {
		if amount <= s.Inventory.MoneyCopper {
			for _, key := range []string{"max_spend_copper", "money_copper", "price_copper", "bid_copper"} {
				add(key, amount, "explicit bounded spending option; not a valuation")
			}
		}
	}
	add("reason", "Choose different work", "cancel current task")
	add("message", "Ready to trade the agreed materials at the agreed price. Please open trade.", "negotiation communication template")
	add("trade_request_message", "Looking for materials for crafting. Please whisper me if you can supply them.", "seller-discovery communication template")
	add("subject", "Requested items", "mail subject template")
	add("body", "Here are the requested items.", "mail body template")
	add("profile", job.profile, "existing stable identity")
	add("profile", deterministicProfile(s.Bot.RaceID, s.Bot.ClassID, "dps", "persistent character"), "stable identity template")
	for _, memory := range job.memories {
		add("memory", memory, "existing memory")
	}
	for _, e := range job.events {
		if e.Type != "chat_received" {
			continue
		}
		var chat struct {
			Sender  string `json:"sender_name"`
			Message string `json:"message"`
		}
		if json.Unmarshal(e.Payload, &chat) != nil {
			continue
		}
		add("player_name", chat.Sender, "recent chat sender")
		add("recipient", chat.Sender, "recent chat sender")
		add("memory", chat.Sender+" said: "+chat.Message, "observed statement, not verified truth")
		add("reason", chat.Message, "recent chat")
		add("subject", chat.Message, "explicit text from recent chat")
		add("body", chat.Message, "explicit text from recent chat")
		for _, digits := range gameplayNumber.FindAllString(chat.Message, -1) {
			amount, err := strconv.ParseUint(digits, 10, 32)
			if err == nil && amount <= uint64(s.Inventory.MoneyCopper) {
				for _, key := range []string{"max_spend_copper", "money_copper", "price_copper", "bid_copper"} {
					add(key, uint32(amount), "number explicitly present in recent chat; select only if it denotes copper")
				}
			}
		}
		// Exact bounded phrases from player chat let Jev select semantic
		// destinations without generating names. Native resolution proves validity.
		words := strings.Fields(chat.Message)
		for start := 0; start < len(words); start++ {
			for end := start + 1; end <= len(words) && end <= start+5; end++ {
				add("destination", strings.Trim(strings.Join(words[start:end], " "), `"'.,!?`), "phrase from recent chat; native destination resolution required")
			}
		}
	}
	for key, schemaRaw := range tool.Properties {
		schema := schemaRaw.(map[string]any)
		if choices, ok := schema["enum"].([]string); ok {
			for _, choice := range choices {
				add(key, choice, "tool-defined option")
			}
		}
	}
	// Multi-material search uses an ordered, deduplicated observed item batch;
	// individual items are also offered, allowing a focused search.
	if _, ok := tool.Properties["item_ids"]; ok {
		itemsTool := tool
		itemsTool.Properties = map[string]any{"item_id": map[string]any{"type": "integer", "minimum": 1}}
		items := gameplayDomains(itemsTool, job)["item_id"]
		var batch []any
		for _, item := range items {
			add("item_ids", []any{item.Value}, item.Source)
			if len(batch) < 8 {
				batch = append(batch, item.Value)
			}
		}
		if len(batch) > 1 {
			add("item_ids", batch, "bounded observed multi-material batch")
		}
	}
	for key, values := range d {
		schema := tool.Properties[key].(map[string]any)
		seen := map[string]bool{}
		kept := []gameplayValue{}
		for _, candidate := range values {
			if !gameplayValueValid(candidate.Value, schema) {
				continue
			}
			encoded, _ := json.Marshal(candidate.Value)
			if strings.HasSuffix(key, "_copper") {
				var amount float64
				if json.Unmarshal(encoded, &amount) != nil || amount > float64(s.Inventory.MoneyCopper) {
					continue
				}
			}
			if !seen[string(encoded)] {
				seen[string(encoded)] = true
				kept = append(kept, candidate)
			}
		}
		// Source projections are bounded just like native observations. Expose
		// truncation in the source rather than pretending the list is complete.
		if len(kept) > 128 {
			kept = kept[:128]
			for i := range kept {
				kept[i].Source += " (partial candidate projection)"
			}
		}
		d[key] = kept
	}
	return d
}

func gameplayValueValid(value any, schema map[string]any) bool {
	encoded, err := json.Marshal(value)
	if err != nil {
		return false
	}
	var decoded any
	if json.Unmarshal(encoded, &decoded) != nil {
		return false
	}
	switch schema["type"] {
	case "integer", "number":
		n, ok := decoded.(float64)
		if !ok || math.IsNaN(n) || math.IsInf(n, 0) || (schema["type"] == "integer" && math.Trunc(n) != n) {
			return false
		}
		for key, bound := range schema {
			b, ok := bound.(int)
			if ok && ((key == "minimum" && n < float64(b)) || (key == "maximum" && n > float64(b))) {
				return false
			}
		}
	case "string":
		s, ok := decoded.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return false
		}
		if n, ok := schema["maxLength"].(int); ok && len([]rune(s)) > n {
			return false
		}
	case "array":
		a, ok := decoded.([]any)
		if !ok || len(a) == 0 || len(a) > 8 {
			return false
		}
		itemSchema := schema["items"].(map[string]any)
		for _, item := range a {
			if !gameplayValueValid(item, itemSchema) {
				return false
			}
		}
	default:
		return false
	}
	return true
}
