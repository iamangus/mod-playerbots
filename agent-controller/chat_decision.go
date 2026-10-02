package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The chat decision pipeline: Jev decides whether, to whom, and on which
// channel the bot communicates; the text model only writes the message body.
// Candidates, channels and failure budgets are enumerated and enforced in this
// code, so the model can neither invent a recipient nor bypass the rate
// limits. A noul without a clear "yes" sends nothing; a writer failure sends
// the canned fallback line and still counts against the same budget.

const chatFallbackMessage = "Sorry, I can't talk right now."
const chatMessageMaxBytes = 200
const chatCandidateLimit = 12
const chatHistoryLimit = 6

type chatCandidate struct {
	Key      string  `json:"key"`
	Name     string  `json:"name"`
	Source   string  `json:"source"` // "recent_chat" | "group" | "nearby"
	IsBot    bool    `json:"is_bot"`
	Distance float64 `json:"distance_m"`
	InGroup  bool    `json:"in_group"`
}

type chatMessageInfo struct {
	From    string  `json:"from"`
	Channel string  `json:"channel"`
	AgeS    float64 `json:"age_s"`
	Message string  `json:"message"`
}

type chatComposeRequest struct {
	BotName   string
	Intent    string
	Channel   string
	Audience  string // person name, empty for broadcast
	GroupSize uint32
	Chats     []chatMessageInfo
	// Money, Character and CurrentTask are always provided as facts the
	// character knows. Sections carries only the Jev-selected optional
	// groups; Guidance names that selection for reply intents so the
	// writer answers the actual question instead of paraphrasing it.
	Money       string
	Character   string
	CurrentTask string
	Sections    map[string]string
	Guidance    string
}

// chatJob carries everything the worker needs; it never reads live actor
// state, so the actor loop stays responsive while models run.
type chatJob struct {
	queuedAt       time.Time
	actor          *actor
	triggerEventID string
	state          map[string]any
	questions      map[string]typesafeQuestion
	candidates     []chatCandidate
	groupSize      uint32
	profile        string
	jitter         time.Duration
	context        *chatContextMaterial
}

// chatContextGroups are the optional writer-context groups Jev may request;
// parsed answers are validated against this allowlist. Groups keep the
// decision battery small while covering anything a player might ask about:
// possessions, abilities, quests, surroundings, activity detail, social and
// economy.
var chatContextGroups = []string{
	"possessions", "abilities", "quests", "surroundings",
	"activity_detail", "social", "economy",
}

// chatContextMaterial holds deterministic bounded gameplay-context slices
// pre-materialized on the actor; chat workers only pick from these, so no
// model call and no live state read happens during composition. Money, the
// character line and the current activity are always-on; groups stay empty
// until Jev selects them for one message.
type chatContextMaterial struct {
	Money     string `json:"money,omitempty"`
	Character string `json:"character,omitempty"`
	Activity  string `json:"activity,omitempty"`

	Possessions    string `json:"possessions,omitempty"`
	Abilities      string `json:"abilities,omitempty"`
	Quests         string `json:"quests,omitempty"`
	Surroundings   string `json:"surroundings,omitempty"`
	ActivityDetail string `json:"activity_detail,omitempty"`
	Social         string `json:"social,omitempty"`
	Economy        string `json:"economy,omitempty"`
}

const (
	chatContextItemLimit     = 10
	chatContextRecipeLimit   = 8
	chatContextQuestLimit    = 5
	chatContextNearbyLimit   = 5
	chatContextTaskLimit     = 5
	chatContextMemoryLimit   = 3
	chatContextVendorLimit   = 5
	chatContextMaterialLimit = 5
	chatContextSectionBytes  = 400
)

// joinCapped joins deterministic parts into one capped section string.
func joinCapped(parts []string, capBytes int) string {
	joined := strings.Join(parts, "; ")
	if len(joined) <= capBytes {
		return joined
	}
	for len(parts) > 1 {
		parts = parts[:len(parts)-1]
		joined = strings.Join(parts, "; ")
		if len(joined) <= capBytes {
			break
		}
	}
	return trimUTF8(joined, capBytes)
}

// moneyLine formats the character's wealth in gold/silver/copper.
func moneyLine(copper uint32) string {
	if copper > 0 {
		return fmt.Sprintf("%d gold, %d silver, %d copper", copper/10000, (copper/100)%100, copper%100)
	}
	return "no money"
}

// buildChatContext materializes the bounded gameplay slices from the latest
// snapshot and actor state.
func (a *actor) buildChatContext() *chatContextMaterial {
	material := &chatContextMaterial{}
	material.Money = moneyLine(a.latest.Inventory.MoneyCopper)

	material.Character = joinCapped(characterParts(a.latest), chatContextSectionBytes)

	if task := a.state.Task; task != nil {
		current := strings.TrimSpace(task.Kind)
		if task.TargetName != "" {
			current += " " + task.TargetName
		}
		if task.GoalCount > 0 {
			current += fmt.Sprintf(" (%d/%d)", task.Completed, task.GoalCount)
		}
		material.Activity = current
	} else if travel := a.latest.TravelTarget; travel != nil && travel.IsTraveling && travel.DestinationName != "" {
		material.Activity = "traveling to " + travel.DestinationName
	}

	material.Possessions = joinCapped(possessionsParts(a.latest), chatContextSectionBytes)
	material.Abilities = joinCapped(abilitiesParts(a.latest), chatContextSectionBytes)
	material.Quests = joinCapped(questParts(a.latest), chatContextSectionBytes)
	material.Surroundings = joinCapped(surroundingsParts(a.latest), chatContextSectionBytes)
	material.ActivityDetail = joinCapped(activityDetailParts(a.state, a.recent), chatContextSectionBytes)
	material.Social = joinCapped(socialParts(a.latest, a.state), chatContextSectionBytes)
	material.Economy = joinCapped(economyParts(a.latest, a.state.Task), chatContextSectionBytes)
	return material
}

// characterParts describes the character: race, class, level, health, status.
func characterParts(snapshot snapshot) []string {
	parts := []string{}
	line := ""
	if race, has := raceNames[snapshot.Bot.RaceID]; has {
		line = race + " "
	}
	if class, has := classNames[snapshot.Bot.ClassID]; has {
		line += class + " "
	}
	line += fmt.Sprintf("level %d, %d%% health", snapshot.Bot.Level, snapshot.Bot.HealthPct)
	if snapshot.Bot.Alive != nil && !*snapshot.Bot.Alive {
		line += ", dead"
	} else if snapshot.Bot.InCombat {
		line += ", in combat"
	} else if snapshot.Bot.InFlight {
		line += ", flying"
	}
	parts = append(parts, strings.TrimSpace(line))
	if snapshot.Guild != nil && snapshot.Guild.Name != "" {
		guild := "guild " + snapshot.Guild.Name
		if snapshot.Guild.Rank != "" {
			guild += " (" + snapshot.Guild.Rank + ")"
		}
		parts = append(parts, guild)
	}
	return parts
}

// possessionsParts lists carried stacks and worn gear.
func possessionsParts(snapshot snapshot) []string {
	type stack struct {
		count uint32
		name  string
	}
	var stacks []stack
	for _, item := range snapshot.Inventory.Items {
		if item.Name == "" || item.Count == 0 {
			continue
		}
		stacks = append(stacks, stack{count: item.Count, name: item.Name})
	}
	sort.Slice(stacks, func(i, j int) bool {
		if stacks[i].count != stacks[j].count {
			return stacks[i].count > stacks[j].count
		}
		return stacks[i].name < stacks[j].name
	})
	if len(stacks) > chatContextItemLimit {
		stacks = stacks[:chatContextItemLimit]
	}
	parts := []string{}
	if len(stacks) > 0 {
		carried := make([]string, 0, len(stacks))
		for _, entry := range stacks {
			carried = append(carried, fmt.Sprintf("%s x%d", entry.name, entry.count))
		}
		parts = append(parts, "carrying "+strings.Join(carried, ", "))
	}
	if snapshot.BagSlotsFree > 0 {
		parts = append(parts, fmt.Sprintf("%d free bag slots", snapshot.BagSlotsFree))
	}
	worn := make([]string, 0, len(snapshot.Equipped))
	for _, item := range snapshot.Equipped {
		if item.Name == "" {
			continue
		}
		worn = append(worn, item.Name)
	}
	sort.Strings(worn)
	if len(worn) > 0 {
		parts = append(parts, "wearing "+strings.Join(worn, ", "))
	}
	return parts
}

// abilitiesParts describes professions, derivable class role and known
// recipes (product names only; reagent needs live in the economy group).
func abilitiesParts(snapshot snapshot) []string {
	parts := []string{}
	professions := make([]string, 0, len(snapshot.Professions))
	for name := range snapshot.Professions {
		professions = append(professions, name)
	}
	sort.Strings(professions)
	if len(professions) > 0 {
		profLines := make([]string, 0, len(professions))
		for _, name := range professions {
			profLines = append(profLines, fmt.Sprintf("%s %d", name, snapshot.Professions[name]))
		}
		parts = append(parts, "professions "+strings.Join(profLines, ", "))
	}
	if roles, has := classRoles[snapshot.Bot.ClassID]; has && len(roles) > 0 {
		ordered := append([]string(nil), roles...)
		sort.Strings(ordered)
		parts = append(parts, "class roles (not verified spec or readiness) "+strings.Join(ordered, "/"))
	}
	if len(snapshot.Abilities) > 0 {
		names := append([]string(nil), snapshot.Abilities...)
		sort.Strings(names)
		parts = append(parts, "action-bar spells (partial spellbook) "+strings.Join(names, ", "))
	}
	recipes := make([]string, 0, len(snapshot.CraftingRecipes))
	for _, recipe := range snapshot.CraftingRecipes {
		if recipe.Name == "" {
			continue
		}
		recipes = append(recipes, recipe.Name)
	}
	sort.Strings(recipes)
	if len(recipes) > chatContextRecipeLimit {
		recipes = recipes[:chatContextRecipeLimit]
	}
	if len(recipes) > 0 {
		parts = append(parts, "recipes "+strings.Join(recipes, ", "))
	}
	if len(snapshot.FlightPaths) > 0 {
		paths := append([]string(nil), snapshot.FlightPaths...)
		sort.Strings(paths)
		if len(paths) > chatContextNearbyLimit {
			paths = paths[:chatContextNearbyLimit]
		}
		parts = append(parts, "flight paths "+strings.Join(paths, ", "))
	}
	return parts
}

// questParts summarizes active quests with objective progress.
func questParts(snapshot snapshot) []string {
	if len(snapshot.Quests) > chatContextQuestLimit {
		snapshot.Quests = snapshot.Quests[:chatContextQuestLimit]
	}
	parts := []string{}
	for _, quest := range snapshot.Quests {
		if quest.Title == "" {
			continue
		}
		objectives := ""
		if len(quest.Objectives) > 0 {
			objectiveParts := make([]string, 0, len(quest.Objectives))
			for _, objective := range quest.Objectives {
				objectiveParts = append(objectiveParts, fmt.Sprintf("%d/%d", objective.Count, objective.Required))
			}
			objectives = " (" + strings.Join(objectiveParts, ", ") + ")"
		}
		parts = append(parts, quest.Title+objectives)
	}
	return parts
}

// surroundingsParts lists who and what is nearby, nearest first.
func surroundingsParts(snapshot snapshot) []string {
	parts := []string{}
	appendNearby := func(entries []string) {
		if len(entries) > chatContextNearbyLimit {
			entries = entries[:chatContextNearbyLimit]
		}
		parts = append(parts, entries...)
	}
	npcs := make([]string, 0, len(snapshot.NearbyNPCs))
	for _, npc := range snapshot.NearbyNPCs {
		if npc.Name == "" {
			continue
		}
		npcs = append(npcs, fmt.Sprintf("%s (%.0fm)", npc.Name, npc.Distance))
	}
	sort.Slice(npcs, func(i, j int) bool { return npcs[i] < npcs[j] })
	appendNearby(npcs)
	mailboxes := make([]string, 0, len(snapshot.NearbyMailboxes))
	for _, mailbox := range snapshot.NearbyMailboxes {
		mailboxes = append(mailboxes, fmt.Sprintf("mailbox %s (%.0fm)", mailbox.Name, mailbox.Distance))
	}
	sort.Strings(mailboxes)
	appendNearby(mailboxes)
	corpses := make([]string, 0, len(snapshot.NearbyCorpses))
	for _, corpse := range snapshot.NearbyCorpses {
		if corpse.Name == "" {
			continue
		}
		corpses = append(corpses, fmt.Sprintf("corpse %s (%.0fm)", corpse.Name, corpse.Distance))
	}
	sort.Strings(corpses)
	appendNearby(corpses)
	objects := make([]string, 0, len(snapshot.NearbyGameObjects))
	for _, object := range snapshot.NearbyGameObjects {
		if object.Name == "" {
			continue
		}
		objects = append(objects, fmt.Sprintf("%s (%.0fm)", object.Name, object.Distance))
	}
	sort.Strings(objects)
	appendNearby(objects)
	return parts
}

// activityDetailParts describes the current task, recent outcomes and
// movement status.
func activityDetailParts(state persistedAgent, recent []recentEvent) []string {
	parts := []string{}
	if state.Task != nil {
		detail := "task " + strings.TrimSpace(state.Task.Kind)
		if state.Task.TargetName != "" {
			detail += " " + state.Task.TargetName
		}
		if state.Task.GoalCount > 0 {
			detail += fmt.Sprintf(" (%d/%d)", state.Task.Completed, state.Task.GoalCount)
		}
		parts = append(parts, detail)
	}
	var results []string
	for index := len(recent) - 1; index >= 0 && len(results) < chatContextTaskLimit; index-- {
		item := recent[index]
		if item.Type != "task_result" {
			continue
		}
		var payload struct {
			Kind      string `json:"kind"`
			Status    string `json:"status"`
			Completed uint32 `json:"completed"`
			Goal      uint32 `json:"goal"`
		}
		if json.Unmarshal(item.Payload, &payload) != nil || payload.Kind == "" {
			continue
		}
		summary := payload.Kind + ": " + payload.Status
		if payload.Goal > 0 {
			summary += fmt.Sprintf(" %d/%d", payload.Completed, payload.Goal)
		}
		results = append([]string{summary}, results...)
	}
	parts = append(parts, results...)
	if state.Task == nil && len(results) == 0 {
		return nil
	}
	return parts
}

// socialParts lists group members and remembered identity details.
func socialParts(snapshot snapshot, state persistedAgent) []string {
	parts := []string{}
	if len(snapshot.Bot.GroupMembers) > 0 {
		members := make([]string, 0, len(snapshot.Bot.GroupMembers))
		for _, member := range snapshot.Bot.GroupMembers {
			members = append(members, fmt.Sprintf("%s lvl%d", member.Name, member.Level))
		}
		sort.Strings(members)
		parts = append(parts, "group "+strings.Join(members, ", "))
	}
	for index, memory := range state.Memories {
		if index >= chatContextMemoryLimit {
			break
		}
		if trimmed := strings.TrimSpace(memory); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	for _, reputation := range snapshot.Reputations {
		if reputation.Faction != "" {
			parts = append(parts, fmt.Sprintf("reputation %s: %d", reputation.Faction, reputation.Standing))
		}
	}
	return parts
}

// economyParts describes vendor prices, material needs and mail. Material
// item names resolve from the carried inventory and the active task's
// goals; unknown item IDs stay numeric rather than being invented.
func economyParts(snapshot snapshot, currentTask *task) []string {
	parts := []string{}
	itemNames := map[uint32]string{}
	for _, item := range snapshot.Inventory.Items {
		if item.Name != "" {
			itemNames[item.ItemID] = item.Name
		}
	}
	vendors := make([]string, 0, chatContextVendorLimit)
	for _, npc := range snapshot.NearbyNPCs {
		for _, offer := range npc.VendorOffers {
			if len(vendors) >= chatContextVendorLimit {
				break
			}
			vendors = append(vendors, fmt.Sprintf("%s sells %s x%d for %d copper",
				npc.Name, offer.Name, offer.BundleCount, offer.PriceCopper))
		}
	}
	sort.Strings(vendors)
	parts = append(parts, vendors...)
	var materialGoals []craftReagent
	if currentTask != nil {
		materialGoals = currentTask.CraftMaterialGoals
	}
	if len(materialGoals) > 0 {
		needs := make([]string, 0, chatContextMaterialLimit)
		for _, goal := range materialGoals {
			if len(needs) >= chatContextMaterialLimit {
				break
			}
			name, has := itemNames[goal.ItemID]
			if !has {
				name = fmt.Sprintf("item %d (name unknown)", goal.ItemID)
			}
			needs = append(needs, fmt.Sprintf("%s x%d", name, goal.Count))
		}
		if len(needs) > 0 {
			parts = append(parts, "needs "+strings.Join(needs, ", "))
		}
	}
	if snapshot.MailCount > 0 {
		parts = append(parts, fmt.Sprintf("%d letters waiting", snapshot.MailCount))
	}
	if snapshot.Hearthstone != "" {
		parts = append(parts, "hearthstone bound to "+snapshot.Hearthstone)
	}
	return parts
}

type chatDecisionResult struct {
	triggerEventID  string
	communicate     bool
	skipReason      string
	audienceKey     string
	audienceName    string
	channel         string
	channelFallback bool
	// audienceFallback marks a reply whose audience pick fell below the
	// confidence gate; the message downgraded to broadcast instead of
	// being dropped.
	audienceFallback bool
	intent           string
	sections         []string
	message          string
	source           string // "writer" | "canned"
	noul             float64
	confidence       float64
	evaluateElapsed  time.Duration
	composeElapsed   time.Duration
	err              string
}

type chatEvaluator interface {
	evaluate(state any, questions map[string]typesafeQuestion) (map[string]typesafeAnswer, error)
}

type chatComposer interface {
	composeChat(slots chan struct{}, profile string, request chatComposeRequest) (string, error)
}

func (c *controller) chatEnabled() bool { return c.chatEval != nil && c.chatCompose != nil }

// toolName reads the nested OpenAI tool name; empty for malformed entries.
func toolName(tool map[string]any) string {
	if nested, has := tool["function"].(map[string]any); has {
		name, _ := nested["name"].(string)
		return name
	}
	return ""
}

// taskTools removes send_chat when the communication pipeline owns chat
// decisions; the task model keeps every non-chat tool unchanged.
func taskTools(tools []map[string]any, chatOwned bool) []map[string]any {
	if !chatOwned {
		return tools
	}
	filtered := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if toolName(tool) == "send_chat" {
			continue
		}
		filtered = append(filtered, tool)
	}
	return filtered
}

func (a *actor) chatEnabled() bool { return a.owner.chatEnabled() }

// scheduleChatPipeline decides whether an incoming chat event is handled by
// the Jev pipeline (true) or by the ordinary decision model path (false).
func (a *actor) scheduleChatPipeline(incoming event) bool {
	if !a.chatEnabled() || a.chatPending || !a.online || !a.bridgeActive || !a.hasSnapshot {
		return false
	}
	if a.latest.Bot.Alive != nil && !*a.latest.Bot.Alive {
		return false // the dead do not chat
	}
	if a.latest.Bot.InCombat {
		return false
	}
	if block := a.chatBudgetBlock(time.Now()); block != "" {
		log.Printf("agent chat_deferred bot=%s block=%s", a.botGUID, block)
		return false
	}
	job, ok := a.buildChatJob(incoming)
	if !ok {
		return false
	}
	a.chatPending = true
	select {
	case a.owner.chatJobs <- job:
		return true
	default:
		a.chatPending = false
		return false
	}
}

func (a *actor) buildChatJob(incoming event) (chatJob, bool) {
	now := time.Now()
	chats := a.recentChats(now)
	if len(chats) == 0 {
		return chatJob{}, false
	}
	candidates := a.chatCandidates(chats)
	profile := a.state.Profile
	if len(profile) > 400 {
		profile = trimUTF8(profile, 400)
	}
	return chatJob{
		queuedAt:       now,
		actor:          a,
		triggerEventID: incoming.EventID,
		state:          a.chatState(chats, candidates),
		questions:      buildChatQuestions(candidates, a.latest.Bot.GroupSize),
		candidates:     candidates,
		groupSize:      a.latest.Bot.GroupSize,
		profile:        profile,
		jitter:         a.owner.cfg.chatJitter,
		context:        a.buildChatContext(),
	}, true
}

func (a *actor) recentChats(now time.Time) []chatMessageInfo {
	chats := make([]chatMessageInfo, 0, len(a.recent))
	for _, item := range a.recent {
		if item.Type != "chat_received" {
			continue
		}
		var payload struct {
			Channel    string `json:"channel"`
			ChatType   string `json:"chat_type"`
			SenderName string `json:"sender_name"`
			Message    string `json:"message"`
		}
		if json.Unmarshal(item.Payload, &payload) != nil || payload.SenderName == "" || payload.Message == "" {
			continue
		}
		if strings.EqualFold(payload.SenderName, a.latest.Bot.Name) {
			continue // the bot's own messages echo back as chat events
		}
		// Native and cluster bridges use chat_type for non-channel messages.
		if payload.ChatType != "" && payload.ChatType != "channel" {
			payload.Channel = payload.ChatType
		}
		age := 0.0
		if item.Timestamp > 0 {
			age = now.Sub(time.UnixMilli(item.Timestamp)).Seconds()
			if age > maxRealtimeEventAge.Seconds() {
				continue
			}
		}
		chats = append(chats, chatMessageInfo{
			From: payload.SenderName, Channel: payload.Channel, AgeS: age, Message: payload.Message,
		})
	}
	if len(chats) > chatHistoryLimit {
		chats = chats[len(chats)-chatHistoryLimit:]
	}
	return chats
}

func (a *actor) chatCandidates(chats []chatMessageInfo) []chatCandidate {
	seen := map[string]bool{strings.ToLower(a.latest.Bot.Name): true}
	candidates := make([]chatCandidate, 0, chatCandidateLimit)
	add := func(name, source string, isBot bool, distance float64, inGroup bool) {
		key := strings.ToLower(name)
		if name == "" || seen[key] || len(candidates) >= chatCandidateLimit {
			return
		}
		seen[key] = true
		candidates = append(candidates, chatCandidate{
			Key: "person_" + strconv.Itoa(len(candidates)), Name: name, Source: source,
			IsBot: isBot, Distance: distance, InGroup: inGroup,
		})
	}
	// Recent speakers first: they are the most likely reply targets.
	for _, chat := range chats {
		add(chat.From, "recent_chat", a.isKnownBot(chat.From), 0, false)
	}
	for _, member := range a.latest.Bot.GroupMembers {
		add(member.Name, "group", member.IsBot, 0, true)
	}
	nearby := append([]playerInfo(nil), a.latest.NearbyPlayers...)
	sort.Slice(nearby, func(i, j int) bool { return nearby[i].Distance < nearby[j].Distance })
	for _, player := range nearby {
		add(player.Name, "nearby", player.IsBot, player.Distance, player.InGroup)
	}
	return candidates
}

func (a *actor) isKnownBot(name string) bool {
	for _, player := range a.latest.NearbyPlayers {
		if strings.EqualFold(player.Name, name) {
			return player.IsBot
		}
	}
	for _, member := range a.latest.Bot.GroupMembers {
		if strings.EqualFold(member.Name, name) {
			return member.IsBot
		}
	}
	return false
}

func (a *actor) chatState(chats []chatMessageInfo, candidates []chatCandidate) map[string]any {
	task := map[string]any{"kind": "none", "phase": "none", "progress": "0/0"}
	if current := a.state.Task; current != nil {
		task = map[string]any{
			"kind": current.Kind, "phase": current.Phase,
			"progress": fmt.Sprintf("%d/%d", current.Completed, current.GoalCount),
		}
	}
	return map[string]any{
		"bot": map[string]any{
			"name": a.latest.Bot.Name, "level": a.latest.Bot.Level,
			"alive":     a.latest.Bot.Alive == nil || *a.latest.Bot.Alive,
			"in_combat": a.latest.Bot.InCombat, "group_size": a.latest.Bot.GroupSize,
			"profile": a.state.Profile,
		},
		"current_task": task,
		"recent_chat":  chats,
		"candidates":   candidates,
	}
}

// buildChatQuestions assembles one battery of atomic questions. Channel
// options are constrained by what this code can validate afterwards, so the
// model cannot pick a channel the character does not have.
func buildChatQuestions(candidates []chatCandidate, groupSize uint32) map[string]typesafeQuestion {
	questions := map[string]typesafeQuestion{
		"should_communicate": {
			Type: "noul",
			Instructions: map[string]any{
				"question": "Should this character send one chat message right now?",
				"focus":    "Judge only the social situation in recent_chat. Quest, loot and task progress are not social reasons to talk.",
			},
			Criteria: map[string]any{
				"true": map[string]any{
					"what": "Someone addressed the character, greeted it, or there is another clear social opening for one short message",
					"examples": []string{
						"A player whispers a question", "Someone says hello nearby",
						"A group member asks whether anyone needs loot",
					},
				},
				"false": map[string]any{
					"what":    "Nobody is engaging the character and there is no natural opening",
					"not_for": "Repeating a greeting, narrating tasks, or filling silence",
				},
			},
		},
		"intent": {
			Type: "choice",
			Instructions: map[string]any{
				"question": "If the character sends a message, what is its purpose?",
			},
			Criteria: map[string]any{
				"reply":        "Answer or react to something said in recent_chat",
				"greet":        "A short friendly hello",
				"assist_offer": "Offer help, grouping or directions",
				"recruit":      "Announce that the character is looking for group members",
				"flavor":       "A short in-character ambient line",
			},
		},
	}
	channelCriteria := map[string]any{
		"say":     "Speak out loud to everyone nearby",
		"yell":    "Shout to a wider nearby area; use sparingly",
		"general": "The zone-wide channel; use sparingly",
	}
	if groupSize >= 2 {
		channelCriteria["party"] = "Only the character's own group can hear it"
	}
	if len(candidates) > 0 {
		channelCriteria["whisper"] = "Private message to one specific person by name"
		audienceCriteria := map[string]any{"broadcast": map[string]any{
			"what":    "Nobody specific; the message is for everyone in hearing range",
			"not_for": "Answering one particular person",
		}}
		for _, candidate := range candidates {
			audienceCriteria[candidate.Key] = map[string]any{
				"name": candidate.Name, "heard_recently": candidate.Source == "recent_chat",
				"in_group": candidate.InGroup, "is_bot": candidate.IsBot,
				"distance_m": candidate.Distance,
			}
		}
		questions["audience"] = typesafeQuestion{
			Type: "choice",
			Instructions: map[string]any{
				"question": "Who is the message for?",
				"focus":    "Pick the person the message is directed at, or broadcast when it is for everyone nearby.",
			},
			Criteria: audienceCriteria,
		}
	}
	questions["channel"] = typesafeQuestion{
		Type:         "choice",
		Instructions: map[string]any{"question": "Which chat channel should the message use?"},
		Criteria:     channelCriteria,
	}
	// Optional writer-context groups. The writer cannot read live game
	// state, so Jev selects which pre-materialized groups the writer needs
	// for this one message; selection only costs tokens when the group is
	// actually requested. Seven groups keep the battery small while
	// covering identity, possessions, abilities, quests, surroundings,
	// activity and economy questions.
	for _, group := range chatContextGroups {
		questions["ctx_"+group] = typesafeQuestion{
			Type: "choice",
			Instructions: map[string]any{
				"question": "Should the writer see this character's " + chatContextGroupLabel(group) + "?",
				"focus":    "Include context only when the message would mention or answer with it.",
			},
			Criteria: map[string]any{
				"include": "The message needs these facts to answer or speak accurately",
				"omit":    "The message does not need this context",
			},
		}
	}
	return questions
}

// chatContextGroupLabel describes one optional context group for the Jev
// question and the writer guidance.
func chatContextGroupLabel(group string) string {
	switch group {
	case "possessions":
		return "carried and worn items"
	case "abilities":
		return "professions, recipes and usable abilities"
	case "quests":
		return "active quests and their progress"
	case "surroundings":
		return "nearby people, vendors, mailboxes and objects"
	case "activity_detail":
		return "current task progress and recent task outcomes"
	case "social":
		return "group members and remembered people"
	case "economy":
		return "vendor prices, material needs and mail"
	default:
		return group
	}
}

func chatChannelAllowed(channel string, personAudience bool, groupSize uint32) bool {
	switch channel {
	case "say", "yell", "general":
		return true
	case "whisper":
		return personAudience
	case "party":
		return groupSize >= 2
	default:
		return false
	}
}

func chatIntentAllowed(intent string) bool {
	switch intent {
	case "reply", "greet", "assist_offer", "recruit", "flavor":
		return true
	default:
		return false
	}
}

func (job chatJob) candidate(key string) *chatCandidate {
	for index := range job.candidates {
		if job.candidates[index].Key == key {
			return &job.candidates[index]
		}
	}
	return nil
}

func sanitizeChatMessage(message string) string {
	message = strings.ReplaceAll(message, "\r", " ")
	message = strings.ReplaceAll(message, "\n", " ")
	return trimUTF8(strings.TrimSpace(message), chatMessageMaxBytes)
}

// runChatPipeline evaluates the question battery, composes the message when
// the decision says to speak, and reports metadata only. All model failures
// degrade to "do not speak" or the canned line; nothing is invented.
func (c *controller) runChatPipeline(job chatJob) chatDecisionResult {
	result := chatDecisionResult{triggerEventID: job.triggerEventID, channel: "say", intent: "reply"}
	if job.jitter > 0 {
		time.Sleep(decisionJitter(job.triggerEventID, job.jitter))
	}
	started := time.Now()
	answers, err := c.chatEval.evaluate(job.state, job.questions)
	result.evaluateElapsed = time.Since(started)
	if err != nil {
		result.skipReason = "evaluate_failed"
		result.err = err.Error()
		return result
	}
	speak, ok := answers["should_communicate"]
	if !ok || speak.Type != "noul" {
		result.skipReason = "missing_should_communicate"
		return result
	}
	result.noul = speak.Noul
	if speak.Noul < c.cfg.chatDecisionThreshold {
		result.skipReason = "below_threshold"
		return result
	}
	if answer, has := answers["intent"]; has && answer.Type == "choice" && chatIntentAllowed(answer.Choice) {
		result.intent = answer.Choice
	}
	audienceKey := "broadcast"
	if answer, has := answers["audience"]; has && answer.Type == "choice" && answer.Choice != "" {
		audienceKey = answer.Choice
		result.confidence = answer.Confidence
	}
	if audienceKey != "broadcast" {
		candidate := job.candidate(audienceKey)
		if candidate == nil {
			result.skipReason = "unknown_audience"
			return result
		}
		if result.confidence < c.cfg.chatAudienceConfidence {
			// A reply the model wants to send but cannot confidently
			// address still deserves an answer: downgrade to an
			// audienceless message so the channel block can pick say or
			// party instead of dropping it. Only a below-threshold
			// should_communicate stays silent.
			if result.intent == "reply" {
				result.audienceFallback = true
				result.confidence = 0
				audienceKey = "broadcast"
			} else {
				result.skipReason = "low_confidence"
				return result
			}
		} else {
			result.audienceKey = audienceKey
			result.audienceName = candidate.Name
		}
	}
	channel := "say"
	if answer, has := answers["channel"]; has && answer.Type == "choice" && answer.Choice != "" {
		channel = answer.Choice
	}
	if !chatChannelAllowed(channel, audienceKey != "broadcast", job.groupSize) {
		result.channelFallback = true
		channel = "say"
	}
	result.channel = channel
	// Optional gameplay context: Jev's per-group include answers are
	// validated against the allowlist and materialized from the snapshot
	// the actor prepared. Reply intents name the selection as writer
	// guidance so the answer addresses the actual question; ambient
	// messages get the facts without steering.
	sections := make(map[string]string, len(chatContextGroups))
	selected := make([]string, 0, len(chatContextGroups))
	if job.context != nil {
		material := map[string]string{
			"possessions":     job.context.Possessions,
			"abilities":       job.context.Abilities,
			"quests":          job.context.Quests,
			"surroundings":    job.context.Surroundings,
			"activity_detail": job.context.ActivityDetail,
			"social":          job.context.Social,
			"economy":         job.context.Economy,
		}
		for _, group := range chatContextGroups {
			if answer, has := answers["ctx_"+group]; !has || answer.Type != "choice" || answer.Choice != "include" {
				continue
			}
			selected = append(selected, group)
			if content := material[group]; content != "" {
				sections[group] = content
			}
		}
	}
	guidance := ""
	if result.intent == "reply" && len(selected) > 0 {
		guidance = "Answer the human's question using these facts about this character: " +
			strings.Join(selected, ", ")
	}
	money, character, currentTask := "", "", ""
	if job.context != nil {
		money = job.context.Money
		character = job.context.Character
		currentTask = job.context.Activity
	}
	composeStart := time.Now()
	message, composeErr := c.chatCompose.composeChat(c.chatSlots, job.profile, chatComposeRequest{
		BotName: chatBotName(job), Intent: result.intent, Channel: result.channel,
		Audience: result.audienceName, GroupSize: job.groupSize,
		Chats: chatHistoryFromState(job.state),
		Money: money, Character: character, CurrentTask: currentTask, Sections: sections, Guidance: guidance,
	})
	result.sections = selected
	result.composeElapsed = time.Since(composeStart)
	if composeErr != nil {
		result.err = composeErr.Error()
	}
	if strings.TrimSpace(message) == "" {
		message = chatFallbackMessage
		result.source = "canned"
	} else {
		result.source = "writer"
	}
	result.message = sanitizeChatMessage(message)
	result.communicate = true
	return result
}

func chatBotName(job chatJob) string {
	if bot, has := job.state["bot"].(map[string]any); has {
		if name, has := bot["name"].(string); has {
			return name
		}
	}
	return ""
}

func chatHistoryFromState(state map[string]any) []chatMessageInfo {
	if chats, has := state["recent_chat"].([]chatMessageInfo); has {
		return chats
	}
	return nil
}

func (c *controller) startChatWorkers(count uint32) {
	for worker := uint32(0); worker < count; worker++ {
		go func() {
			for job := range c.chatJobs {
				result := c.runChatPipeline(job)
				select {
				case job.actor.inbox <- event{Type: "internal_chat_decision", chatDecision: &result}:
				case <-job.actor.stop:
				}
			}
		}()
	}
}

func (a *actor) handleChatDecisionResult(result chatDecisionResult) {
	a.chatPending = false
	// The auto-reply changes the context any in-flight decision model call saw.
	a.revision++
	log.Printf("agent chat_decision bot=%s event_id=%q communicate=%t skip=%q audience=%q recipient=%q channel=%q fallback=%t audience_fallback=%t intent=%q sections=%v source=%q noul=%.2f confidence=%.2f eval_ms=%d compose_ms=%d error=%q",
		a.botGUID, result.triggerEventID, result.communicate, result.skipReason, result.audienceKey,
		result.audienceName, result.channel, result.channelFallback, result.audienceFallback, result.intent,
		result.sections, result.source, result.noul, result.confidence, result.evaluateElapsed.Milliseconds(),
		result.composeElapsed.Milliseconds(), result.err)
	now := time.Now()
	if result.communicate {
		if block := a.chatBudgetBlock(now); block != "" {
			log.Printf("agent chat_skipped bot=%s block=%s", a.botGUID, block)
		} else {
			arguments := map[string]any{"channel": result.channel, "message": result.message}
			if result.channel == "whisper" && result.audienceName != "" {
				arguments["recipient"] = result.audienceName
			}
			a.sendPrimitive("send_chat", arguments)
			a.recordChatSent(now)
			a.rememberEvent(event{Type: "chat_auto_replied", Timestamp: now.UnixMilli(),
				Payload: mustJSON(map[string]any{
					"channel": result.channel, "recipient": result.audienceName,
					"message": result.message, "source": result.source,
				})})
		}
	}
	// Resume the ordinary task decision exactly as a plain chat event would.
	if a.pendingDecisionReason == "" {
		a.pendingDecisionReason = "chat_received"
	}
	if a.bridgeActive {
		a.requestSnapshot()
	}
	a.persist()
}

// Chat volume is bounded in code for every sender, model-driven or not.
func (a *actor) pruneChatBudget(now time.Time) {
	windowStart := now.Add(-a.owner.cfg.chatWindow).UnixMilli()
	kept := a.state.ChatRecent[:0]
	for _, sent := range a.state.ChatRecent {
		if sent >= windowStart {
			kept = append(kept, sent)
		}
	}
	a.state.ChatRecent = kept
}

func (a *actor) chatBudgetBlock(now time.Time) string {
	a.pruneChatBudget(now)
	if a.state.ChatLastSent != 0 && now.UnixMilli()-a.state.ChatLastSent < a.owner.cfg.chatMinInterval.Milliseconds() {
		return "min_interval"
	}
	if uint32(len(a.state.ChatRecent)) >= a.owner.cfg.chatMaxPerWindow {
		return "window_limit"
	}
	return ""
}

func (a *actor) recordChatSent(now time.Time) {
	a.state.ChatRecent = append(a.state.ChatRecent, now.UnixMilli())
	a.state.ChatLastSent = now.UnixMilli()
}
