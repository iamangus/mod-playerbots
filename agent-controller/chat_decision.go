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
	// Money and CurrentTask are always provided as facts the character
	// knows. Sections carries only the Jev-selected optional material;
	// Guidance names that selection for reply intents so the writer
	// answers the actual question instead of paraphrasing it.
	Money       string
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

// chatContextSections are the optional writer-context sections Jev may
// request; parsed answers are validated against this allowlist.
var chatContextSections = []string{"inventory", "quests", "skills", "past_tasks"}

// chatContextMaterial holds deterministic bounded gameplay-context slices
// pre-materialized on the actor; chat workers only pick from these, so no
// model call and no live state read happens during composition.
type chatContextMaterial struct {
	Money       string `json:"money,omitempty"`
	CurrentTask string `json:"current_task,omitempty"`
	Inventory   string `json:"inventory,omitempty"`
	Quests      string `json:"quests,omitempty"`
	Skills      string `json:"skills,omitempty"`
	PastTasks   string `json:"past_tasks,omitempty"`
}

const (
	chatContextItemLimit    = 10
	chatContextQuestLimit   = 5
	chatContextTaskLimit    = 5
	chatContextSectionBytes = 400
)

// buildChatContext materializes the bounded gameplay slices from the latest
// snapshot and actor state. Money and the current activity are always-on;
// the optional sections stay empty until Jev selects them for one message.
func (a *actor) buildChatContext() *chatContextMaterial {
	material := &chatContextMaterial{}

	copper := a.latest.Inventory.MoneyCopper
	if copper > 0 {
		material.Money = fmt.Sprintf("%d gold, %d silver, %d copper",
			copper/10000, (copper/100)%100, copper%100)
	} else {
		material.Money = "no money"
	}

	if task := a.state.Task; task != nil {
		current := strings.TrimSpace(task.Kind)
		if task.TargetName != "" {
			current += " " + task.TargetName
		}
		material.CurrentTask = current
	} else if travel := a.latest.TravelTarget; travel != nil && travel.IsTraveling && travel.DestinationName != "" {
		material.CurrentTask = "traveling to " + travel.DestinationName
	}

	type inventoryStack struct {
		count uint32
		name  string
	}
	var stacks []inventoryStack
	for _, item := range a.latest.Inventory.Items {
		if item.Name == "" || item.Count == 0 {
			continue
		}
		stacks = append(stacks, inventoryStack{count: item.Count, name: item.Name})
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
	if len(stacks) > 0 {
		parts := make([]string, 0, len(stacks))
		for _, stack := range stacks {
			parts = append(parts, fmt.Sprintf("%s x%d", stack.name, stack.count))
		}
		material.Inventory = trimUTF8(strings.Join(parts, "; "), chatContextSectionBytes)
	}

	if len(a.latest.Quests) > chatContextQuestLimit {
		// Deterministic subset: keep the first quests in log order.
		a.latest.Quests = a.latest.Quests[:chatContextQuestLimit]
	}
	if len(a.latest.Quests) > 0 {
		parts := make([]string, 0, len(a.latest.Quests))
		for _, quest := range a.latest.Quests {
			if quest.Title == "" {
				continue
			}
			objectives := ""
			if len(quest.Objectives) > 0 {
				objectiveParts := make([]string, 0, len(quest.Objectives))
				for _, objective := range quest.Objectives {
					objectiveParts = append(objectiveParts,
						fmt.Sprintf("%d/%d", objective.Count, objective.Required))
				}
				objectives = " (" + strings.Join(objectiveParts, ", ") + ")"
			}
			parts = append(parts, quest.Title+objectives)
		}
		material.Quests = trimUTF8(strings.Join(parts, "; "), chatContextSectionBytes)
	}

	if len(a.latest.Professions) > 0 {
		names := make([]string, 0, len(a.latest.Professions))
		for name := range a.latest.Professions {
			names = append(names, name)
		}
		sort.Strings(names)
		parts := make([]string, 0, len(names))
		for _, name := range names {
			parts = append(parts, fmt.Sprintf("%s %d", name, a.latest.Professions[name]))
		}
		material.Skills = trimUTF8(strings.Join(parts, ", "), chatContextSectionBytes)
	}

	var pastTasks []string
	for index := len(a.recent) - 1; index >= 0 && len(pastTasks) < chatContextTaskLimit; index-- {
		item := a.recent[index]
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
		pastTasks = append([]string{summary}, pastTasks...)
	}
	if len(pastTasks) > 0 {
		material.PastTasks = trimUTF8(strings.Join(pastTasks, "; "), chatContextSectionBytes)
	}

	return material
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
	// Optional writer-context sections. The writer cannot read live game
	// state, so Jev selects which pre-materialized gameplay facts the
	// writer needs for this one message; selection only costs tokens when
	// the message actually needs them.
	for _, section := range chatContextSections {
		questions["ctx_"+section] = typesafeQuestion{
			Type: "choice",
			Instructions: map[string]any{
				"question": "Should the writer see this character's " + chatContextSectionLabel(section) + "?",
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

// chatContextSectionLabel describes one optional context section for the
// Jev question and the writer guidance.
func chatContextSectionLabel(section string) string {
	switch section {
	case "inventory":
		return "carried items (names and counts)"
	case "quests":
		return "active quests and their objective progress"
	case "skills":
		return "professions and their levels"
	case "past_tasks":
		return "recently finished tasks and their outcomes"
	default:
		return section
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
	// Optional gameplay context: Jev's per-section include answers are
	// validated against the allowlist and materialized from the snapshot
	// the actor prepared. Reply intents name the selection as writer
	// guidance so the answer addresses the actual question; ambient
	// messages get the facts without steering.
	sections := make(map[string]string, len(chatContextSections))
	selected := make([]string, 0, len(chatContextSections))
	for _, section := range chatContextSections {
		if answer, has := answers["ctx_"+section]; !has || answer.Type != "choice" || answer.Choice != "include" {
			continue
		}
		selected = append(selected, section)
		if job.context == nil {
			continue
		}
		switch section {
		case "inventory":
			if job.context.Inventory != "" {
				sections["inventory"] = job.context.Inventory
			}
		case "quests":
			if job.context.Quests != "" {
				sections["quests"] = job.context.Quests
			}
		case "skills":
			if job.context.Skills != "" {
				sections["skills"] = job.context.Skills
			}
		case "past_tasks":
			if job.context.PastTasks != "" {
				sections["past_tasks"] = job.context.PastTasks
			}
		}
	}
	guidance := ""
	if result.intent == "reply" && len(selected) > 0 {
		guidance = "Answer the human's question using these facts about this character: " +
			strings.Join(selected, ", ")
	}
	money, currentTask := "", ""
	if job.context != nil {
		money, currentTask = job.context.Money, job.context.CurrentTask
	}
	composeStart := time.Now()
	message, composeErr := c.chatCompose.composeChat(c.chatSlots, job.profile, chatComposeRequest{
		BotName: chatBotName(job), Intent: result.intent, Channel: result.channel,
		Audience: result.audienceName, GroupSize: job.groupSize,
		Chats: chatHistoryFromState(job.state),
		Money: money, CurrentTask: currentTask, Sections: sections, Guidance: guidance,
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
