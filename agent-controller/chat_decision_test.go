package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type stubChatEvaluator struct {
	answers map[string]typesafeAnswer
	err     error
}

func (stub *stubChatEvaluator) evaluate(state any, questions map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
	if stub.err != nil {
		return nil, stub.err
	}
	// The battery must always ask the gating question.
	if _, has := questions["should_communicate"]; !has {
		return nil, errStubBattery
	}
	return stub.answers, nil
}

var errStubBattery = &stubError{"battery missing should_communicate"}

type stubError struct{ message string }

func (err *stubError) Error() string { return err.message }

type stubChatWriter struct {
	message string
	err     error
	last    *chatComposeRequest
}

func (stub *stubChatWriter) composeChat(slots chan struct{}, profile string, request chatComposeRequest) (string, error) {
	captured := request
	stub.last = &captured
	if stub.err != nil {
		return "", stub.err
	}
	return stub.message, nil
}

func chatTestSetup(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.owner.chatEval = &stubChatEvaluator{}
	a.owner.chatCompose = &stubChatWriter{}
	a.owner.cfg.chatDecisionThreshold = 0.7
	a.owner.cfg.chatAudienceConfidence = 0.6
	a.owner.cfg.chatMinInterval = 20 * time.Second
	a.owner.cfg.chatWindow = 5 * time.Minute
	a.owner.cfg.chatMaxPerWindow = 5
	a.owner.cfg.chatJitter = time.Millisecond
	a.latest.Bot.Name = "Testbot"
	a.latest.Bot.GroupSize = 1
	a.rememberEvent(event{Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"chat_type":"whisper","channel":"whisper","sender_name":"Peer","message":"hello there"}`)})
	return a, commands
}

func chatResultEvent(result chatDecisionResult) event {
	return event{Type: "internal_chat_decision", chatDecision: &result}
}

func TestRecentChatsPreserveBridgeChatTypes(t *testing.T) {
	for _, kind := range []string{"whisper", "say", "yell", "party"} {
		t.Run(kind, func(t *testing.T) {
			a, _ := chatTestSetup(t)
			a.recent = nil
			a.rememberEvent(event{Type: "chat_received", Timestamp: time.Now().UnixMilli(),
				Payload: mustJSON(map[string]any{"chat_type": kind, "channel": "", "sender_name": "Peepee", "message": "hello"})})
			chats := a.recentChats(time.Now())
			if len(chats) != 1 || chats[0].Channel != kind {
				t.Fatalf("lost incoming chat type: %+v", chats)
			}
		})
	}
}

func TestChatPipelineSendsConfidentWhisperReply(t *testing.T) {
	a, commands := chatTestSetup(t)
	chatEvent := event{EventID: "evt1", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"whisper","sender_name":"Peer","message":"need help?"}`)}
	a.rememberEvent(chatEvent)
	job, ok := a.buildChatJob(chatEvent)
	if !ok {
		t.Fatal("chat job was not built")
	}
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.9},
		"channel":            {Type: "choice", Choice: "whisper", Confidence: 0.9},
		"intent":             {Type: "choice", Choice: "reply", Confidence: 0.8},
	}}
	a.owner.chatCompose = &stubChatWriter{message: "Sure, need a hand?"}
	result := a.owner.runChatPipeline(job)
	if !result.communicate || result.channel != "whisper" || result.audienceName != "Peer" || result.source != "writer" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if !strings.Contains(result.message, "hand") {
		t.Fatalf("writer text lost: %q", result.message)
	}
	a.chatPending = true
	a.handleEvent(chatResultEvent(result))
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "send_chat" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	var args map[string]string
	if err := json.Unmarshal(cmd.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	if args["channel"] != "whisper" || args["recipient"] != "Peer" {
		t.Fatalf("args=%v", args)
	}
	if len(a.state.ChatRecent) != 1 || a.state.ChatLastSent == 0 {
		t.Fatal("dispatch was not recorded against the chat budget")
	}
	if a.pendingDecisionReason != "chat_received" {
		t.Fatal("task decision did not resume after the chat pipeline")
	}
	found := false
	for _, item := range a.recent {
		if item.Type == "chat_auto_replied" {
			found = true
		}
	}
	if !found {
		t.Fatal("chat_auto_replied context was not recorded")
	}
}

func TestChatPipelineSkipsBelowThreshold(t *testing.T) {
	a, commands := chatTestSetup(t)
	chatEvent := event{EventID: "evt2", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"anyone here"}`)}
	a.rememberEvent(chatEvent)
	job, ok := a.buildChatJob(chatEvent)
	if !ok {
		t.Fatal("chat job was not built")
	}
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.3},
	}}
	result := a.owner.runChatPipeline(job)
	if result.communicate || result.skipReason != "below_threshold" {
		t.Fatalf("unexpected result: %+v", result)
	}
	a.handleEvent(chatResultEvent(result))
	select {
	case cmd := <-commands:
		if cmd.Operation != "snapshot" {
			t.Fatalf("unexpected command %s", cmd.Operation)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func TestChatPipelineLowConfidenceReplyDowngradesToSay(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt3", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, ok := a.buildChatJob(chatEvent)
	if !ok {
		t.Fatal("chat job was not built")
	}
	// The exact capture pattern: noul 0.91 with audience confidence 0.24
	// used to drop the reply entirely; it must now degrade to a say.
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.91},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.24},
		"channel":            {Type: "choice", Choice: "whisper", Confidence: 0.5},
		"intent":             {Type: "choice", Choice: "reply", Confidence: 0.9},
	}}
	a.owner.chatCompose = &stubChatWriter{message: "hey there"}
	result := a.owner.runChatPipeline(job)
	if !result.communicate || !result.audienceFallback || result.skipReason != "" {
		t.Fatalf("low-confidence reply must downgrade, got %+v", result)
	}
	if result.audienceName != "" || result.channel != "say" {
		t.Fatalf("downgraded reply must be audienceless say: %+v", result)
	}
}

func TestChatPipelineLowConfidenceGreetSkips(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt3b", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.4},
		"intent":             {Type: "choice", Choice: "greet", Confidence: 0.8},
	}}
	result := a.owner.runChatPipeline(job)
	if result.communicate || result.skipReason != "low_confidence" {
		t.Fatalf("non-reply with low audience confidence must still skip: %+v", result)
	}
}

func TestChatPipelineContextSectionsAndGuidance(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt6", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"whisper","sender_name":"Peer","message":"what are you carrying?"}`)}
	a.rememberEvent(chatEvent)
	a.latest.Inventory.MoneyCopper = 123456 // 12 gold, 34 silver, 56 copper
	a.latest.Inventory.Items = []struct {
		ItemID uint32 `json:"item_id"`
		Count  uint32 `json:"count"`
		Name   string `json:"name"`
	}{{ItemID: 1, Count: 12, Name: "Linen Cloth"}, {ItemID: 2, Count: 3, Name: "Copper Shortsword"}}
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.9},
		"channel":            {Type: "choice", Choice: "whisper", Confidence: 0.9},
		"intent":             {Type: "choice", Choice: "reply", Confidence: 0.9},
		"ctx_inventory":      {Type: "choice", Choice: "include", Confidence: 0.9},
		"ctx_quests":         {Type: "choice", Choice: "omit", Confidence: 0.9},
		"ctx_bogus":          {Type: "choice", Choice: "include", Confidence: 0.9},
	}}
	a.owner.chatCompose = &stubChatWriter{message: "12 Linen Cloth and a shortsword"}
	result := a.owner.runChatPipeline(job)
	if !result.communicate {
		t.Fatalf("unexpected result: %+v", result)
	}
	request := a.owner.chatCompose.(*stubChatWriter).last
	if request == nil {
		t.Fatal("writer request was not captured")
	}
	if request.Money != "12 gold, 34 silver, 56 copper" {
		t.Fatalf("money not formatted: %q", request.Money)
	}
	if got, has := request.Sections["inventory"]; !has || !strings.Contains(got, "Linen Cloth x12") {
		t.Fatalf("inventory section missing: %v", request.Sections)
	}
	if _, has := request.Sections["quests"]; has {
		t.Fatal("omitted section must not be materialized")
	}
	if len(request.Sections) != 1 {
		t.Fatalf("unknown ctx answers must be ignored: %v", request.Sections)
	}
	if request.Guidance == "" || !strings.Contains(request.Guidance, "inventory") {
		t.Fatalf("reply guidance must name the selected sections: %q", request.Guidance)
	}
}

func TestChatPipelineContextFactsWithoutGuidanceForFlavor(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt7", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"nice weather"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"intent":             {Type: "choice", Choice: "flavor", Confidence: 0.9},
		"ctx_quests":         {Type: "choice", Choice: "include", Confidence: 0.9},
	}}
	a.owner.chatCompose = &stubChatWriter{message: "the road calls"}
	result := a.owner.runChatPipeline(job)
	if !result.communicate {
		t.Fatalf("unexpected result: %+v", result)
	}
	request := a.owner.chatCompose.(*stubChatWriter).last
	if request == nil {
		t.Fatal("writer request was not captured")
	}
	if request.Guidance != "" {
		t.Fatalf("non-reply intents must not steer the writer: %q", request.Guidance)
	}
	if request.Money == "" {
		t.Fatal("money is always-on context")
	}
}

func TestChatPipelineContextDefaultsWhenUnanswered(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt8", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
	}}
	a.owner.chatCompose = &stubChatWriter{message: "hello"}
	result := a.owner.runChatPipeline(job)
	if !result.communicate || len(result.sections) != 0 {
		t.Fatalf("unanswered context questions default to none: %+v", result)
	}
	request := a.owner.chatCompose.(*stubChatWriter).last
	if request == nil || request.Money == "" || len(request.Sections) != 0 {
		t.Fatalf("always-on context must remain: %+v", request)
	}
}

func TestBuildChatContextMaterial(t *testing.T) {
	a, _ := chatTestSetup(t)
	a.latest.Inventory.MoneyCopper = 0
	a.latest.Inventory.Items = []struct {
		ItemID uint32 `json:"item_id"`
		Count  uint32 `json:"count"`
		Name   string `json:"name"`
	}{{ItemID: 1, Count: 5, Name: "Malachite"}, {ItemID: 2, Count: 30, Name: "Copper Ore"}, {ItemID: 3, Count: 0, Name: "Ghost"}}
	a.latest.Quests = []questInfo{
		{Title: "Kobold Cleanup", Objectives: []questObjective{{Count: 3, Required: 10}, {Count: 1, Required: 1}}},
		{Title: "", Objectives: []questObjective{{Count: 1, Required: 1}}},
	}
	a.latest.Professions = map[string]uint32{"Herbalism": 150, "Alchemy": 87}
	a.latest.TravelTarget = &struct {
		DestinationName string    `json:"destination_name"`
		IsTraveling     bool      `json:"is_traveling"`
		IsWorking       bool      `json:"is_working"`
		Arrived         bool      `json:"arrived"`
		Position        []float64 `json:"position"`
		MapID           uint32    `json:"map_id"`
	}{DestinationName: "Goldshire", IsTraveling: true}
	a.rememberEvent(event{Type: "task_result", Timestamp: time.Now().UnixMilli(),
		Payload: mustJSON(map[string]any{"kind": "kill", "status": "done", "completed": 5, "goal": 5})})

	material := a.buildChatContext()
	if material.Money != "no money" {
		t.Fatalf("zero money must say so: %q", material.Money)
	}
	// No current task and travel target present: activity line comes from travel.
	if material.CurrentTask != "traveling to Goldshire" {
		t.Fatalf("travel activity missing: %q", material.CurrentTask)
	}
	if got := material.Inventory; !strings.HasPrefix(got, "Copper Ore x30") || !strings.Contains(got, "Malachite x5") ||
		strings.Contains(got, "Ghost") {
		t.Fatalf("inventory must be count-sorted and skip empties: %q", got)
	}
	if got := material.Quests; got != "Kobold Cleanup (3/10, 1/1)" {
		t.Fatalf("quest summary wrong: %q", got)
	}
	if got := material.Skills; got != "Alchemy 87, Herbalism 150" {
		t.Fatalf("skills summary wrong: %q", got)
	}
	if got := material.PastTasks; got != "kill: done 5/5" {
		t.Fatalf("past tasks summary wrong: %q", got)
	}
}

func TestBuildChatContextPrefersCurrentTask(t *testing.T) {
	a, _ := chatTestSetup(t)
	a.state.Task = &task{Kind: "craft_item", TargetName: "Copper Chain"}
	a.latest.TravelTarget = &struct {
		DestinationName string    `json:"destination_name"`
		IsTraveling     bool      `json:"is_traveling"`
		IsWorking       bool      `json:"is_working"`
		Arrived         bool      `json:"arrived"`
		Position        []float64 `json:"position"`
		MapID           uint32    `json:"map_id"`
	}{DestinationName: "Goldshire", IsTraveling: true}
	material := a.buildChatContext()
	if material.CurrentTask != "craft_item Copper Chain" {
		t.Fatalf("current task must win over travel: %q", material.CurrentTask)
	}
}

func TestChatPipelineUnknownAudienceSkips(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt4", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"audience":           {Type: "choice", Choice: "person_99", Confidence: 0.95},
	}}
	result := a.owner.runChatPipeline(job)
	if result.communicate || result.skipReason != "unknown_audience" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestChatPipelineWriterFailureSendsCannedLine(t *testing.T) {
	a, commands := chatTestSetup(t)
	chatEvent := event{EventID: "evt5", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"whisper","sender_name":"Peer","message":"hello?"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.95},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.9},
		"channel":            {Type: "choice", Choice: "whisper", Confidence: 0.9},
	}}
	a.owner.chatCompose = &stubChatWriter{err: &stubError{"writer down"}}
	result := a.owner.runChatPipeline(job)
	if !result.communicate || result.source != "canned" || result.message != chatFallbackMessage {
		t.Fatalf("unexpected result: %+v", result)
	}
	a.handleEvent(chatResultEvent(result))
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "send_chat" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	var args map[string]string
	if err := json.Unmarshal(cmd.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	if args["message"] != chatFallbackMessage {
		t.Fatalf("message=%q", args["message"])
	}
}

func TestChatPipelineChannelConstraintFallsBackToSay(t *testing.T) {
	a, _ := chatTestSetup(t)
	a.latest.Bot.GroupSize = 1
	chatEvent := event{EventID: "evt6", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"channel":            {Type: "choice", Choice: "party", Confidence: 0.9},
	}}
	result := a.owner.runChatPipeline(job)
	if !result.communicate || result.channel != "say" || !result.channelFallback {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestChatPipelineEvaluateFailureSendsNothing(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt7", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, _ := a.buildChatJob(chatEvent)
	a.owner.chatEval = &stubChatEvaluator{err: &stubError{"typesafe down"}}
	result := a.owner.runChatPipeline(job)
	if result.communicate || result.skipReason != "evaluate_failed" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestChatBudgetBlocksAndSurvivesRestart(t *testing.T) {
	a, _ := chatTestSetup(t)
	now := time.Now()
	for sent := 0; sent < 5; sent++ {
		// The newest send is 30s old: past the min interval, inside the window.
		a.recordChatSent(now.Add(-30*time.Second - time.Duration(sent)*time.Second))
	}
	if block := a.chatBudgetBlock(now); block != "window_limit" {
		t.Fatalf("block=%q", block)
	}
	// Budgets must survive serialization, not just this actor tick.
	encoded, err := json.Marshal(a.state)
	if err != nil {
		t.Fatal(err)
	}
	var restored persistedAgent
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	a.state = restored
	if block := a.chatBudgetBlock(now); block != "window_limit" {
		t.Fatal("chat window budget did not survive a restart")
	}
	minGap := a.owner.cfg.chatMinInterval
	a.owner.cfg.chatMinInterval = time.Second
	a.state.ChatRecent = nil
	a.recordChatSent(now)
	if block := a.chatBudgetBlock(now.Add(2 * time.Second)); block != "" {
		t.Fatalf("min interval blocked after expiry: %q", block)
	}
	a.owner.cfg.chatMinInterval = minGap
}

func TestChatSchedulingSkipsDeadCombatAndBudget(t *testing.T) {
	a, _ := chatTestSetup(t)
	dead := false
	a.latest.Bot.Alive = &dead
	chatEvent := event{EventID: "evt8", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	if a.scheduleChatPipeline(chatEvent) {
		t.Fatal("dead bot scheduled a chat decision")
	}
	alive := true
	a.latest.Bot.Alive = &alive
	a.latest.Bot.InCombat = true
	if a.scheduleChatPipeline(chatEvent) {
		t.Fatal("combat bot scheduled a chat decision")
	}
	a.latest.Bot.InCombat = false
	now := time.Now()
	for sent := 0; sent < 5; sent++ {
		a.recordChatSent(now.Add(-30*time.Second - time.Duration(sent)*time.Second))
	}
	if a.scheduleChatPipeline(chatEvent) {
		t.Fatal("budget-exhausted bot scheduled a chat decision")
	}
}

func TestTaskToolsFilterRemovesOnlySendChat(t *testing.T) {
	owned := taskTools(agentTools, true)
	for _, tool := range owned {
		if toolName(tool) == "send_chat" {
			t.Fatal("send_chat survived chat ownership")
		}
	}
	if len(owned) != len(agentTools)-1 {
		t.Fatalf("owned tool count %d, want %d", len(owned), len(agentTools)-1)
	}
	unowned := taskTools(agentTools, false)
	if len(unowned) != len(agentTools) {
		t.Fatal("legacy path lost tools")
	}
	negotiates := false
	for _, tool := range owned {
		if toolName(tool) == "negotiate_crafting_trade" {
			negotiates = true
		}
	}
	if !negotiates {
		t.Fatal("trade negotiation lost its message ability")
	}
}

func TestSystemPromptChatOwnershipVariants(t *testing.T) {
	owned := systemPrompt("profile", true)
	if !strings.Contains(owned, "no send_chat tool") {
		t.Fatal("owned prompt does not declare the chat handover")
	}
	if strings.Contains(owned, "must get a visible send_chat reply") {
		t.Fatal("owned prompt still demands chat replies")
	}
	legacy := systemPrompt("profile", false)
	if !strings.Contains(legacy, "must get a visible send_chat reply") {
		t.Fatal("legacy prompt lost the reply directive")
	}
}

func TestChatPipelineDisabledWithoutTypesafeKey(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt10", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.owner.chatEval = nil
	if a.scheduleChatPipeline(chatEvent) {
		t.Fatal("pipeline scheduled without an evaluator")
	}
	// The production wiring must only assign a real client; a typed-nil
	// *typesafeClient inside the interface would count as enabled.
	nilClient := newTypesafeClient(config{typesafeEndpoint: "https://api.typesafe.ai/v1/systemone", typesafeModel: "jev-latest"})
	if nilClient != nil {
		t.Fatal("client constructed without an API key")
	}
	withKey := newTypesafeClient(config{
		typesafeEndpoint: "https://api.typesafe.ai/v1/systemone", typesafeAPIKey: "key", typesafeModel: "jev-latest"})
	if withKey == nil {
		t.Fatal("client not constructed with an API key")
	}
	c := &controller{chatCompose: &stubChatWriter{}}
	if client := newTypesafeClient(config{
		typesafeEndpoint: "https://api.typesafe.ai/v1/systemone", typesafeModel: "jev-latest"}); client != nil {
		c.chatEval = client
	}
	if c.chatEnabled() {
		t.Fatal("pipeline enabled without an API key")
	}
	_ = a
}

func TestChatReceivedFallsBackToDecisionModelWithoutJev(t *testing.T) {
	a, _ := chatTestSetup(t)
	a.owner.chatEval = nil
	a.handleEvent(event{EventID: "evt9", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)})
	if a.pendingDecisionReason != "chat_received" {
		t.Fatal("legacy decision path was not preserved without a Jev client")
	}
	if a.chatPending {
		t.Fatal("pipeline pending flag set without a Jev client")
	}
}

func TestDecisionModelChatAlsoCountsAgainstBudget(t *testing.T) {
	a, commands := chatTestSetup(t)
	now := time.Now()
	for sent := 0; sent < 5; sent++ {
		a.recordChatSent(now.Add(-time.Duration(sent) * time.Second))
	}
	a.applyTool(toolCall{Name: "send_chat", Arguments: `{"channel":"say","message":"spam"}`})
	select {
	case cmd := <-commands:
		t.Fatalf("unexpected command %s while budget exhausted: %s", cmd.Operation, cmd.Operation)
	case <-time.After(100 * time.Millisecond):
	}
}
