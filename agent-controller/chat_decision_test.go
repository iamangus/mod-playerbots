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
}

func (stub *stubChatWriter) composeChat(slots chan struct{}, profile string, request chatComposeRequest) (string, error) {
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

func TestChatPipelineLowConfidenceAudienceSkips(t *testing.T) {
	a, _ := chatTestSetup(t)
	chatEvent := event{EventID: "evt3", Type: "chat_received", Timestamp: time.Now().UnixMilli(),
		Payload: json.RawMessage(`{"channel":"say","sender_name":"Peer","message":"hi"}`)}
	a.rememberEvent(chatEvent)
	job, ok := a.buildChatJob(chatEvent)
	if !ok {
		t.Fatal("chat job was not built")
	}
	a.owner.chatEval = &stubChatEvaluator{answers: map[string]typesafeAnswer{
		"should_communicate": {Type: "noul", Noul: 0.9},
		"audience":           {Type: "choice", Choice: "person_0", Confidence: 0.4},
	}}
	result := a.owner.runChatPipeline(job)
	if result.communicate || result.skipReason != "low_confidence" {
		t.Fatalf("unexpected result: %+v", result)
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
