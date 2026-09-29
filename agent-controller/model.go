package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type modelClient struct {
	endpoint  string
	apiKey    string
	model     string
	timeout   time.Duration
	maxTokens uint32
	client    *http.Client
}

type modelToolCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func newModelClient(cfg config) *modelClient {
	return &modelClient{
		endpoint:  cfg.modelEndpoint,
		apiKey:    cfg.modelAPIKey,
		model:     cfg.modelName,
		timeout:   cfg.requestTimeout,
		maxTokens: cfg.maxTokens,
		client:    &http.Client{Timeout: cfg.requestTimeout},
	}
}

func (client *modelClient) decide(slots chan struct{}, profile string, memories []string,
	state snapshot, events []recentEvent, currentTask *task, trigger string) (*toolCall, error) {
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-time.After(client.timeout):
		return nil, fmt.Errorf("model concurrency limit timed out")
	}

	observation, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"trigger":        trigger,
		"state":          state,
		"events":         events,
		"current_task":   currentTask,
		"memory":         memories,
	})
	if err != nil {
		return nil, fmt.Errorf("encode observation: %w", err)
	}

	requestBody := map[string]any{
		"model":      client.model,
		"max_tokens": client.maxTokens,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt(profile)},
			{"role": "user", "content": string(observation)},
		},
		"tools":               agentTools,
		"tool_choice":         "auto",
		"parallel_tool_calls": false,
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("encode model request: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create model request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if client.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+client.apiKey)
	}
	resp, err := client.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("model request failed: %w", err)
	}
	defer resp.Body.Close()
	responseBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxLLMResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read model response: %w", err)
	}
	if len(responseBytes) > maxLLMResponseBytes {
		return nil, fmt.Errorf("model response exceeded size limit")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("model returned HTTP %d", resp.StatusCode)
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function modelToolCall `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBytes, &decoded); err != nil {
		return nil, fmt.Errorf("decode model response: %w", err)
	}
	if len(decoded.Choices) == 0 || len(decoded.Choices[0].Message.ToolCalls) == 0 {
		return nil, nil
	}
	call := decoded.Choices[0].Message.ToolCalls[0].Function
	if call.Name == "" {
		return nil, fmt.Errorf("model tool call has no name")
	}
	if call.Arguments == "" {
		call.Arguments = "{}"
	}
	return &toolCall{Name: call.Name, Arguments: call.Arguments}, nil
}

func systemPrompt(profile string) string {
	return "You control one persistent World of Warcraft 3.3.5a playerbot. The live state in the observation is authoritative. " +
		"All high-level quest, kill-count, gathering, and social loops are executed by this external agent. AzerothCore " +
		"only executes bounded navigation, combat-target, loot/interact, chat, and group primitives. Combat rotations " +
		"remain in the existing bot AI. Coordinate with other agents only through visible in-game chat; never assume hidden " +
		"state. Events with new=false are context only and must not be acted on again. Do not repeat an operation already " +
		"in progress. Reply in the incoming message's language; SAY/YELL use the bot's faction language. Use remember only " +
		"for durable useful facts, and keep identity stable. Profile: " + profile
}

// generateProfileSync produces a short playstyle persona for a newly
// provisioned bot. It falls back to a deterministic template when the model is
// unavailable so provisioning never blocks on inference.
func (client *modelClient) generateProfileSync(race, class uint32, role, reason string) string {
	fallback := fmt.Sprintf("A steady %s %s who prefers playing %s. Created to join the world (%s). "+
		"Plays predictably, helps nearby players, and works on quests honestly.",
		raceNames[race], classNames[class], role, strings.ReplaceAll(reason, "_", " "))
	if client == nil || client.endpoint == "" || client.model == "" {
		return fallback
	}

	spec := fmt.Sprintf("Create a short identity profile (2-3 sentences) for a newly created World of Warcraft "+
		"3.3.5a character: a %s %s who will usually play as %s. Describe playstyle, priorities, and social "+
		"temperament. Plain text only, no lists.", raceNames[race], classNames[class], role)
	requestBody := map[string]any{
		"model": client.model,
		"max_tokens": 200,
		"messages": []map[string]string{
			{"role": "system", "content": "You create concise, stable personas for persistent game characters. " +
				"Keep the tone grounded and family-friendly."},
			{"role": "user", "content": spec},
		},
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return fallback
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return fallback
	}
	req.Header.Set("Content-Type", "application/json")
	if client.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+client.apiKey)
	}
	resp, err := client.client.Do(req)
	if err != nil {
		return fallback
	}
	defer resp.Body.Close()
	responseBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxLLMResponseBytes+1))
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fallback
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(responseBytes, &decoded) != nil || len(decoded.Choices) == 0 {
		return fallback
	}
	profile := strings.TrimSpace(decoded.Choices[0].Message.Content)
	if profile == "" {
		return fallback
	}
	return trimUTF8(profile, 600)
}

var agentTools = []map[string]any{
	functionTool("send_chat", "Send an ordinary visible in-game chat message.", map[string]any{
		"channel": map[string]any{"type": "string", "enum": []string{"say", "yell", "whisper", "party", "raid", "guild", "officer", "world", "general", "trade", "lfg", "local_defense", "world_defense", "guild_recruitment"}},
		"message": map[string]any{"type": "string"}, "recipient": map[string]any{"type": "string"},
	}, []string{"channel", "message"}),
	functionTool("invite_to_group", "Invite a nearby player to a group.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("accept_group_invite", "Accept the pending group invitation.", map[string]any{}, nil),
	functionTool("decline_group_invite", "Decline the pending group invitation.", map[string]any{}, nil),
	functionTool("navigate_to_player", "Meet a nearby player.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("follow_player", "Follow a nearby player until replaced or cancelled.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("navigate_to_destination", "Travel to a known semantic destination.", map[string]any{
		"destination": map[string]any{"type": "string"},
	}, []string{"destination"}),
	functionTool("work_on_quest", "Run an external quest-objective loop for one active solo quest.", map[string]any{
		"quest_id": map[string]any{"type": "integer"},
	}, []string{"quest_id"}),
	functionTool("kill_count", "Kill and loot a count of nearby creatures with the given entry.", map[string]any{
		"creature_entry": map[string]any{"type": "integer"},
		"count":          map[string]any{"type": "integer"},
		"radius":         map[string]any{"type": "number"},
	}, []string{"creature_entry", "count"}),
	functionTool("gather_resources", "Gather nearby herb, ore, or fishing nodes in a loop.", map[string]any{
		"profession": map[string]any{"type": "string", "enum": []string{"herbalism", "mining", "fishing"}},
		"count":      map[string]any{"type": "integer"},
		"radius":     map[string]any{"type": "number"},
	}, []string{"profession", "count"}),
	functionTool("stop_task", "Cancel the current external task and core primitive.", map[string]any{
		"reason": map[string]any{"type": "string"},
	}, nil),
	functionTool("remember", "Store a short durable fact or commitment.", map[string]any{
		"memory": map[string]any{"type": "string"},
	}, []string{"memory"}),
	functionTool("set_identity", "Set a stable persona/profile for this character.", map[string]any{
		"profile": map[string]any{"type": "string"},
	}, []string{"profile"}),
}

func functionTool(name, description string, properties map[string]any, required []string) map[string]any {
	parameters := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) != 0 {
		parameters["required"] = required
	}
	return map[string]any{"type": "function", "function": map[string]any{
		"name": name, "description": description, "parameters": parameters,
	}}
}
