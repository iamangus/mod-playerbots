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
	endpoint        string
	apiKey          string
	model           string
	timeout         time.Duration
	maxTokens       uint32
	reasoningEffort string
	client          *http.Client
}

type modelToolCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func newModelClient(cfg config) *modelClient {
	return &modelClient{
		endpoint:        cfg.modelEndpoint,
		apiKey:          cfg.modelAPIKey,
		model:           cfg.modelName,
		timeout:         cfg.requestTimeout,
		maxTokens:       cfg.maxTokens,
		reasoningEffort: cfg.reasoningEffort,
		client:          &http.Client{Timeout: cfg.requestTimeout},
	}
}

// reasoningParam caps reasoning-style models. Providers that do not support
// the unified reasoning API ignore it.
func (client *modelClient) reasoningParam() map[string]any {
	if client == nil || client.reasoningEffort == "" {
		return nil
	}
	return map[string]any{"effort": client.reasoningEffort}
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
	if reasoning := client.reasoningParam(); reasoning != nil {
		requestBody["reasoning"] = reasoning
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
		"in progress. Prefer concrete quest or combat progress over repeated acknowledgements. Do not reply to every bot " +
		"party message or wait indefinitely for another bot to lead. If navigation fails, choose a reachable nearby " +
		"objective rather than repeating the same failed destination. Chat does not move you or accept quests: use " +
		"the corresponding task tools, and do not claim an action succeeded without observed results. " +
		"When a player asks you to lead, choose a concrete destination or quest/combat task; following the player is not leading. " +
		"When social_progression is present, use its authoritative friend presence, level band and region preferences. Keep independent goals in the friend's general zone; this is not a follow order. Favor low-XP work while ahead or after confirmed friends-offline grace, normal level-appropriate progression while behind, and never interrupt active group obligations for pacing. Unknown or stale presence is not logout. A friendship comes from an explicit human friend-list relationship, never a casual whisper or proximity. " +
		"Grouped non-leader bots assist their leader automatically; if you are the group leader, choose the group's actual " +
		"task (destination, quest, or combat) when the player asks you to lead — never pick assist_leader for yourself. " +
		"Any message addressed to you from a real player must get a visible send_chat reply in the same channel, even if " +
		"brief, before or alongside any task action; never stay silent when directly addressed. " +
		"If you are the group leader and a player asks you to promote someone, use pass_leadership immediately. " +
		"When asked to fill or grow the group, announce it in /say first (who you are, what you plan, that invites are coming), " +
		"then invite only nearby characters with in_group=false; if the candidates are " +
		"already in another group, say so honestly in chat and name who is actually free. If an invite is rejected because " +
		"the target is already grouped, never retry that target: choose your next action (a kill, gather, quest, or travel " +
		"task) or explain in chat. When you are not in a group and " +
		"players are nearby, offer to group up in /say from time to time and invite anyone who responds or asks. " +
		"You can invite by name anyone you have seen in chat, even far away; invites are not range limited. " +
		"The snapshot lists quests you can take nearby in available_quests; to grow your quest log pick one and call " +
		"accept_quest, then work_on_quest on it. " +
		"Reply to any message in the same chat channel it was said in, whether the sender is a player or another bot. " +
		"Direct requests from real players outrank your current task: acknowledge and act on them. " +
		"If a player asks to join your group and it is full (5/5), say so in that channel and " +
		"point them to a bot outside the group; if you can invite, invite immediately instead of just agreeing in chat. " +
		"A pending trade is not necessarily an open window: check trade.window_open, and open it before asking the player to offer items. " +
		"Reply in the incoming message's language; SAY/YELL use the bot's faction language. Use remember only " +
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
		"model":      client.model,
		"max_tokens": 200,
		"messages": []map[string]string{
			{"role": "system", "content": "You create concise, stable personas for persistent game characters. " +
				"Keep the tone grounded and family-friendly."},
			{"role": "user", "content": spec},
		},
	}
	if reasoning := client.reasoningParam(); reasoning != nil {
		requestBody["reasoning"] = reasoning
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
	functionTool("negotiate_crafting_trade", "Continue an active craft_item trade-discovery parent after a seller replies. Interpret recent chat to agree a total price within the parent's budget, then choose an observed nearby seller and whisper message asking them to open the trade at the meeting. The parent meets them, opens their pending trade, sets the exact agreed money once, waits for their acceptance, validates all requested material stacks and verifies the transfer. No inventory inspection of other players, automatic counteroffers or uncertain replay.", map[string]any{
		"player_name": map[string]any{"type": "string"}, "price_copper": map[string]any{"type": "integer", "minimum": 0}, "message": map[string]any{"type": "string", "minLength": 1, "maxLength": 255},
	}, []string{"player_name", "price_copper", "message"}),
	functionTool("bid_auction", "Place one exact bid below buyout on a fresh observed listing, with explicit bid_copper and maximum total budget. Native listing identity/minimum price and escrow deduction must be verified. Submission is not inventory. A persisted pending_auction_bids journal tracks the win/refund; no automatic rebidding. Up to 8 outstanding bids. Native proceeds are checked opportunistically at observed mailboxes when otherwise idle.", map[string]any{
		"auction_id": map[string]any{"type": "integer", "minimum": 1}, "item_id": map[string]any{"type": "integer", "minimum": 1}, "bid_copper": map[string]any{"type": "integer", "minimum": 1}, "max_spend_copper": map[string]any{"type": "integer", "minimum": 1},
	}, []string{"auction_id", "item_id", "bid_copper", "max_spend_copper"}),
	functionTool("collect_auction_proceeds", "When idle, check one pending bid's exact auction-won attachment or outbid/cancellation refund at an observed mailbox. Native mail identity, quantity and escrow refund must match the journal. Won items additionally require correlated inventory. Empty/delayed mail does not prove a loss, refund or win and never authorizes rebidding.", map[string]any{
		"auction_id": map[string]any{"type": "integer", "minimum": 1}, "mailbox_guid": map[string]any{"type": "string"},
	}, []string{"auction_id", "mailbox_guid"}),
	functionTool("search_auctions", "Inspect bounded pages for up to 8 distinct item_ids at an observed auctioneer. A persistent parent advances native page children without model calls. At most 16 pages per item and 160 cached offers per item; cached_auction_items.partial marks retained partial evidence. Fresh pages for different materials are retained independently. No spending.", map[string]any{
		"npc_guid": map[string]any{"type": "string"}, "item_ids": map[string]any{"type": "array", "minItems": 1, "maxItems": 8, "items": map[string]any{"type": "integer", "minimum": 1}},
	}, []string{"npc_guid", "item_ids"}),
	functionTool("inspect_auctions", "Travel to an observed nearby auctioneer and inspect one bounded public-book page for item_id. Use auctions.next_cursor while auctions.more is true; prices are only observed hints and purchases revalidate them. This replaces ordinary work, not an active crafting/purchase parent. It never spends money.", map[string]any{
		"npc_guid": map[string]any{"type": "string"}, "item_id": map[string]any{"type": "integer", "minimum": 1}, "cursor": map[string]any{"type": "integer", "minimum": 0},
	}, []string{"npc_guid", "item_id"}),
	functionTool("buy_auction", "Acquire count additional items via buyouts from a fresh observed auction page, under one explicit total copper budget. Requires an observed nearby auctioneer and mailbox. Whole auction stacks may exceed count. Purchase, exact won-mail collection, and correlated inventory checks run as persistent children. Bids and uncertain repurchases are not supported.", map[string]any{
		"item_id": map[string]any{"type": "integer", "minimum": 1}, "count": map[string]any{"type": "integer", "minimum": 1, "maximum": 40}, "max_spend_copper": map[string]any{"type": "integer", "minimum": 0},
	}, []string{"item_id", "count", "max_spend_copper"}),
	functionTool("inspect_crafting_recipes", "Request live known crafting recipes without replacing the current task. Browse 40-recipe pages using offset and crafting_recipe_page.next_offset, or inspect item_id with its known subcraft dependencies. Do not combine nonzero offset with item_id. Focused results are bounded; dependencies_truncated warns about omitted work. Omit both arguments to return to the first general page.", map[string]any{
		"offset":  map[string]any{"type": "integer", "minimum": 0, "maximum": 4096},
		"item_id": map[string]any{"type": "integer", "minimum": 1},
	}, nil),
	functionTool("craft_item", "Produce count additional items from an observed deterministic recipe with known subcraft dependencies. Deficits use vendors, actual-loot gathering evidence (including bounded ordinary travel to fresh same-map observed sites), or cached auction offers with an observed mailbox. Missing auction evidence can nest bounded multi-item search at an observed auctioneer. Purchases share one explicit max_spend_copper budget. An already negotiated partner-accepted bundle of up to 6 material types and 40 items may instead use that budget; no bot items or enchant services are given away. Auction children collect exact won mail. All children check correlated inventory. No free reagents or replay of uncertain spending/acceptance.", map[string]any{
		"procurement":           map[string]any{"type": "string", "enum": []string{"auto", "trade"}, "description": "Trade mode starts a ten-minute seller-discovery/negotiation child; use chat and negotiate_crafting_trade after a seller replies."},
		"trade_request_message": map[string]any{"type": "string", "minLength": 1, "maxLength": 255},
		"trade_request_channel": map[string]any{"type": "string", "enum": []string{"trade", "say", "world"}},
		"item_id":               map[string]any{"type": "integer", "minimum": 1},
		"count":                 map[string]any{"type": "integer", "minimum": 1, "maximum": 40},
		"max_spend_copper":      map[string]any{"type": "integer", "minimum": 0},
	}, []string{"item_id", "count"}),
	functionTool("send_mail", "Visit an observed mailbox and submit one ordinary non-COD mail to recipient. Optionally send money_copper and one whole stack identified by item_guid from tradeable_stacks. Explicit max_spend_copper must cover the gift plus 30 copper postage. Submission is verified, not recipient delivery; never automatically replay an uncertain send.", map[string]any{
		"mailbox_guid":     map[string]any{"type": "string"},
		"recipient":        map[string]any{"type": "string", "minLength": 1, "maxLength": 48},
		"subject":          map[string]any{"type": "string", "minLength": 1, "maxLength": 128},
		"body":             map[string]any{"type": "string", "maxLength": 4096},
		"item_guid":        map[string]any{"type": "string"},
		"money_copper":     map[string]any{"type": "integer", "minimum": 0},
		"max_spend_copper": map[string]any{"type": "integer", "minimum": 30},
	}, []string{"mailbox_guid", "recipient", "subject", "max_spend_copper"}),
	functionTool("discover_flight_path", "Visit an observed flight master and discover its node using ordinary taxi interaction.", map[string]any{
		"npc_guid": map[string]any{"type": "string"},
	}, []string{"npc_guid"}),
	functionTool("take_flight", "Fly from an observed flight master to a known direct destination from its taxi_routes, paying the normal fare within an explicit budget. The loop completes at arrival, not takeoff.", map[string]any{
		"npc_guid":         map[string]any{"type": "string"},
		"destination_node": map[string]any{"type": "integer", "minimum": 1},
		"max_spend_copper": map[string]any{"type": "integer", "minimum": 0},
	}, []string{"npc_guid", "destination_node", "max_spend_copper"}),
	functionTool("collect_mail", "Visit an observed mailbox and collect at most count delivered non-COD attachments or money transfers. Does not delete, return, send, or pay COD mail; each native transfer is verified on the world thread.", map[string]any{
		"mailbox_guid": map[string]any{"type": "string"},
		"count":        map[string]any{"type": "integer", "minimum": 1, "maximum": 40},
	}, []string{"mailbox_guid"}),
	functionTool("fish_count", "Catch and acquire count fishing loot results at the current nearby water. Requires learned fishing and an equipped fishing pole. The local loop faces water, casts, waits for its own bobber's bite, reels, and verifies inventory gain. No free equipment or skill is granted.", map[string]any{
		"count": map[string]any{"type": "integer", "minimum": 1, "maximum": 40},
	}, []string{"count"}),
	functionTool("visit_vendor", "Visit an observed NPC to repair equipped gear, sell at most count grey junk stacks, or buy count vendor bundles of item_id. Spending operations require an explicit copper budget; the native loop rechecks eligibility and never exceeds that budget.", map[string]any{
		"npc_guid":         map[string]any{"type": "string"},
		"service":          map[string]any{"type": "string", "enum": []string{"repair", "sell_junk", "buy"}},
		"item_id":          map[string]any{"type": "integer", "minimum": 1},
		"count":            map[string]any{"type": "integer", "minimum": 1, "maximum": 40},
		"max_spend_copper": map[string]any{"type": "integer", "minimum": 0},
	}, []string{"npc_guid", "service"}),
	functionTool("train_class_spells", "Visit an observed class trainer and learn at most count eligible class spells within an explicit copper budget. Does not choose or learn professions.", map[string]any{
		"npc_guid":         map[string]any{"type": "string"},
		"count":            map[string]any{"type": "integer", "minimum": 1, "maximum": 40},
		"max_spend_copper": map[string]any{"type": "integer", "minimum": 0},
	}, []string{"npc_guid", "max_spend_copper"}),
	functionTool("send_chat", "Send an ordinary visible in-game chat message.", map[string]any{
		"channel": map[string]any{"type": "string", "enum": []string{"say", "yell", "whisper", "party", "raid", "guild", "officer", "world", "general", "trade", "lfg", "local_defense", "world_defense", "guild_recruitment"}},
		"message": map[string]any{"type": "string"}, "recipient": map[string]any{"type": "string"},
	}, []string{"channel", "message"}),
	functionTool("invite_to_group", "Invite a nearby player to a group.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("accept_group_invite", "Accept the pending group invitation.", map[string]any{}, nil),
	functionTool("decline_group_invite", "Decline the pending group invitation.", map[string]any{}, nil),
	functionTool("begin_trade", "Open the pending trade window. Use the current trade.revision; pause travel while trading.", map[string]any{
		"trade_revision": map[string]any{"type": "integer"},
	}, []string{"trade_revision"}),
	functionTool("accept_trade", "Accept the currently observed trade offer. Inspect both offers first; never accept an unseen or changed offer.", map[string]any{
		"trade_revision": map[string]any{"type": "integer"},
	}, []string{"trade_revision"}),
	functionTool("cancel_trade", "Cancel the current trade.", map[string]any{
		"trade_revision": map[string]any{"type": "integer"},
	}, []string{"trade_revision"}),
	functionTool("offer_trade_money", "Set the copper offered in the current trade; this does not accept the trade.", map[string]any{
		"trade_revision": map[string]any{"type": "integer"},
		"money_copper":   map[string]any{"type": "integer", "minimum": 0},
	}, []string{"trade_revision", "money_copper"}),
	functionTool("offer_trade_item", "Offer one entire carried stack from inventory.tradeable_stacks. Does not accept the trade.", map[string]any{
		"trade_revision": map[string]any{"type": "integer"},
		"item_guid":      map[string]any{"type": "string"},
		"trade_slot":     map[string]any{"type": "integer", "minimum": 0, "maximum": 5},
	}, []string{"trade_revision", "item_guid", "trade_slot"}),
	functionTool("navigate_to_player", "Meet a nearby player.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("follow_player", "Follow a nearby player until replaced or cancelled.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("assist_leader", "If you are in a group, follow your group leader, fight whatever they fight, and help any group member under attack until told otherwise.", map[string]any{}, nil),
	functionTool("pass_leadership", "If you are the group leader, make another current member the group leader.", map[string]any{
		"player_name": map[string]any{"type": "string"},
	}, []string{"player_name"}),
	functionTool("navigate_to_destination", "Travel to a known semantic destination.", map[string]any{
		"destination": map[string]any{"type": "string"},
	}, []string{"destination"}),
	functionTool("work_on_quest", "Run an external quest-objective loop for one active solo quest.", map[string]any{
		"quest_id": map[string]any{"type": "integer"},
	}, []string{"quest_id"}),
	functionTool("accept_quest", "Travel to a nearby quest giver and accept one quest from available_quests.", map[string]any{
		"quest_id": map[string]any{"type": "integer"},
	}, []string{"quest_id"}),
	functionTool("kill_count", "Kill and loot a count of nearby creatures with the given entry.", map[string]any{
		"creature_entry": map[string]any{"type": "integer"},
		"count":          map[string]any{"type": "integer"},
		"radius":         map[string]any{"type": "number"},
	}, []string{"creature_entry", "count"}),
	functionTool("loot_nearby", "Loot a bounded batch of observed nearby corpses (at most 12), nearest first. Does not attack, wander, skin, or gather. Completion reports corpses processed, not items gained.", map[string]any{
		"radius": map[string]any{"type": "number", "minimum": 1, "maximum": 100},
	}, nil),
	functionTool("gather_resources", "Gather nearby herb or ore nodes in a loop.", map[string]any{
		"profession": map[string]any{"type": "string", "enum": []string{"herbalism", "mining"}},
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
