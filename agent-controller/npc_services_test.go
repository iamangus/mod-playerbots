package main

import (
	"encoding/json"
	"testing"
)

func npcServiceTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	a, commands := loopTestActor(t)
	a.latest.NearbyNPCs = []npcInfo{{GUID: "123", Name: "Vendor", NPCFlags: npcFlagVendor | npcFlagRepair | npcFlagTrainer | npcFlagFlightMaster,
		ClassTrainer: true, TaxiNodeID: 1, TaxiRoutes: []taxiRouteInfo{{ToNode: 2, Name: "Destination", PriceCopper: 100}}}}
	a.latest.NearbyMailboxes = []mailboxInfo{{GUID: "456", Name: "Mailbox", Distance: 5}}
	return a, commands
}

func TestNPCServicesDispatchBoundedNativeLoops(t *testing.T) {
	for _, tc := range []struct{ tool, args, operation string }{
		{"visit_vendor", `{"npc_guid":"123","service":"repair","max_spend_copper":100}`, "repair_equipment"},
		{"visit_vendor", `{"npc_guid":"123","service":"sell_junk","count":3}`, "sell_junk"},
		{"visit_vendor", `{"npc_guid":"123","service":"buy","item_id":159,"count":3,"max_spend_copper":100}`, "buy_vendor_item"},
		{"train_class_spells", `{"npc_guid":"123","count":3,"max_spend_copper":100}`, "train_class_spells"},
		{"collect_mail", `{"mailbox_guid":"456","count":3}`, "collect_mail"},
		{"send_mail", `{"mailbox_guid":"456","recipient":"Friend","subject":"Hello","max_spend_copper":30}`, "send_mail"},
		{"discover_flight_path", `{"npc_guid":"123"}`, "discover_flight_path"},
		{"take_flight", `{"npc_guid":"123","destination_node":2,"max_spend_copper":100}`, "take_flight"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			a, commands := npcServiceTestActor(t)
			a.applyTool(toolCall{Name: tc.tool, Arguments: tc.args})
			cmd := nextLoopCommand(t, commands)
			if cmd.Operation != tc.operation || a.state.Task.ServiceOperation != tc.operation {
				t.Fatalf("wrong service dispatch: %+v", cmd)
			}
			a.handleTaskOperation(event{Payload: mustJSON(map[string]any{"operation_id": cmd.OperationID, "status": "accepted", "persistent": true})})
			if a.state.Task.OperationID != cmd.OperationID {
				t.Fatal("acceptance released native loop ownership")
			}
			a.handleTaskOperation(event{Payload: mustJSON(map[string]any{"operation_id": cmd.OperationID, "status": "completed", "reason": "postcondition verified"})})
			if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
				t.Fatal("terminal service result did not complete the task")
			}
		})
	}
}

func TestNPCServicesRejectUnsafeArguments(t *testing.T) {
	for _, args := range []string{
		`{"service":"repair","max_spend_copper":100}`,
		`{"npc_guid":"123","service":"repair"}`,
		`{"npc_guid":"123","service":"repair","max_spend_copper":null}`,
		`{"npc_guid":"123","service":"repair","max_spend_copper":-1}`,
		`{"npc_guid":"123","service":"buy","max_spend_copper":100}`,
		`{"npc_guid":"123","service":"sell_junk","count":41}`,
		`{"npc_guid":"123","service":"sell_junk","count":null}`,
		`{"npc_guid":"123","service":"sell_everything"}`,
		`{"npc_guid":"999","service":"sell_junk"}`,
	} {
		t.Run(args, func(t *testing.T) {
			a, _ := npcServiceTestActor(t)
			previous := &task{Kind: "follow_player"}
			a.state.Task = previous
			var parsed map[string]json.RawMessage
			if err := json.Unmarshal([]byte(args), &parsed); err != nil {
				t.Fatal(err)
			}
			a.startNPCServiceTask("visit_vendor", parsed)
			if a.state.Task != previous {
				t.Fatal("unsafe service request replaced an existing task")
			}
		})
	}
}

func TestNPCServiceDoesNotReplayFailure(t *testing.T) {
	a, commands := npcServiceTestActor(t)
	a.applyTool(toolCall{Name: "visit_vendor", Arguments: `{"npc_guid":"123","service":"buy","item_id":159,"count":3,"max_spend_copper":100}`})
	cmd := nextLoopCommand(t, commands)
	a.handleTaskOperation(event{Payload: mustJSON(map[string]any{"operation_id": cmd.OperationID, "status": "rejected", "reason": "budget exhausted"})})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("failed economic operation was scheduled for replay")
	}
}

func TestNPCServiceUncertainOwnerLossBlocksReplay(t *testing.T) {
	a, commands := npcServiceTestActor(t)
	a.applyTool(toolCall{Name: "visit_vendor", Arguments: `{"npc_guid":"123","service":"sell_junk"}`})
	_ = nextLoopCommand(t, commands)
	a.state.OwnerToken = "owner"
	a.handleEvent(event{Type: "bot_online", OwnerToken: "replacement", Payload: json.RawMessage(`{}`)})
	a.handleEvent(event{Type: "snapshot", OwnerToken: "replacement", Payload: json.RawMessage(`{"bot":{},"nearby_npcs":[]}`)})
	if a.state.Task != nil || a.pendingDecisionReason != "task_blocked" {
		t.Fatal("uncertain economic effect was replayed on a replacement core")
	}
}

func TestNPCServiceClonesImmutableArguments(t *testing.T) {
	original := &task{Kind: "npc_service", ServiceArguments: json.RawMessage(`{"count":3}`)}
	copy := cloneTask(original)
	copy.ServiceArguments[0] = '['
	if original.ServiceArguments[0] != '{' {
		t.Fatal("worker clone aliases service arguments")
	}
}

func TestNPCFlagsSurviveModelObservationRoundTrip(t *testing.T) {
	var state snapshot
	if err := json.Unmarshal([]byte(`{"nearby_npcs":[{"guid":"123","npc_flags":4224}]}`), &state); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored snapshot
	if err := json.Unmarshal(encoded, &restored); err != nil || restored.NearbyNPCs[0].NPCFlags != npcFlagVendor|npcFlagRepair {
		t.Fatal("NPC service capability flags were lost")
	}
}

func TestMailCollectionRequiresObservedMailbox(t *testing.T) {
	a, _ := npcServiceTestActor(t)
	a.applyTool(toolCall{Name: "collect_mail", Arguments: `{"mailbox_guid":"unknown"}`})
	if a.state.Task != nil {
		t.Fatal("mail collection started at an unobserved mailbox")
	}
	var state snapshot
	if err := json.Unmarshal([]byte(`{"nearby_mailboxes":[{"guid":"456","name":"Mailbox","distance":5}]}`), &state); err != nil || len(state.NearbyMailboxes) != 1 {
		t.Fatal("mailbox observations were not retained")
	}
}

func TestMailSendRequiresBudgetAndObservedAttachment(t *testing.T) {
	for _, args := range []string{
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello"}`,
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello","max_spend_copper":29}`,
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello","money_copper":100,"max_spend_copper":129}`,
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello","money_copper":null,"max_spend_copper":130}`,
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello","item_guid":"1234","max_spend_copper":30}`,
		`{"mailbox_guid":"456","recipient":"","subject":"Hello","max_spend_copper":30}`,
		`{"mailbox_guid":"456","recipient":"Friend","subject":"Hello\u0000bad","max_spend_copper":30}`,
	} {
		t.Run(args, func(t *testing.T) {
			a, _ := npcServiceTestActor(t)
			a.applyTool(toolCall{Name: "send_mail", Arguments: args})
			if a.state.Task != nil {
				t.Fatal("unsafe mail send started")
			}
		})
	}
}

func TestMailSendPreservesOneWholeStackAndGift(t *testing.T) {
	a, commands := npcServiceTestActor(t)
	a.latest.Inventory.TradeableStacks = json.RawMessage(`[{"item_guid":"1234","item_id":159,"count":5}]`)
	a.applyTool(toolCall{Name: "send_mail", Arguments: `{"mailbox_guid":"456","recipient":"Friend","subject":"Supplies","item_guid":"1234","money_copper":100,"max_spend_copper":130}`})
	cmd := nextLoopCommand(t, commands)
	var args struct {
		ItemGUID string `json:"item_guid"`
		Money    uint32 `json:"money_copper"`
		Budget   uint32 `json:"max_spend_copper"`
		Count    uint32 `json:"count"`
	}
	if json.Unmarshal(cmd.Arguments, &args) != nil || args.ItemGUID != "1234" || args.Money != 100 || args.Budget != 130 || args.Count != 1 {
		t.Fatalf("mail submission changed gift or attachment: %+v", args)
	}
	previous := a.state.Task
	a.applyTool(toolCall{Name: "send_mail", Arguments: `{"mailbox_guid":"456","recipient":"Friend","subject":"Supplies","max_spend_copper":30}`})
	if a.state.Task != previous || a.state.Task.OperationID != cmd.OperationID {
		t.Fatal("repeated send replayed or replaced an in-flight submission")
	}
}
