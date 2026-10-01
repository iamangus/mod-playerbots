package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// A protocol stub captures controller commands without a live game/cluster.
func loopTestActor(t *testing.T) (*actor, <-chan command) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	commands := make(chan command, 32)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, `INFO {"server_id":"loop-test","max_payload":1048576}`+"\r\n")
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			switch fields[0] {
			case "PING":
				_, _ = io.WriteString(conn, "PONG\r\n")
			case "PUB":
				size, err := strconv.Atoi(fields[len(fields)-1])
				if err != nil {
					return
				}
				payload := make([]byte, size+2)
				if _, err := io.ReadFull(reader, payload); err != nil {
					return
				}
				var cmd command
				if json.Unmarshal(payload[:size], &cmd) == nil {
					commands <- cmd
				}
			}
		}
	}()
	nc, err := nats.Connect("nats://"+listener.Addr().String(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	owner := testPopulationManager(t, 1).owner
	owner.nats = nc
	owner.cfg.subjectPrefix = "test"
	a := &actor{owner: owner, botGUID: "bot", ownerToken: "owner", online: true, bridgeActive: true, hasSnapshot: true}
	a.state.Profile = "test"
	return a, commands
}

func nextLoopCommand(t *testing.T, commands <-chan command) command {
	t.Helper()
	select {
	case cmd := <-commands:
		return cmd
	case <-time.After(time.Second):
		t.Fatal("task did not advance without a supervision timer")
	}
	return command{}
}

func TestAssistLeaderStartsPersistentPrimitive(t *testing.T) {
	a, commands := loopTestActor(t)
	a.latest.Bot.GroupSize = 2
	a.startAssistLeaderTask()
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "assist_leader" || cmd.OperationID != a.state.Task.OperationID {
		t.Fatal("assist_leader did not start one persistent loop")
	}
	// Not in a group: reject instead of starting a loop.
	b, _ := loopTestActor(t)
	b.latest.Bot.GroupSize = 1
	b.startAssistLeaderTask()
	if b.state.Task != nil {
		t.Fatal("assist_leader started without a group")
	}
}

func TestGroupedBotAutoAssistsWithoutTask(t *testing.T) {
	a, commands := loopTestActor(t)
	a.latest.Bot.GUID = "GUID_Full__0x0000000000000001_Type__Player_Low__1"
	a.latest.Bot.GroupLeaderGUID = "GUID_Full__0x0000000000000002_Type__Player_Low__2"
	a.latest.Bot.GroupSize = 2
	a.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(
		`{"schema_version":1,"bot":{"guid":"GUID_Full__0x0000000000000001_Type__Player_Low__1","group_leader_guid":"GUID_Full__0x0000000000000002_Type__Player_Low__2","group_size":2}}`)})
	if a.state.Task == nil || a.state.Task.Kind != "assist_leader" {
		t.Fatal("grouped bot without a task did not start assisting")
	}
	if cmd := nextLoopCommand(t, commands); cmd.Operation != "assist_leader" {
		t.Fatalf("operation=%s", cmd.Operation)
	}
	// The group leader never assists; leading is its own job.
	b, _ := loopTestActor(t)
	b.latest.Bot.GUID = "GUID_Full__0x0000000000000001_Type__Player_Low__1"
	b.latest.Bot.GroupLeaderGUID = b.latest.Bot.GUID
	b.latest.Bot.GroupSize = 2
	b.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(
		`{"schema_version":1,"bot":{"guid":"GUID_Full__0x0000000000000001_Type__Player_Low__1","group_leader_guid":"GUID_Full__0x0000000000000001_Type__Player_Low__1","group_size":2}}`)})
	if b.state.Task != nil {
		t.Fatal("leader was assigned assist_leader automatically")
	}
	// An explicit task wins over the automatic default.
	c, _ := loopTestActor(t)
	c.latest.Bot.GroupSize = 2
	c.state.Task = &task{Kind: "kill_count", Phase: "combat", OperationID: "op", LastProgressUTC: time.Now()}
	c.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(`{"schema_version":1,"bot":{"guid":"GUID_Full__0x0000000000000003_Type__Player_Low__3","group_leader_guid":"GUID_Full__0x0000000000000002_Type__Player_Low__2","group_size":2}}`)})
	if c.state.Task.Kind != "kill_count" {
		t.Fatal("automatic assist replaced an explicit task")
	}
}

func TestFollowStartsOnePersistentPrimitive(t *testing.T) {
	a, commands := loopTestActor(t)
	a.latest.NearbyPlayers = []playerInfo{{GUIDRaw: "1", Name: "Human"}}
	a.startPlayerNavigationTask("follow_player", map[string]json.RawMessage{"player_name": json.RawMessage(`"Human"`)})
	cmd := nextLoopCommand(t, commands)
	if cmd.Operation != "follow_player" || cmd.OperationID != a.state.Task.OperationID {
		t.Fatal("follow did not start its own persistent loop")
	}
	if err := a.owner.nats.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case cmd := <-commands:
		t.Fatalf("unexpected second primitive: %s", cmd.Operation)
	default:
	}
}

func TestTaskTransitionsUseTerminalEvents(t *testing.T) {
	for _, tc := range []struct {
		kind, phase, nextPhase, nextOperation string
		goal                                  uint32
	}{
		{"kill_count", "approaching", "combat", "engage_target", 3},
		{"kill_count", "combat", "loot", "loot_target", 3},
		{"quest", "combat", "quest_loot", "loot_target", 0},
		{"kill_count", "loot", "select", "snapshot", 3},
		{"gather_resources", "gathering", "select", "snapshot", 3},
		{"quest", "interacting", "select", "snapshot", 0},
		{"quest", "travel", "select", "snapshot", 0},
		{"quest", "turnin_travel", "turnin_giver", "snapshot", 0},
		{"quest", "turnin_interaction", "verify_turnin", "snapshot", 0},
		{"kill_count", "scanning", "select", "snapshot", 3},
	} {
		t.Run(tc.kind+"/"+tc.phase, func(t *testing.T) {
			a, commands := loopTestActor(t)
			a.state.Task = &task{Kind: tc.kind, Phase: tc.phase, OperationID: "op", TargetGUID: "1", GoalCount: tc.goal, LastScanUTC: time.Now()}
			a.handleTaskOperation(event{Payload: json.RawMessage(`{"operation_id":"op","status":"completed","persistent":true}`)})
			if a.state.Task.Phase != tc.nextPhase {
				t.Fatalf("phase=%s", a.state.Task.Phase)
			}
			if cmd := nextLoopCommand(t, commands); cmd.Operation != tc.nextOperation {
				t.Fatalf("operation=%s", cmd.Operation)
			}
			if tc.phase == "scanning" && !a.state.Task.LastScanUTC.IsZero() {
				t.Fatal("search completion retained an artificial cooldown")
			}
		})
	}
}

func TestNavigationTerminalEventsCompleteTasks(t *testing.T) {
	for _, tc := range []struct{ kind, phase string }{{"navigate_player", "moving"}, {"navigate_destination", "travel"}} {
		t.Run(tc.kind, func(t *testing.T) {
			a, _ := loopTestActor(t)
			a.state.Task = &task{Kind: tc.kind, Phase: tc.phase, OperationID: "op"}
			a.handleTaskOperation(event{Payload: json.RawMessage(`{"operation_id":"op","status":"completed","persistent":true}`)})
			if a.state.Task != nil || a.pendingDecisionReason != "task_completed" {
				t.Fatal("arrival did not complete the higher-level task")
			}
		})
	}
}

func TestPersistentOperationsRetainOwnershipOnAcceptance(t *testing.T) {
	for _, tc := range []struct{ kind, phase, operation string }{
		{"follow_player", "moving", "follow_player"},
		{"navigate_player", "moving", "navigate_to_player"},
		{"navigate_destination", "travel", "navigate_to_destination"},
		{"kill_count", "approaching", "approach_target"},
		{"kill_count", "combat", "engage_target"},
		{"kill_count", "loot", "loot_target"},
		{"kill_count", "scanning", "move_random"},
		{"gather_resources", "gathering", "gather_target"},
		{"quest", "travel", "navigate_to_quest_objective"},
		{"quest", "turnin_travel", "navigate_to_quest_turnin"},
		{"quest", "interacting", "use_gameobject"},
		{"quest", "turnin_interaction", "interact_quest_giver"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot", ownerToken: "owner"}
			a.state.Task = &task{Kind: tc.kind, Phase: tc.phase, OperationID: "op", LastProgressUTC: time.Now()}
			a.handleTaskOperation(event{Payload: mustJSON(map[string]any{
				"operation_id": "op", "operation": tc.operation, "status": "accepted", "persistent": true,
			})})
			if a.state.Task.OperationID != "op" || a.state.Task.Phase != tc.phase || a.state.Task.Completed != 0 {
				t.Fatal("acceptance completed or released the lower-level loop")
			}
		})
	}
}

func TestCompletedLootAndGatherReturnToSelection(t *testing.T) {
	for _, tc := range []struct{ kind, phase string }{{"kill_count", "loot"}, {"gather_resources", "gathering"}} {
		t.Run(tc.kind, func(t *testing.T) {
			a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot"}
			a.state.Task = &task{Kind: tc.kind, Phase: tc.phase, OperationID: "op", GoalCount: 4, TargetGUID: "target"}
			a.handleTaskOperation(event{Payload: json.RawMessage(`{"operation_id":"op","status":"completed","persistent":true}`)})
			if a.state.Task.Completed != 1 || a.state.Task.Phase != "select" || a.state.Task.TargetGUID != "" {
				t.Fatal("terminal result did not advance exactly one iteration")
			}
			a.handleTaskOperation(event{Payload: json.RawMessage(`{"operation_id":"op","status":"completed","persistent":true}`)})
			if a.state.Task.Completed != 1 {
				t.Fatal("duplicate terminal result counted twice")
			}
		})
	}
}

func TestUnsolicitedSnapshotDoesNotReleasePersistentOperation(t *testing.T) {
	a := &actor{owner: testPopulationManager(t, 1).owner, botGUID: "bot", online: true, bridgeActive: true}
	a.state.Profile = "test"
	a.state.Task = &task{Kind: "follow_player", Phase: "moving", OperationID: "op", LastProgressUTC: time.Now()}
	a.handleEvent(event{Type: "snapshot", Payload: json.RawMessage(`{"schema_version":1,"bot":{"level":2}}`)})
	if a.latest.Bot.Level != 2 || a.state.Task.OperationID != "op" {
		t.Fatal("observation replaced the running follow loop")
	}
}
