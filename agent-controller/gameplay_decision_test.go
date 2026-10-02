package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type gameplayEvaluatorFunc func(any, map[string]typesafeQuestion) (map[string]typesafeAnswer, error)

func (f gameplayEvaluatorFunc) evaluate(s any, q map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
	return f(s, q)
}

func gameplayFixture(t *testing.T) decisionJob {
	t.Helper()
	var state snapshot
	err := json.Unmarshal([]byte(`{
		"bot":{"name":"Bot","class_id":1,"race_id":1,"alive":true},
		"quests":[{"quest_id":1,"title":"Active quest"}],
		"available_quests":[{"quest_id":2,"title":"Offered quest"}],
		"nearby_creatures":[{"entry":3,"name":"Wolf"}],
		"nearby_players":[{"name":"Human"}],
		"nearby_mailboxes":[{"guid":"mailbox","name":"Mailbox"}],
		"nearby_npcs":[{"guid":"npc","name":"Vendor","taxi_routes":[{"to_node":4,"name":"Town","price_copper":100}],"vendor_offers":[{"item_id":5,"name":"Cloth","price_copper":50}]}],
		"travel_target":{"destination_name":"Town"},
		"crafting_recipes":[{"item_id":6,"reagents":[{"item_id":5,"count":1}]}],
		"crafting_recipe_page":{"next_offset":40},
		"auctions":{"next_cursor":9,"offers":[{"auction_id":7,"item_id":5,"minimum_bid_copper":30,"buyout_copper":50}]},
		"pending_auction_bids":[{"auction_id":7}],
		"trade":{"revision":8},
		"inventory":{"money_copper":1000,"tradeable_stacks":[{"item_guid":"item"}]}
	}`), &state)
	if err != nil {
		t.Fatal(err)
	}
	return decisionJob{profile: "Stable identity", state: state, memories: []string{"Human is my friend"},
		events: []recentEvent{{Type: "chat_received", New: true, Payload: json.RawMessage(`{"sender_name":"Human","message":"Go to Town. Trade for 123 copper."}`)}},
		task:   &task{ID: "persistent", Kind: "fishing"}, trigger: "periodic_check"}
}

func TestEveryGameplayToolAndParameterHasJevDomain(t *testing.T) {
	job := gameplayFixture(t)
	seen := map[string]bool{}
	for _, tool := range gameplayTools() {
		seen[tool.Name] = true
		d := gameplayDomains(tool, job)
		for key := range tool.Properties {
			if len(d[key]) == 0 {
				t.Errorf("%s.%s has no argument selection support", tool.Name, key)
			}
		}
		calls := 0
		p := &gameplayPlanner{evaluator: gameplayEvaluatorFunc(func(state any, questions map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
			calls++
			observation := state.(map[string]any)
			if observation["current_task"] != job.task || observation["events"] == nil || observation["state"] == nil {
				t.Fatal("decision lost state/events/current task")
			}
			if action, ok := questions["action"]; ok {
				if _, offered := action.Criteria.(map[string]any)[tool.Name]; !offered {
					t.Fatalf("%s was not offered", tool.Name)
				}
				return map[string]typesafeAnswer{"action": {Type: "choice", Choice: tool.Name, Confidence: 1}}, nil
			}
			answers := map[string]typesafeAnswer{}
			for key := range questions {
				answers[key] = typesafeAnswer{Type: "choice", Choice: "v0", Confidence: 1}
			}
			return answers, nil
		})}
		call, err := p.decide(job)
		if err != nil || call == nil || call.Name != tool.Name {
			t.Fatalf("%s cannot be realized through Jev: %v %+v", tool.Name, err, call)
		}
		if calls < 1 || calls > 2 {
			t.Fatalf("unbounded decision calls for %s", tool.Name)
		}
		var args map[string]any
		if json.Unmarshal([]byte(call.Arguments), &args) != nil {
			t.Fatal("bad argument JSON")
		}
		for key := range tool.Required {
			if _, ok := args[key]; !ok {
				t.Fatalf("missing %s.%s", tool.Name, key)
			}
		}
	}
	if seen["send_chat"] || len(seen) != len(agentTools)-1 {
		t.Fatal("registry migration is incomplete or exposes chat as a gameplay tool")
	}
}

func TestJevContinueAndInvalidAnswersFailClosed(t *testing.T) {
	for _, choice := range []string{"continue", "invented_tool", "send_chat"} {
		p := &gameplayPlanner{evaluator: gameplayEvaluatorFunc(func(any, map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
			return map[string]typesafeAnswer{"action": {Type: "choice", Choice: choice, Confidence: 1}}, nil
		})}
		call, err := p.decide(gameplayFixture(t))
		if call != nil || (choice != "continue" && err == nil) || (choice == "continue" && err != nil) {
			t.Fatalf("bad action accepted: %s %v %+v", choice, err, call)
		}
	}
	for _, answer := range []typesafeAnswer{{Type: "choice", Choice: "arbitrary JSON", Confidence: 1}, {Type: "choice", Choice: "v999999", Confidence: 1}, {Type: "choice", Choice: "v0", Confidence: 0}} {
		p := &gameplayPlanner{evaluator: gameplayEvaluatorFunc(func(_ any, q map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
			if _, ok := q["action"]; ok {
				return map[string]typesafeAnswer{"action": {Type: "choice", Choice: "accept_quest", Confidence: 1}}, nil
			}
			return map[string]typesafeAnswer{"quest_id": answer}, nil
		})}
		if call, err := p.decide(gameplayFixture(t)); call != nil || err == nil {
			t.Fatal("unoffered argument was accepted")
		}
	}
}

func TestGameplayWorkerUsesJevNotOpenRouter(t *testing.T) {
	requests := 0
	jev := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("bad request")
		}
		if _, ok := body["messages"]; ok {
			t.Error("gameplay sent a text-model request")
		}
		observation := body["state"].(map[string]any)
		if observation["events"] == nil || observation["current_task"] == nil || observation["state"] == nil {
			t.Error("missing uniform decision inputs")
		}
		questions := body["questions"].(map[string]any)
		if _, ok := questions["action"]; ok {
			fmt.Fprint(w, `{"answers":{"action":{"type":"choice","choice":"accept_quest","confidence":1}}}`)
		} else {
			fmt.Fprint(w, `{"answers":{"quest_id":{"type":"choice","choice":"v0","confidence":1}}}`)
		}
	}))
	defer jev.Close()
	openrouter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("gameplay called OpenRouter") }))
	defer openrouter.Close()
	job := gameplayFixture(t)
	c := &controller{modelJobs: make(chan decisionJob, 1), model: newModelClient(config{modelEndpoint: openrouter.URL}),
		chatEval: &typesafeClient{endpoint: jev.URL, timeout: time.Second, client: jev.Client()}}
	a := &actor{inbox: make(chan event, 1), stop: make(chan struct{})}
	job.actor = a
	c.startModelWorkers(1)
	c.modelJobs <- job
	close(c.modelJobs)
	select {
	case e := <-a.inbox:
		if e.decision.err != nil || e.decision.call == nil || e.decision.call.Name != "accept_quest" {
			t.Fatalf("worker migration failed: %+v", e.decision)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not complete")
	}
	if requests != 2 {
		t.Fatalf("expected two typed Jev stages, got %d", requests)
	}
}

func TestJevRequiredAndProfilesNeverCallOpenRouter(t *testing.T) {
	t.Setenv("LLM_ENDPOINT", "http://unused.invalid")
	t.Setenv("LLM_MODEL", "writer")
	t.Setenv("AGENT_TYPESAFE_API_KEY", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "Jev is required") {
		t.Fatal("startup must not silently restore OpenRouter decisioning")
	}
	if profile := deterministicProfile(1, 1, "dps", "population"); profile == "" || profile != deterministicProfile(1, 1, "dps", "population") {
		t.Fatal("profile initialization must be deterministic")
	}
}

func TestPopulationProvisioningDoesNotCallTextModel(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; fmt.Fprint(w, `{"choices":[]}`) }))
	defer server.Close()
	m := testPopulationManager(t, 100)
	m.stop = make(chan struct{})
	m.owner.model = newModelClient(config{modelEndpoint: server.URL, modelName: "writer", requestTimeout: time.Second})
	// No native owner: provisioning stops without dispatch; profile creation
	// happens before owner discovery and must still make zero writer requests.
	m.executeCreation("request", populationReservation{Race: 1, Class: 1, Role: "dps"}, "population_refill")
	if calls != 0 {
		t.Fatal("provisioning used OpenRouter")
	}
}

func TestOpenRouterWriterNeverGetsTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Fatal("bad writer request")
		}
		for _, key := range []string{"tools", "tool_choice", "parallel_tool_calls", "questions"} {
			if _, ok := body[key]; ok {
				t.Errorf("writer received decision field %s", key)
			}
		}
		fmt.Fprint(w, `{"choices":[{"message":{"content":"Hello."}}]}`)
	}))
	defer server.Close()
	writer := newModelClient(config{modelEndpoint: server.URL, requestTimeout: time.Second})
	if message, err := writer.composeChat(make(chan struct{}, 1), "profile", chatComposeRequest{Intent: "greet", Channel: "say"}); err != nil || message != "Hello." {
		t.Fatalf("writer failed: %q %v", message, err)
	}
}

// Explicit opt-in: evaluates choices only, never dispatches gameplay or calls
// the writer. Use a real snapshot fixture to verify production API compatibility.
func TestLiveJevGameplaySmoke(t *testing.T) {
	if os.Getenv("JEV_GAMEPLAY_SMOKE") != "1" {
		t.Skip("live evaluation is opt-in")
	}
	job := gameplayFixture(t)
	if path := os.Getenv("JEV_GAMEPLAY_SNAPSHOT"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &job.state); err != nil {
			t.Fatal(err)
		}
		job.task = nil
		job.events = nil
		job.trigger = "periodic_check"
	}
	client := newTypesafeClient(config{typesafeEndpoint: "https://api.typesafe.ai/v1/systemone",
		typesafeAPIKey: os.Getenv("AGENT_TYPESAFE_API_KEY"), typesafeModel: "jev-latest", chatRequestTimeout: 20 * time.Second})
	if client == nil {
		t.Fatal("live smoke requires configured Jev")
	}
	call, err := (&gameplayPlanner{evaluator: client}).decide(job)
	if err != nil {
		t.Fatal(err)
	}
	if call == nil {
		t.Log("Jev selected continue")
	} else {
		t.Logf("Jev selected %s with typed arguments (not dispatched)", call.Name)
	}
}
