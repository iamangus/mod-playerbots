package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParsePolicyCensus(t *testing.T) {
	payload := json.RawMessage(`{
		"status": "completed", "observed_ms": 1234, "realm_id": 1, "partial": false,
		"bots": [{"guid": 7, "name": "Bot", "race": 1, "class": 1, "level": 3, "faction": 1,
			"online": true, "busy": false, "map_id": 0, "instance_id": 0, "zone_id": 12,
			"x": 1.5, "y": 2.5, "z": 3.5, "observed_ms": 1234}],
		"humans": [{"guid": 42, "name": "Angoo", "faction": 1, "map_id": 0,
			"instance_id": 0, "x": 0.0, "y": 0.0, "zone_id": 12, "observed_ms": 1234}]
	}`)
	census := parsePolicyCensus(payload)
	if census == nil {
		t.Fatal("valid census payload must parse")
	}
	if census.Status != "completed" || len(census.Bots) != 1 || len(census.Humans) != 1 {
		t.Fatalf("census parsed incorrectly: %+v", census)
	}
	if census.Bots[0].GUID != 7 || !census.Bots[0].Online || census.Humans[0].GUID != 42 {
		t.Fatalf("census fields parsed incorrectly: %+v", census)
	}

	// Snapshot payloads are not census payloads.
	if parsePolicyCensus(json.RawMessage(`{"status":"completed","total":1,"counts":[],"zones":[]}`)) != nil {
		t.Fatal("population snapshot payload must not parse as census")
	}
}

func TestPolicyCommitmentExpiry(t *testing.T) {
	m := testPopulationManager(t, 100)
	runtime := &policyRuntime{createTTL: 30 * time.Minute, admitTTL: 30 * time.Minute, arrivedTTL: 24 * time.Hour}
	now := int64(10_000_000)
	stored := []policyCommitment{
		{ID: "fresh-create", RuleID: "r", Kind: policyCommitCreate, State: policyCommitmentPending, CreatedMs: now - 1000},
		{ID: "wedged-create", RuleID: "r", Kind: policyCommitCreate, State: policyCommitmentPending, CreatedMs: now - 61*60_000},
		{ID: "allocated", RuleID: "r", Kind: policyCommitAllocate, BotGUID: 5, State: policyCommitmentPending, CreatedMs: now - 500*60_000},
		{ID: "wedged-admit", RuleID: "r", Kind: policyCommitAdmit, BotGUID: 6, State: policyCommitmentPending, CreatedMs: now - 61*60_000},
		{ID: "fresh-admit", RuleID: "r", Kind: policyCommitAdmit, BotGUID: 7, State: policyCommitmentPending, CreatedMs: now - 1000},
		{ID: "released", RuleID: "r", Kind: policyCommitCreate, State: policyCommitmentReleased},
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.owner.redis.Set(ctx, m.prefixKey("policy:commitments"), encoded, 0).Err(); err != nil {
		t.Fatal(err)
	}
	live := m.loadPolicyCommitments(now, runtime)
	var ids []string
	for _, c := range live {
		ids = append(ids, c.ID)
	}
	joined := strings.Join(ids, ",")
	for _, want := range []string{"fresh-create", "allocated", "fresh-admit"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected %s to survive, got %v", want, ids)
		}
	}
	for _, gone := range []string{"wedged-create", "wedged-admit", "released"} {
		if strings.Contains(joined, gone) {
			t.Fatalf("expected %s to expire or be filtered, got %v", gone, ids)
		}
	}
}

func TestApplyAndLinkPolicyCommitments(t *testing.T) {
	m := testPopulationManager(t, 100)
	added := policyCommitment{ID: "vic/create/1", RuleID: "vic", Kind: policyCommitCreate,
		State: policyCommitmentPending, AreaID: "northshire", CreatedMs: 1}
	m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &added}})

	m.updatePolicyCommitmentBot("vic/create/1", 777)
	stored := m.loadPolicyCommitments(2, &policyRuntime{createTTL: time.Hour, arrivedTTL: 24 * time.Hour})
	if len(stored) != 1 || stored[0].BotGUID != 777 {
		t.Fatalf("creation receipt must link to its commitment: %+v", stored)
	}

	// Release removes the commitment.
	m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "release", ID: "vic/create/1"}})
	if got := m.loadPolicyCommitments(2, &policyRuntime{createTTL: time.Hour}); len(got) != 0 {
		t.Fatalf("released commitment must be removed, got %+v", got)
	}
}

func TestPolicyLogoutGraceMarkers(t *testing.T) {
	m := testPopulationManager(t, 100)
	runtime := &policyRuntime{logoutGrace: 5 * time.Minute}

	if got := m.policyLogoutReady(runtime); len(got) != 0 {
		t.Fatalf("empty marker queue must return nothing, got %v", got)
	}
	m.queuePolicyLogout(11)
	m.queuePolicyLogout(11) // idempotent first-seen
	m.queuePolicyLogout(12)
	if got := m.policyLogoutReady(runtime); len(got) != 0 {
		t.Fatalf("grace window must hold candidates, got %v", got)
	}

	// Backdate one marker past the grace window.
	hctx, hcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer hcancel()
	if err := m.owner.redis.HSet(hctx, m.prefixKey("policy:logout-candidates"), "11",
		time.Now().Add(-10*time.Minute).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	ready := m.policyLogoutReady(runtime)
	if len(ready) != 1 || ready[0] != 11 {
		t.Fatalf("only elapsed candidate should be ready, got %v", ready)
	}
	m.clearPolicyLogout(11)
	if got := m.policyLogoutReady(runtime); len(got) != 0 {
		t.Fatalf("cleared marker must leave the queue, got %v", got)
	}
}

func TestLoadPolicyRuntime(t *testing.T) {
	t.Setenv("AGENT_POPULATION_POLICIES", "")
	if loadPolicyRuntime() != nil {
		t.Fatal("no policies env must keep legacy mode")
	}
	t.Setenv("AGENT_POPULATION_POLICIES", "not json")
	if loadPolicyRuntime() != nil {
		t.Fatal("invalid JSON must keep legacy mode")
	}
	t.Setenv("AGENT_POPULATION_POLICIES", `[{"id":"bad","kind":"starter_area","mode":"maintain","target":5,"areas":["missing"],"enabled":true}]`)
	if loadPolicyRuntime() != nil {
		t.Fatal("invalid rules must keep legacy mode")
	}
	areas := `[{"id":"northshire","name":"Northshire","map_id":0,"zone_id":12,"faction":1,
		"x":0.0,"y":0.0,"z":0.0,"radius_yards":150}]`
	t.Setenv("AGENT_POPULATION_POLICY_AREAS", areas)
	t.Setenv("AGENT_POPULATION_POLICIES", `[{"id":"maint","kind":"starter_area","mode":"maintain",
		"target":5,"areas":["northshire"],"enabled":true}]`)
	runtime := loadPolicyRuntime()
	if runtime == nil {
		t.Fatal("valid rules must enable policy mode")
	}
	if runtime.maxOnline != 500 || runtime.logoutGrace != 5*time.Minute || runtime.maxActions != 40 {
		t.Fatalf("unexpected defaults: %+v", runtime)
	}
	t.Setenv("AGENT_POPULATION_POLICY_MAX_ONLINE", "250")
	t.Setenv("AGENT_POPULATION_POLICY_MAX_ACTIONS", "12")
	t.Setenv("AGENT_POPULATION_POLICY_LOGOUT_GRACE", "90s")
	runtime = loadPolicyRuntime()
	if runtime.maxOnline != 250 || runtime.maxActions != 12 || runtime.logoutGrace != 90*time.Second {
		t.Fatalf("env overrides not applied: %+v", runtime)
	}
}

func TestFinishPolicyCreateParsesReason(t *testing.T) {
	m := testPopulationManager(t, 100)
	added := policyCommitment{ID: "vic/create/1", RuleID: "vic", Kind: policyCommitCreate,
		State: policyCommitmentPending, CreatedMs: 1}
	m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &added}})

	m.finishPolicyCreate("policy:vic/create/1", 555)
	m.finishPolicyCreate("zone_refill", 556) // legacy reasons must be inert
	stored := m.loadPolicyCommitments(2, &policyRuntime{createTTL: time.Hour, arrivedTTL: 24 * time.Hour})
	if len(stored) != 1 || stored[0].BotGUID != 555 {
		t.Fatalf("policy reason must link receipt: %+v", stored)
	}
}
