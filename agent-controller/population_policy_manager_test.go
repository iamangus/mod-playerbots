package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentPolicyReceiptLinksPreserveAllCommitments(t *testing.T) {
	m := testPopulationManager(t, 100)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b", "c", "d"} {
		c := policyCommitment{ID: id, Kind: policyCommitCreate, CreatedMs: 1}
		m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &c}})
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			m.updatePolicyCommitmentBot(id, 777)
		}(id)
	}
	c := policyCommitment{ID: "e", Kind: policyCommitCreate, CreatedMs: 1}
	m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &c}})
	wg.Wait()
	live := m.loadPolicyCommitments(2, &policyRuntime{createTTL: time.Hour, arrivedTTL: time.Hour})
	if len(live) != 5 {
		t.Fatalf("concurrent receipts must not lose commitments: %+v", live)
	}
	for _, c := range live {
		if c.ID != "e" && c.BotGUID != 777 {
			t.Fatalf("lost receipt link: %+v", c)
		}
	}
}

func TestParsePolicyCensus(t *testing.T) {
	payload := json.RawMessage(`{
		"status": "completed", "observed_ms": 1234, "realm_id": 1, "partial": false,
		"bots": [{"guid": 7, "name": "Bot", "race": 1, "class": 1, "level": 3, "faction": 1,
			"online": true, "busy": false, "map_id": 0, "instance_id": 0, "zone_id": 12,
			"x": 1.5, "y": 2.5, "z": 3.5, "observed_ms": 1234}],
		"humans": [{"guid": 42, "name": "Angoo", "faction": 1, "online": true, "map_id": 0,
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
	// Live-session humans are always online; the native census must state
	// it explicitly because the planner skips offline humans.
	if !census.Humans[0].Online {
		t.Fatal("census humans must carry online:true or the planner ignores all demand")
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
	t.Setenv("AGENT_POPULATION_POLICY_RECONCILE_INTERVAL", "")
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
	if runtime.reconcileInterval != 30*time.Second {
		t.Fatalf("policy census must default to 30s, got %s", runtime.reconcileInterval)
	}
	t.Setenv("AGENT_POPULATION_POLICY_RECONCILE_INTERVAL", "1s")
	if got := loadPolicyRuntime().reconcileInterval; got != 15*time.Second {
		t.Fatalf("cadence must respect the native 15s throttle, got %s", got)
	}
	t.Setenv("AGENT_POPULATION_POLICY_MAX_ONLINE", "250")
	t.Setenv("AGENT_POPULATION_POLICY_MAX_ACTIONS", "12")
	t.Setenv("AGENT_POPULATION_POLICY_LOGOUT_GRACE", "90s")
	runtime = loadPolicyRuntime()
	if runtime.maxOnline != 250 || runtime.maxActions != 12 || runtime.logoutGrace != 90*time.Second {
		t.Fatalf("env overrides not applied: %+v", runtime)
	}
}

func TestPolicyLoginKickCoalescesAndSuppressesLegacyCohort(t *testing.T) {
	m := testPopulationManager(t, 100)
	m.policy = &policyRuntime{}
	m.policyKick = make(chan struct{}, 1)
	m.cfg.cohortEnabled = true
	for i := 0; i < 3; i++ {
		if err := m.handlePopulationEvent([]byte(`{"type":"player_entered_world"}`)); err != nil {
			t.Fatal(err)
		}
		m.handlePlayerFirstEntry(event{})
	}
	if len(m.policyKick) != 1 {
		t.Fatal("login events must coalesce into one wakeup")
	}
	keys, err := m.owner.redis.Keys(context.Background(), m.prefixKey("cohort:*")).Result()
	if err != nil || len(keys) != 0 {
		t.Fatalf("policy mode must not create legacy cohorts: %v %v", keys, err)
	}
}

func TestPolicyUndispatchedCreateReleasesCommitment(t *testing.T) {
	m := testPopulationManager(t, 100)
	m.policy = &policyRuntime{areas: []policyArea{{ID: "start", ZoneID: 12}}}
	m.createSlot = make(chan struct{}, 1)
	m.createSlot <- struct{}{}
	c := policyCommitment{ID: "queued", Kind: policyCommitCreate, CreatedMs: 1}
	m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &c}})
	m.executePolicyCreate(policyAction{AreaID: "start", CommitID: c.ID})
	if got := m.loadPolicyCommitments(2, &policyRuntime{createTTL: time.Hour}); len(got) != 0 {
		t.Fatalf("undispatched work must not satisfy demand: %+v", got)
	}
}

// livePlayerVicinityPolicies and liveStarterAreas are the exact JSON strings
// deployed to the cluster for TASK-016's first canary: one player_vicinity
// rule, 300 bots within 300 yards of every fresh online human, replenished
// by level-1 newcomers at the closest eligible starter. Start positions come
// from acore_world.playercreateinfo; faction encoding matches the native
// census (1 = Alliance, 2 = Horde). This test pins the deployed strings so a
// schema change cannot silently invalidate live configuration.
const livePlayerVicinityPolicies = `[{"id":"vicinity","kind":"player_vicinity","target":300,"radius_yards":300,"enabled":true}]`

const liveStarterAreas = `[` +
	`{"id":"elwynn","name":"Elwynn Forest (Human start)","map_id":0,"zone_id":12,"area_id":12,"faction":1,"x":-8949.95,"y":-132.49,"z":83.53,"radius_yards":300},` +
	`{"id":"durotar","name":"Durotar (Orc/Troll start)","map_id":1,"zone_id":14,"area_id":14,"faction":2,"x":-618.52,"y":-4251.67,"z":38.72,"radius_yards":300},` +
	`{"id":"dun-morogh","name":"Dun Morogh (Dwarf/Gnome start)","map_id":0,"zone_id":1,"area_id":1,"faction":1,"x":-6240.32,"y":331.03,"z":382.76,"radius_yards":300},` +
	`{"id":"teldrassil","name":"Teldrassil (Night Elf start)","map_id":1,"zone_id":141,"area_id":141,"faction":1,"x":10311.30,"y":832.46,"z":1326.41,"radius_yards":300},` +
	`{"id":"tirisfal","name":"Tirisfal Glades (Undead start)","map_id":0,"zone_id":85,"area_id":85,"faction":2,"x":1676.71,"y":1678.31,"z":121.67,"radius_yards":300},` +
	`{"id":"mulgore","name":"Mulgore (Tauren start)","map_id":1,"zone_id":215,"area_id":215,"faction":2,"x":-2917.58,"y":-257.98,"z":53.00,"radius_yards":300},` +
	`{"id":"eversong","name":"Eversong Woods (Blood Elf start)","map_id":530,"zone_id":3430,"area_id":3431,"faction":2,"x":10349.60,"y":-6357.29,"z":33.40,"radius_yards":300},` +
	`{"id":"azuremyst","name":"Azuremyst Isle (Draenei start)","map_id":530,"zone_id":3483,"area_id":3526,"faction":1,"x":-3961.64,"y":-13931.20,"z":100.62,"radius_yards":300}]`

func TestLivePlayerVicinityConfiguration(t *testing.T) {
	t.Setenv("AGENT_POPULATION_POLICIES", livePlayerVicinityPolicies)
	t.Setenv("AGENT_POPULATION_POLICY_AREAS", liveStarterAreas)
	t.Setenv("AGENT_POPULATION_POLICY_MAX_ONLINE", "300")
	runtime := loadPolicyRuntime()
	if runtime == nil {
		t.Fatal("deployed vicinity configuration must enable policy mode")
	}
	if len(runtime.rules) != 1 {
		t.Fatalf("exactly one rule expected, got %+v", runtime.rules)
	}
	rule := runtime.rules[0]
	if rule.ID != "vicinity" || rule.Kind != policyKindPlayerVicinity || !rule.Enabled ||
		rule.Target != 300 || rule.RadiusYards != 300 {
		t.Fatalf("deployed rule drifted: %+v", rule)
	}
	zoneByID := map[uint32]bool{}
	for _, area := range runtime.areas {
		if area.ID == "" || area.ZoneID == 0 || (area.Faction != 1 && area.Faction != 2) || area.RadiusYards <= 0 {
			t.Fatalf("deployed area incomplete: %+v", area)
		}
		zoneByID[area.ZoneID] = true
	}
	for _, zone := range []uint32{12, 14, 1, 85, 215, 141, 3430, 3483} {
		if !zoneByID[zone] {
			t.Fatalf("starter zone %d missing from deployed areas", zone)
		}
	}
	if runtime.maxOnline != 300 {
		t.Fatalf("deployed online cap drifted: %d", runtime.maxOnline)
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

func TestRejectedPolicyAdmissionKeepsCharacterAndDemand(t *testing.T) {
	m := testPopulationManager(t, 100)
	for _, c := range []policyCommitment{
		{ID: "refused", Kind: policyCommitAdmit, BotGUID: 7, CenterX: 500, Radius: 100, CreatedMs: 1},
		{ID: "accepted", Kind: policyCommitAdmit, BotGUID: 8, CreatedMs: 1},
	} {
		m.applyPolicyCommitmentOps([]policyCommitmentOp{{Op: "add", New: &c}})
	}
	m.retryRejectedPolicyAdmissions([]uint32{7})
	live := m.loadPolicyCommitments(2, &policyRuntime{admitTTL: time.Hour, arrivedTTL: time.Hour})
	if len(live) != 2 || live[0].Kind != policyCommitCreate || live[0].BotGUID != 7 ||
		live[0].CenterX != 500 || live[1].Kind != policyCommitAdmit {
		t.Fatalf("refusal must permit retry without replacing the character or disturbing accepted admission: %+v", live)
	}
}
