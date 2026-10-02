package main

import (
	"testing"
)

// Census and fixture helpers. Coordinates are abstract test values, not
// asserted game geography.

const policyTestFreshnessMs = 60000

func policyTestArea(id string, mapID uint32, faction uint32, x, y float64) policyArea {
	return policyArea{
		ID: id, Name: id, MapID: mapID, ZoneID: 1, Faction: faction,
		X: x, Y: y, RadiusYards: 100, ContinentRank: int(mapID),
	}
}

func policyTestBot(guid uint32, online bool, x, y float64, level uint32) policyBot {
	b := policyBot{
		GUID: guid, Race: 1, Class: 1, Level: level, Faction: 1,
		Online: online, MapID: 0, X: x, Y: y, ObservedMs: 1000,
	}
	return b
}

func policyTestHuman(guid uint64, x, y float64, observedMs int64) policyHuman {
	return policyHuman{
		GUID: guid, Name: "human", Faction: 1, Online: true,
		MapID: 0, X: x, Y: y, ObservedMs: observedMs,
	}
}

func countActions(plan policyPlan, kind string) int {
	count := 0
	for _, a := range plan.Actions {
		if a.Kind == kind {
			count++
		}
	}
	return count
}

func countCommitOps(plan policyPlan, kind string) int {
	count := 0
	for _, op := range plan.Commitments {
		if op.Op == "add" && op.New != nil && op.New.Kind == kind {
			count++
		}
	}
	return count
}

func countCommitOpsForRule(plan policyPlan, ruleID string, kind string) int {
	count := 0
	for _, op := range plan.Commitments {
		if op.Op == "add" && op.New != nil && op.New.RuleID == ruleID && op.New.Kind == kind {
			count++
		}
	}
	return count
}

func liveFromPlan(base []policyCommitment, plan policyPlan) []policyCommitment {
	live := make([]policyCommitment, 0, len(base))
	for _, c := range base {
		if c.State == policyCommitmentReleased {
			continue
		}
		released := false
		for _, op := range plan.Commitments {
			if op.Op == "release" && op.ID == c.ID {
				released = true
				break
			}
		}
		if !released {
			live = append(live, c)
		}
	}
	for _, op := range plan.Commitments {
		if op.Op == "add" && op.New != nil {
			live = append(live, *op.New)
		}
	}
	return live
}

func testCaps(maxOnline, maxPool, poolSize, maxActions uint32) policyCaps {
	return policyCaps{
		MaxOnline: maxOnline, MaxPool: maxPool, PoolSize: poolSize,
		MaxActionsPerReconcile: maxActions,
	}
}

func TestPolicyValidationRejectsBadRules(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 0, 0)}
	cases := []policyRule{
		{ID: "", Kind: policyKindStarterArea, Mode: policyModeMaintain, Target: 1, Areas: []string{"northshire"}, Enabled: true},
		{ID: "a", Kind: policyKindStarterArea, Mode: policyModeMaintain, Target: 0, Areas: []string{"northshire"}, Enabled: true},
		{ID: "b", Kind: policyKindStarterArea, Mode: "other", Target: 1, Areas: []string{"northshire"}, Enabled: true},
		{ID: "c", Kind: policyKindStarterArea, Mode: policyModeMaintain, Target: 1, Enabled: true},
		{ID: "d", Kind: policyKindStarterArea, Mode: policyModeMaintain, Target: 1, Areas: []string{"nope"}, Enabled: true},
		{ID: "e", Kind: policyKindPlayerVicinity, Target: 1, Enabled: true},
		{ID: "f", Kind: policyKindPlayerVicinity, Target: 1, RadiusYards: -1, Enabled: true},
		{ID: "g", Kind: "unknown", Target: 1, Enabled: true},
		{ID: "h", Kind: policyKindPlayerVicinity, Target: 1, RadiusYards: 100, SourceArea: "nope", Enabled: true},
	}
	for _, rule := range cases {
		plan := planPopulation([]policyRule{rule}, areas, nil, nil, nil, testCaps(10, 10, 0, 10), 1000, 2000)
		if plan.Status != planInvalid {
			t.Fatalf("rule %+v: expected invalid_rules, got %q", rule, plan.Status)
		}
		if len(plan.Actions) != 0 {
			t.Fatalf("rule %+v: invalid plan produced actions", rule)
		}
	}
	// Duplicate IDs are invalid too.
	dup := policyRule{ID: "x", Kind: policyKindPlayerVicinity, Target: 1, RadiusYards: 100, Enabled: true}
	plan := planPopulation([]policyRule{dup, dup}, areas, nil, nil, nil, testCaps(10, 10, 0, 10), 1000, 2000)
	if plan.Status != planInvalid {
		t.Fatalf("duplicate rule ids: expected invalid_rules, got %q", plan.Status)
	}
}

func TestVicinitySharedCounting(t *testing.T) {
	// Two coincident players with target 100 and 20 nearby bots share the
	// shortage: 80 commitments total, not 160.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	var bots []policyBot
	for i := uint32(1); i <= 20; i++ {
		bots = append(bots, policyTestBot(i, true, 10, 10, 1))
	}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 100, RadiusYards: 100, Enabled: true}
	humans := []policyHuman{policyTestHuman(1, 0, 0, 1000), policyTestHuman(2, 5, 5, 1000)}

	plan := planPopulation([]policyRule{rule}, areas, bots, humans, nil,
		testCaps(500, 1000, 20, 500), 2000, 3000)
	if got := countActions(plan, policyActionCreate); got != 80 {
		t.Fatalf("coincident players: expected 80 create actions, got %d", got)
	}
	live := liveFromPlan(nil, plan)

	// Second reconcile with the same commitments introduces nothing new.
	plan2 := planPopulation([]policyRule{rule}, areas, bots, humans, live,
		testCaps(500, 1000, 100, 500), 4000, 5000)
	if got := countActions(plan2, policyActionCreate); got != 0 {
		t.Fatalf("repeat reconcile: expected 0 create actions, got %d", got)
	}
	for _, s := range plan2.Statuses {
		if s.RuleID == "vic" && s.Satisfied != 100 {
			t.Fatalf("satisfied should count observed + committed as 100, got %d", s.Satisfied)
		}
		if s.RuleID == "vic" && s.Observed != 20 {
			t.Fatalf("observed nearby must stay 20, not falsely report arrivals, got %d", s.Observed)
		}
	}
}

func TestVicinityPartialOverlapSharedCommitments(t *testing.T) {
	// Player 2 is inside player 1's demand region: commitments made for
	// player 1 cover player 2's identical shortage.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 10, RadiusYards: 100, Enabled: true}
	humans := []policyHuman{policyTestHuman(1, 0, 0, 1000), policyTestHuman(2, 60, 0, 1000)}

	plan := planPopulation([]policyRule{rule}, areas, nil, humans, nil,
		testCaps(500, 1000, 0, 500), 2000, 3000)
	if got := countActions(plan, policyActionCreate); got != 10 {
		t.Fatalf("overlapping shortage: expected 10 creates, got %d", got)
	}
	live := liveFromPlan(nil, plan)
	plan2 := planPopulation([]policyRule{rule}, areas, nil, humans, live,
		testCaps(500, 1000, 10, 500), 4000, 5000)
	if got := countActions(plan2, policyActionCreate); got != 0 {
		t.Fatalf("covered player re-introduced shortage: got %d creates", got)
	}

	// A distant player does not share the commitments.
	far := policyTestHuman(3, 5000, 5000, 1000)
	plan3 := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{far}, live,
		testCaps(500, 1000, 10, 500), 4000, 5000)
	if got := countActions(plan3, policyActionCreate); got != 10 {
		t.Fatalf("distant player should demand its own 10, got %d", got)
	}
}

func TestClosestStarterSelection(t *testing.T) {
	// A player on map 0 uses the closest same-map area; a Horde player is
	// never replenished from an Alliance-guarded area by default.
	areas := []policyArea{
		policyTestArea("northshire", 0, 1, 100, 100),
		policyTestArea("farzone", 0, 1, 9000, 9000),
		policyTestArea("durotar", 1, 2, 0, 0),
	}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 1, RadiusYards: 100, Enabled: true}
	human := policyTestHuman(1, 150, 150, 1000)
	plan := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{human}, nil,
		testCaps(500, 1000, 0, 10), 2000, 3000)
	if got := countActions(plan, policyActionCreate); got != 1 {
		t.Fatalf("expected 1 create, got %d", got)
	}
	for _, a := range plan.Actions {
		if a.Kind == policyActionCreate && a.AreaID != "northshire" {
			t.Fatalf("expected closest area northshire, got %q", a.AreaID)
		}
	}
	// Horde player: Alliance areas are ineligible, so the Horde area wins
	// even though it is cross-map.
	horde := policyTestHuman(2, 150, 150, 1000)
	horde.Faction = 2
	plan2 := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{horde}, nil,
		testCaps(500, 1000, 0, 10), 2000, 3000)
	if got := countActions(plan2, policyActionCreate); got != 1 {
		t.Fatalf("expected 1 create for horde player, got %d", got)
	}
	for _, a := range plan2.Actions {
		if a.Kind == policyActionCreate && a.AreaID != "durotar" {
			t.Fatalf("horde player must not replenish from guarded Alliance area, got %q", a.AreaID)
		}
	}
}

func TestAdmitBeforeCreate(t *testing.T) {
	// An offline bot already located near the player is admitted first;
	// no level-1 character is created while reuse covers the deficit.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	offline := policyTestBot(7, false, 20, 0, 5)
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 2, RadiusYards: 100, Enabled: true}
	human := policyTestHuman(1, 0, 0, 1000)

	plan := planPopulation([]policyRule{rule}, areas, []policyBot{offline}, []policyHuman{human}, nil,
		testCaps(500, 1000, 1, 10), 2000, 3000)
	if got := countActions(plan, policyActionAdmit); got != 1 {
		t.Fatalf("expected 1 admit, got %d", got)
	}
	if got := countActions(plan, policyActionCreate); got != 1 {
		t.Fatalf("remaining deficit should create 1 newcomer, got %d", got)
	}
	live := liveFromPlan(nil, plan)
	plan2 := planPopulation([]policyRule{rule}, areas, []policyBot{offline}, []policyHuman{human}, live,
		testCaps(500, 1000, 2, 10), 4000, 5000)
	if got := countActions(plan2, policyActionAdmit); got != 0 {
		t.Fatalf("admitted bot must not be admitted twice, got %d", got)
	}
	if got := countActions(plan2, policyActionCreate); got != 0 {
		t.Fatalf("commitments must prevent repeat creation, got %d", got)
	}
}

func TestOnlineCapBlocksAndReportsShortfall(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 10, RadiusYards: 100, Priority: 5, Enabled: true}
	low := policyRule{ID: "low", Kind: policyKindPlayerVicinity, Target: 10, RadiusYards: 100, Priority: 1, Enabled: true}
	human := policyTestHuman(1, 0, 0, 1000)

	// Online cap 3 admits only the higher-priority rule's demand.
	plan := planPopulation([]policyRule{rule, low}, areas, nil, []policyHuman{human}, nil,
		testCaps(3, 1000, 0, 100), 2000, 3000)
	if got := countActions(plan, policyActionCreate); got != 3 {
		t.Fatalf("expected 3 creates under cap, got %d", got)
	}
	var highStatus, lowStatus policyRuleStatus
	for _, s := range plan.Statuses {
		switch s.RuleID {
		case "vic":
			highStatus = s
		case "low":
			lowStatus = s
		}
	}
	if highStatus.Shortfall != 7 {
		t.Fatalf("high priority shortfall should be 7, got %d", highStatus.Shortfall)
	}
	if lowStatus.Shortfall != 10 {
		t.Fatalf("low priority shortfall should be 10, got %d", lowStatus.Shortfall)
	}
	if len(lowStatus.Notes) == 0 {
		t.Fatal("cap-blocked rule must report a note")
	}

	// Pool cap blocks creation beyond the pool ceiling.
	plan2 := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{human}, nil,
		testCaps(500, 4, 2, 100), 2000, 3000)
	if got := countActions(plan2, policyActionCreate); got != 2 {
		t.Fatalf("pool cap should limit creates to 2, got %d", got)
	}

	// Action budget bounds a single reconcile.
	plan3 := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{human}, nil,
		testCaps(500, 1000, 0, 4), 2000, 3000)
	if len(plan3.Actions) != 4 {
		t.Fatalf("action budget should bound actions to 4, got %d", len(plan3.Actions))
	}
}

func TestLogoutOnHumanGoneAndExcess(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 2, RadiusYards: 100, Enabled: true}

	// Human present with 4 nearby bots: the two farthest are unclaimed.
	var bots []policyBot
	for i := uint32(1); i <= 4; i++ {
		bots = append(bots, policyTestBot(i, true, float64(i)*10, 0, 1))
	}
	human := policyTestHuman(1, 0, 0, 1000)
	plan := planPopulation([]policyRule{rule}, areas, bots, []policyHuman{human}, nil,
		testCaps(500, 1000, 4, 10), 2000, 3000)
	if got := countActions(plan, policyActionLogout); got != 2 {
		t.Fatalf("excess bots should be logout candidates, got %d", got)
	}
	for _, a := range plan.Actions {
		if a.Kind == policyActionLogout && (a.BotGUID == 1 || a.BotGUID == 2) {
			t.Fatalf("nearest claimed bots must not be logged out, got %d", a.BotGUID)
		}
	}

	// Last human logs out: every previously claimed bot is a candidate.
	stale := policyTestHuman(1, 0, 0, 1000)
	stale.Online = false
	plan2 := planPopulation([]policyRule{rule}, areas, bots, []policyHuman{stale}, nil,
		testCaps(500, 1000, 4, 10), 2000, 3000)
	if got := countActions(plan2, policyActionLogout); got != 4 {
		t.Fatalf("all bots should pause after the last human logs out, got %d", got)
	}

	// Busy bots are never paused.
	busy := bots[0]
	busy.Busy = true
	bots[0] = busy
	plan3 := planPopulation([]policyRule{rule}, areas, bots, []policyHuman{stale}, nil,
		testCaps(500, 1000, 4, 10), 2000, 3000)
	for _, a := range plan3.Actions {
		if a.Kind == policyActionLogout && a.BotGUID == busy.GUID {
			t.Fatal("busy bot must never be a logout candidate")
		}
	}
}

func TestStaleCensusActsOnNothing(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 5, RadiusYards: 100, Enabled: true}
	// No fresh humans and no fresh bots: unknown, not zero.
	human := policyTestHuman(1, 0, 0, 1000)
	human.ObservedMs = 0
	plan := planPopulation([]policyRule{rule}, areas, nil, []policyHuman{human}, nil,
		testCaps(500, 1000, 0, 10), 999999, 2000)
	if plan.Status != planStaleCensus {
		t.Fatalf("expected stale_census, got %q", plan.Status)
	}
	if len(plan.Actions) != 0 {
		t.Fatal("stale census must produce no actions")
	}
}

func TestInitialModeNoReplenishOnDeparture(t *testing.T) {
	// Initial allocation: 100 existing bots in the area are allocated, 400
	// are created. Bots later leaving the area never triggers replenish.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	var bots []policyBot
	for i := uint32(1); i <= 100; i++ {
		bots = append(bots, policyTestBot(i, true, 1000, 1000, 1))
	}
	rule := policyRule{ID: "init", Kind: policyKindStarterArea, Mode: policyModeInitial,
		Target: 500, Areas: []string{"northshire"}, Enabled: true}

	plan := planPopulation([]policyRule{rule}, areas, bots, nil, nil,
		testCaps(500, 1000, 100, 500), 2000, 3000)
	if got := countActions(plan, policyActionCreate); got != 400 {
		t.Fatalf("expected 400 creates, got %d", got)
	}
	live := liveFromPlan(nil, plan)

	// All bots depart the area; no replenishment for the initial cohort.
	var departed []policyBot
	for i := range bots {
		b := bots[i]
		b.X, b.Y = 9000, 9000
		departed = append(departed, b)
	}
	plan2 := planPopulation([]policyRule{rule}, areas, departed, nil, live,
		testCaps(500, 1000, 500, 500), 4000, 5000)
	if got := countActions(plan2, policyActionCreate); got != 0 {
		t.Fatalf("initial allocation must not replenish on departure, got %d creates", got)
	}
	for _, s := range plan2.Statuses {
		if s.RuleID == "init" && s.Satisfied != 500 {
			t.Fatalf("initial satisfaction is allocation-based, got %d", s.Satisfied)
		}
	}
}

func TestMaintainAreaReusesAndCreates(t *testing.T) {
	// Maintain mode reconciles live occupancy: 2 online in area, 1 offline
	// in area, target 4 => 1 admit + 1 create; repeats stay stable.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 0, 0)}
	bots := []policyBot{
		policyTestBot(1, true, 10, 0, 1),
		policyTestBot(2, true, 20, 0, 1),
		policyTestBot(3, false, 30, 0, 1),
	}
	rule := policyRule{ID: "maint", Kind: policyKindStarterArea, Mode: policyModeMaintain,
		Target: 4, Areas: []string{"northshire"}, Enabled: true}

	plan := planPopulation([]policyRule{rule}, areas, bots, nil, nil,
		testCaps(500, 1000, 3, 10), 2000, 3000)
	if got := countActions(plan, policyActionAdmit); got != 1 {
		t.Fatalf("expected 1 admit, got %d", got)
	}
	if got := countActions(plan, policyActionCreate); got != 1 {
		t.Fatalf("expected 1 create, got %d", got)
	}
	live := liveFromPlan(nil, plan)
	plan2 := planPopulation([]policyRule{rule}, areas, bots, nil, live,
		testCaps(500, 1000, 4, 10), 4000, 5000)
	if got := countActions(plan2, policyActionAdmit) + countActions(plan2, policyActionCreate); got != 0 {
		t.Fatalf("maintain reconcile must be stable with commitments, got %d actions", got)
	}
}

func TestCommitmentReleaseOnArrival(t *testing.T) {
	// A committed newcomer observed inside its demand region releases the
	// commitment and is counted exactly once as an observed bot.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 3, RadiusYards: 100, Enabled: true}
	human := policyTestHuman(1, 0, 0, 1000)

	base := []policyCommitment{{
		ID: "c1", RuleID: "vic", Kind: policyCommitCreate, PlayerGUID: 1, BotGUID: 42,
		State: policyCommitmentPending, MapID: 0, CenterX: 0, CenterY: 0, Radius: 100, CreatedMs: 1,
	}}
	bots := []policyBot{func() policyBot {
		b := policyTestBot(42, true, 10, 0, 1)
		return b
	}()}

	plan := planPopulation([]policyRule{rule}, areas, bots, []policyHuman{human}, base,
		testCaps(500, 1000, 1, 10), 2000, 3000)
	released := false
	for _, op := range plan.Commitments {
		if op.Op == "release" && op.ID == "c1" {
			released = true
		}
	}
	if !released {
		t.Fatal("arrived committed bot must release its commitment")
	}
	if got := countActions(plan, policyActionCreate); got != 2 {
		t.Fatalf("satisfied should be observed 1 + covered 0 after release; expected 2 creates, got %d", got)
	}

	// Commitment still outstanding while the bot is elsewhere: it counts.
	traveling := policyTestBot(42, true, 1000, 1000, 1)
	plan2 := planPopulation([]policyRule{rule}, areas, []policyBot{traveling}, []policyHuman{human}, base,
		testCaps(500, 1000, 1, 10), 2000, 3000)
	for _, op := range plan2.Commitments {
		if op.Op == "release" && op.ID == "c1" {
			t.Fatal("commitment must not release while its bot is outside the region")
		}
	}
	if got := countActions(plan2, policyActionCreate); got != 2 {
		t.Fatalf("covered commitment should satisfy one unit of 3; expected 2 creates, got %d", got)
	}
}

func TestNewcomersAreLevel1AndNeverRelocated(t *testing.T) {
	// The planner only emits admit/create/logout. It never teleports or
	// moves an online bot and never resets any character's level.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	high := policyTestBot(1, true, 0, 0, 40) // high-level bot near the player
	rule := policyRule{ID: "vic", Kind: policyKindPlayerVicinity, Target: 1, RadiusYards: 100, Enabled: true}
	human := policyTestHuman(1, 0, 0, 1000)

	plan := planPopulation([]policyRule{rule}, areas, []policyBot{high}, []policyHuman{human}, nil,
		testCaps(500, 1000, 1, 10), 2000, 3000)
	for _, a := range plan.Actions {
		switch a.Kind {
		case policyActionAdmit, policyActionCreate, policyActionLogout:
		default:
			t.Fatalf("unexpected action kind %q", a.Kind)
		}
	}
	if got := countActions(plan, policyActionCreate); got != 0 {
		t.Fatal("existing eligible high-level bot counts toward demand without relevel")
	}
	if got := countActions(plan, policyActionLogout); got != 0 {
		t.Fatal("claimed bot must not be logged out")
	}
	for _, s := range plan.Statuses {
		if s.RuleID == "vic" && s.Observed != 1 {
			t.Fatalf("level-40 bot should satisfy level-agnostic vicinity rule, got %d", s.Observed)
		}
	}
}

func TestPriorityOrderingIsDeterministic(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	ruleB := policyRule{ID: "b-rule", Kind: policyKindPlayerVicinity, Target: 2, RadiusYards: 100, Priority: 1, Enabled: true}
	ruleA := policyRule{ID: "a-rule", Kind: policyKindPlayerVicinity, Target: 2, RadiusYards: 100, Priority: 9, Enabled: true}
	human := policyTestHuman(1, 0, 0, 1000)

	var first []policyAction
	for run := 0; run < 3; run++ {
		plan := planPopulation([]policyRule{ruleB, ruleA}, areas, nil, []policyHuman{human}, nil,
			testCaps(2, 1000, 0, 100), 2000, 3000)
		if got := countActions(plan, policyActionCreate); got != 2 {
			t.Fatalf("expected 2 creates, got %d", got)
		}
		for _, a := range plan.Actions {
			if a.Kind == policyActionCreate && a.RuleID != "a-rule" {
				t.Fatalf("higher priority rule must win the cap, got %q", a.RuleID)
			}
		}
		if run == 0 {
			first = plan.Actions
			continue
		}
		if len(first) != len(plan.Actions) {
			t.Fatal("plan length must be deterministic")
		}
		for i := range first {
			if first[i] != plan.Actions[i] {
				t.Fatal("plan actions must be deterministic across runs")
			}
		}
	}
}

func TestDisabledRulesAreInert(t *testing.T) {
	areas := []policyArea{policyTestArea("northshire", 0, 1, 1000, 1000)}
	rule := policyRule{ID: "off", Kind: policyKindPlayerVicinity, Target: 5, RadiusYards: 100, Enabled: false}
	human := policyTestHuman(1, 0, 0, 1000)
	bots := []policyBot{policyTestBot(1, true, 0, 0, 1)}

	plan := planPopulation([]policyRule{rule}, areas, bots, []policyHuman{human}, nil,
		testCaps(500, 1000, 1, 10), 2000, 3000)
	if len(plan.Actions) != 0 {
		t.Fatalf("disabled rule must produce no actions, got %d", len(plan.Actions))
	}
	if len(plan.Statuses) != 0 {
		t.Fatalf("disabled rule must produce no status, got %d", len(plan.Statuses))
	}
}

func TestMaintainAreaLogoutExcess(t *testing.T) {
	// Over-target maintain area: the farthest excess bots are candidates.
	areas := []policyArea{policyTestArea("northshire", 0, 1, 0, 0)}
	var bots []policyBot
	for i := uint32(1); i <= 5; i++ {
		bots = append(bots, policyTestBot(i, true, float64(i)*10, 0, 1))
	}
	rule := policyRule{ID: "maint", Kind: policyKindStarterArea, Mode: policyModeMaintain,
		Target: 2, Areas: []string{"northshire"}, Enabled: true}

	plan := planPopulation([]policyRule{rule}, areas, bots, nil, nil,
		testCaps(500, 1000, 5, 10), 2000, 3000)
	if got := countActions(plan, policyActionLogout); got != 3 {
		t.Fatalf("expected 3 excess logout candidates, got %d", got)
	}
	for _, a := range plan.Actions {
		if a.Kind == policyActionLogout && (a.BotGUID == 1 || a.BotGUID == 2) {
			t.Fatalf("nearest bots stay claimed, got logout for %d", a.BotGUID)
		}
	}
}
