package main

// Policy-mode population management. While a validated policy rule set is
// configured, the manager runs the pure planner against the native live
// census instead of the legacy total/zone refill: it persists commitment
// mutations in Redis, executes bounded admit/logout/create actions with
// native receipts, and applies logout grace. Legacy refill and policy
// reconciliation never run at the same time.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// policyCensus is the parsed population_policy_census result payload.
type policyCensus struct {
	Status     string        `json:"status"`
	ObservedMs int64         `json:"observed_ms"`
	RealmID    uint32        `json:"realm_id"`
	Partial    bool          `json:"partial"`
	Bots       []policyBot   `json:"bots"`
	Humans     []policyHuman `json:"humans"`
}

type policyRuntime struct {
	rules       []policyRule
	areas       []policyArea
	freshness   time.Duration
	maxOnline   uint32
	maxPool     uint32
	maxActions  uint32
	logoutGrace time.Duration
	// Action and created-commitment retry windows bound failed native
	// receipts so a wedge cannot persist silently.
	admitTTL   time.Duration
	createTTL  time.Duration
	arrivedTTL time.Duration
}

// envUintDefault returns the parsed env value or def when unset/invalid.
func envUintDefault(key string, def uint32) uint32 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return def
	}
	return uint32(value)
}

func envDurationDefault(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return def
	}
	return value
}

// loadPolicyRuntime parses and validates the optional policy rule set. A nil
// result keeps legacy population management in place; an invalid rule set is
// never silently applied.
func loadPolicyRuntime() *policyRuntime {
	rawRules := strings.TrimSpace(os.Getenv("AGENT_POPULATION_POLICIES"))
	if rawRules == "" {
		return nil
	}
	var rules []policyRule
	if err := json.Unmarshal([]byte(rawRules), &rules); err != nil {
		log.Printf("population policies disabled: AGENT_POPULATION_POLICIES is not valid JSON: %v", err)
		return nil
	}
	var areas []policyArea
	if raw := strings.TrimSpace(os.Getenv("AGENT_POPULATION_POLICY_AREAS")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &areas); err != nil {
			log.Printf("population policies disabled: AGENT_POPULATION_POLICY_AREAS is not valid JSON: %v", err)
			return nil
		}
	}

	// Dry-run validation against the parsed areas; bad rules fall back to
	// the last known good (legacy) behaviour instead of changing population.
	plan := planPopulation(rules, areas, nil, nil, nil,
		policyCaps{MaxOnline: 1, MaxActionsPerReconcile: 1}, 1000, 2000)
	if plan.Status != planOK {
		log.Printf("population policies disabled: invalid rule set: %v", plan.Notes)
		return nil
	}

	runtime := &policyRuntime{
		rules:       rules,
		areas:       areas,
		freshness:   envDurationDefault("AGENT_POPULATION_POLICY_FRESHNESS", 3*time.Minute),
		maxOnline:   envUintDefault("AGENT_POPULATION_POLICY_MAX_ONLINE", 500),
		maxPool:     envUintDefault("AGENT_POPULATION_POLICY_MAX_POOL", 0),
		maxActions:  envUintDefault("AGENT_POPULATION_POLICY_MAX_ACTIONS", 40),
		logoutGrace: envDurationDefault("AGENT_POPULATION_POLICY_LOGOUT_GRACE", 5*time.Minute),
		admitTTL:    envDurationDefault("AGENT_POPULATION_POLICY_ADMIT_TTL", 30*time.Minute),
		createTTL:   envDurationDefault("AGENT_POPULATION_POLICY_CREATE_TTL", 30*time.Minute),
		arrivedTTL:  envDurationDefault("AGENT_POPULATION_POLICY_ARRIVED_TTL", 24*time.Hour),
	}
	if runtime.freshness < time.Minute {
		runtime.freshness = time.Minute
	}
	if runtime.maxActions < 1 {
		runtime.maxActions = 1
	}
	return runtime
}

// parsePolicyCensus recognizes a population_policy_census result payload.
func parsePolicyCensus(payload json.RawMessage) *policyCensus {
	var probe struct {
		Bots *json.RawMessage `json:"bots"`
	}
	if json.Unmarshal(payload, &probe) != nil || probe.Bots == nil {
		return nil
	}
	var census policyCensus
	if json.Unmarshal(payload, &census) != nil {
		return nil
	}
	return &census
}

// policyReconcile runs one planner pass: census, plan, persist commitments,
// execute bounded actions with authoritative receipts.
func (m *populationManager) policyReconcile() {
	runtime := m.policy
	if runtime == nil {
		return
	}
	census := m.requestPolicyCensus()
	if census == nil || census.Status != "completed" {
		log.Printf("policy reconcile skipped: no fresh census")
		return
	}
	now := census.ObservedMs
	if now <= 0 {
		now = time.Now().UnixMilli()
	}

	commitments := m.loadPolicyCommitments(now, runtime)
	caps := policyCaps{
		MaxOnline: runtime.maxOnline, MaxPool: runtime.maxPool,
		PoolSize:               uint32(len(census.Bots)),
		MaxActionsPerReconcile: runtime.maxActions,
	}
	plan := planPopulation(runtime.rules, runtime.areas, census.Bots, census.Humans, commitments,
		caps, runtime.freshness.Milliseconds(), now)

	m.applyPolicyCommitmentOps(plan.Commitments)
	for _, status := range plan.Statuses {
		log.Printf("policy rule %s: target %d observed %d committed %d satisfied %d shortfall %d notes %v",
			status.RuleID, status.Target, status.Observed, status.Committed, status.Satisfied,
			status.Shortfall, status.Notes)
	}
	m.executePolicyActions(plan.Actions, census)
}

// requestPolicyCensus publishes the census command and awaits its result.
func (m *populationManager) requestPolicyCensus() *policyCensus {
	owner := m.activeOwnerToken()
	if owner == "" {
		return nil
	}
	requestID := newID()
	wait := make(chan policyCensus, 1)
	m.pendingCensusMu.Lock()
	if m.pendingCensus == nil {
		m.pendingCensus = make(map[string]chan policyCensus)
	}
	m.pendingCensus[requestID] = wait
	m.pendingCensusMu.Unlock()
	defer func() {
		m.pendingCensusMu.Lock()
		delete(m.pendingCensus, requestID)
		m.pendingCensusMu.Unlock()
	}()

	body, err := json.Marshal(map[string]any{
		"version": 1, "owner_token": owner, "request_id": requestID,
		"operation": "population_policy_census", "arguments": map[string]any{},
	})
	if err != nil {
		return nil
	}
	if err := m.owner.nats.Publish(m.owner.cfg.subjectPrefix+".population.commands."+owner, body); err != nil {
		log.Printf("publish population_policy_census: %v", err)
		return nil
	}
	select {
	case census := <-wait:
		return &census
	case <-time.After(30 * time.Second):
		log.Printf("policy census timed out (request %s)", requestID)
		return nil
	case <-m.stop:
		return nil
	}
}

// requestPolicyGuids dispatches a bounded per-bot admit or logout command.
func (m *populationManager) requestPolicyGuids(operation string, guids []string) bool {
	owner := m.activeOwnerToken()
	if owner == "" || len(guids) == 0 {
		return false
	}
	requestID := newID()
	wait := make(chan populationEventResult, 1)
	m.pendingMu.Lock()
	if m.pending == nil {
		m.pending = make(map[string]chan populationEventResult)
	}
	m.pending[requestID] = wait
	m.pendingMu.Unlock()
	defer func() {
		m.pendingMu.Lock()
		delete(m.pending, requestID)
		m.pendingMu.Unlock()
	}()

	parsed := make([]uint64, 0, len(guids))
	for _, raw := range guids {
		var guid uint64
		if _, err := fmt.Sscanf(raw, "%d", &guid); err != nil {
			return false
		}
		parsed = append(parsed, guid)
	}
	body, err := json.Marshal(map[string]any{
		"version": 1, "owner_token": owner, "request_id": requestID,
		"operation": operation, "arguments": map[string]any{"guids": parsed},
	})
	if err != nil {
		return false
	}
	if err := m.owner.nats.Publish(m.owner.cfg.subjectPrefix+".population.commands."+owner, body); err != nil {
		log.Printf("publish %s: %v", operation, err)
		return false
	}
	select {
	case result := <-wait:
		return result.Status == "completed"
	case <-time.After(30 * time.Second):
		log.Printf("%s timed out (request %s)", operation, requestID)
		return false
	case <-m.stop:
		return false
	}
}

// loadPolicyCommitments reads persisted commitments, expiring wedged ones.
// Expired pending work is dropped for bounded retry, not silently counted.
func (m *populationManager) loadPolicyCommitments(nowMs int64, runtime *policyRuntime) []policyCommitment {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	data, err := m.owner.redis.Get(ctx, m.prefixKey("policy:commitments")).Bytes()
	if err != nil {
		return nil
	}
	var stored []policyCommitment
	if json.Unmarshal(data, &stored) != nil {
		return nil
	}
	live := make([]policyCommitment, 0, len(stored))
	for _, c := range stored {
		if c.State == policyCommitmentReleased {
			continue
		}
		switch {
		case c.Kind == policyCommitAllocate:
			// Permanent cohort allocation.
		case c.BotGUID == 0 && c.CreatedMs > 0 && nowMs-c.CreatedMs > runtime.createTTL.Milliseconds():
			log.Printf("policy commitment %s expired without a creation receipt", c.ID)
			continue
		case c.BotGUID != 0 && c.Kind == policyCommitAdmit && c.CreatedMs > 0 &&
			nowMs-c.CreatedMs > runtime.admitTTL.Milliseconds():
			log.Printf("policy admit commitment %s expired without an observed arrival", c.ID)
			continue
		case c.BotGUID != 0 && c.Kind == policyCommitCreate && c.CreatedMs > 0 &&
			nowMs-c.CreatedMs > runtime.arrivedTTL.Milliseconds():
			log.Printf("policy commitment %s expired without an observed arrival", c.ID)
			continue
		}
		live = append(live, c)
	}
	return live
}

// applyPolicyCommitmentOps persists plan commitment mutations verbatim;
// planner IDs are already unique per reconcile.
func (m *populationManager) applyPolicyCommitmentOps(ops []policyCommitmentOp) {
	if len(ops) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := m.prefixKey("policy:commitments")
	data, err := m.owner.redis.Get(ctx, key).Bytes()
	var stored []policyCommitment
	if err == nil {
		_ = json.Unmarshal(data, &stored)
	}
	for _, op := range ops {
		switch op.Op {
		case "release":
			kept := stored[:0]
			for _, c := range stored {
				if c.ID == op.ID {
					continue
				}
				kept = append(kept, c)
			}
			stored = kept
		case "add":
			if op.New != nil {
				stored = append(stored, *op.New)
			}
		}
	}
	if len(stored) > 2000 {
		log.Printf("policy commitment store exceeded its bound; refusing further growth")
		return
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return
	}
	if err := m.owner.redis.Set(ctx, key, encoded, 0).Err(); err != nil {
		log.Printf("persist policy commitments: %v", err)
	}
}

// updatePolicyCommitmentBot records the created character on its commitment.
func (m *populationManager) updatePolicyCommitmentBot(commitID string, guid uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := m.prefixKey("policy:commitments")
	data, err := m.owner.redis.Get(ctx, key).Bytes()
	if err != nil {
		return
	}
	var stored []policyCommitment
	if json.Unmarshal(data, &stored) != nil {
		return
	}
	changed := false
	for i := range stored {
		if stored[i].ID == commitID && stored[i].BotGUID == 0 {
			stored[i].BotGUID = guid
			changed = true
		}
	}
	if !changed {
		return
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return
	}
	if err := m.owner.redis.Set(ctx, key, encoded, 0).Err(); err != nil {
		log.Printf("persist policy commitment receipt: %v", err)
	}
}

// executePolicyActions performs admits, bounded logouts with grace, and
// creation dispatch. Each native result is authoritative; failures are
// logged and retried on a later reconcile within the commitment TTLs.
func (m *populationManager) executePolicyActions(actions []policyAction, census *policyCensus) {
	runtime := m.policy
	if runtime == nil {
		return
	}
	var admitGuids []string
	inPlan := make(map[uint32]bool)
	for _, action := range actions {
		switch action.Kind {
		case policyActionAdmit:
			if action.BotGUID != 0 {
				admitGuids = append(admitGuids, fmt.Sprintf("%d", action.BotGUID))
			}
		case policyActionLogout:
			inPlan[action.BotGUID] = true
			m.queuePolicyLogout(action.BotGUID)
		case policyActionCreate:
			m.executePolicyCreate(action)
		}
	}
	// Dispatch in bounded native batches of 32.
	for start := 0; start < len(admitGuids); start += 32 {
		end := start + 32
		if end > len(admitGuids) {
			end = len(admitGuids)
		}
		if !m.requestPolicyGuids("population_policy_admit_bots", admitGuids[start:end]) {
			log.Printf("policy admit batch failed natively; commitments remain pending for retry")
		}
	}
	// Logouts execute only for candidates whose grace window elapsed and
	// which this plan still wants paused; released candidates clear their
	// marker so a later pause starts a fresh grace window.
	ready := m.policyLogoutReady(runtime)
	var logoutBatch []string
	for _, guid := range ready {
		if inPlan[guid] {
			logoutBatch = append(logoutBatch, fmt.Sprintf("%d", guid))
		} else {
			m.clearPolicyLogout(guid)
		}
	}
	for start := 0; start < len(logoutBatch); start += 32 {
		end := start + 32
		if end > len(logoutBatch) {
			end = len(logoutBatch)
		}
		if m.requestPolicyGuids("population_policy_logout_bots", logoutBatch[start:end]) {
			for _, raw := range logoutBatch[start:end] {
				var guid uint64
				if _, err := fmt.Sscanf(raw, "%d", &guid); err == nil {
					m.clearPolicyLogout(uint32(guid))
				}
			}
		} else {
			log.Printf("policy logout batch failed natively; candidates remain marked for retry")
		}
	}
}

// queuePolicyLogout records a logout candidate's first-seen timestamp; the
// action executes only after the grace window elapses so transient census
// blips or a briefly absent human never mass-pause the pool.
func (m *populationManager) queuePolicyLogout(guid uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := m.prefixKey("policy:logout-candidates")
	_ = m.owner.redis.HSetNX(ctx, key, fmt.Sprintf("%d", guid), time.Now().UnixMilli()).Err()
}

// clearPolicyLogout drops a satisfied or released logout candidate marker.
func (m *populationManager) clearPolicyLogout(guid uint32) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = m.owner.redis.HDel(ctx, m.prefixKey("policy:logout-candidates"), fmt.Sprintf("%d", guid)).Err()
}

// policyLogoutReady returns candidate GUIDs whose grace window elapsed.
// Malformed entries are dropped rather than blocking the queue forever.
func (m *populationManager) policyLogoutReady(runtime *policyRuntime) []uint32 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	key := m.prefixKey("policy:logout-candidates")
	entries, err := m.owner.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil
	}
	var ready []uint32
	now := time.Now().UnixMilli()
	for field, firstSeen := range entries {
		guid, err := strconv.ParseUint(field, 10, 32)
		if err != nil {
			m.owner.redis.HDel(ctx, key, field)
			continue
		}
		seen, err := strconv.ParseInt(firstSeen, 10, 64)
		if err != nil {
			m.owner.redis.HDel(ctx, key, field)
			continue
		}
		if now-seen > runtime.logoutGrace.Milliseconds() {
			ready = append(ready, uint32(guid))
		}
	}
	return ready
}

// executePolicyCreate dispatches one level-1 creation for the action's
// starter area, tagging the reservation with the commitment ID so the
// native receipt can be recorded on the right commitment.
func (m *populationManager) executePolicyCreate(action policyAction) {
	if m.policy == nil {
		return
	}
	var zone uint32
	for _, area := range m.policy.areas {
		if area.ID == action.AreaID {
			zone = area.ZoneID
			break
		}
	}
	if zone == 0 {
		log.Printf("policy create for unknown area %q skipped", action.AreaID)
		return
	}
	m.createBot(zone, "policy:"+action.CommitID)
}

// finishPolicyCreate links a successful creation receipt to its commitment.
func (m *populationManager) finishPolicyCreate(reason string, guid uint32) {
	if !strings.HasPrefix(reason, "policy:") {
		return
	}
	commitID := strings.TrimPrefix(reason, "policy:")
	if commitID == "" {
		return
	}
	m.updatePolicyCommitmentBot(commitID, guid)
}
