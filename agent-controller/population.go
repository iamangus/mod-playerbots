package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

const (
	populationReservationTTL = 15 * time.Minute
	populationCreateTimeout  = 90 * time.Second
	populationCohortTTL      = 7 * 24 * time.Hour
	populationRateKeyTTL     = 2 * time.Hour
	populationLeaderTTL      = 45 * time.Second
)

// populationConfig controls the external bot population manager. All values
// come from environment variables so the Helm chart can drive them.
type populationConfig struct {
	enabled          bool
	targetTotal      uint32
	zoneTargets      map[uint32]uint32
	raceWeights      map[uint32]float64
	classWeights     map[uint32]float64
	roleWeights      map[string]float64
	maxConcurrent    int
	maxPerHour       uint32
	cohortEnabled    bool
	cohortSize       uint32
	snapshotInterval time.Duration
	refillInterval   time.Duration
}

func envBool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return value
}

func loadPopulationConfig() populationConfig {
	cfg := populationConfig{
		enabled:      envBool("AGENT_POPULATION_ENABLED", false),
		targetTotal:  envUint("AGENT_POPULATION_TARGET_TOTAL", 200),
		zoneTargets:  envJSONUintMap("AGENT_POPULATION_ZONE_TARGETS"),
		raceWeights:  envJSONFloatMap("AGENT_POPULATION_RACE_WEIGHTS"),
		classWeights: envJSONFloatMap("AGENT_POPULATION_CLASS_WEIGHTS"),
		roleWeights: map[string]float64{
			"tank": 0.10, "healer": 0.15, "dps": 0.75,
		},
		maxConcurrent:    int(envUint("AGENT_POPULATION_MAX_CONCURRENT", 2)),
		maxPerHour:       envUint("AGENT_POPULATION_MAX_PER_HOUR", 60),
		cohortEnabled:    envBool("AGENT_POPULATION_COHORT_ENABLED", false),
		cohortSize:       envUint("AGENT_POPULATION_COHORT_SIZE", 12),
		snapshotInterval: envDuration("AGENT_POPULATION_SNAPSHOT_INTERVAL", 2*time.Minute),
		refillInterval:   envDuration("AGENT_POPULATION_REFILL_INTERVAL", 5*time.Minute),
	}
	if cfg.maxConcurrent < 1 {
		cfg.maxConcurrent = 1
	}
	if cfg.snapshotInterval < 30*time.Second {
		cfg.snapshotInterval = 30 * time.Second
	}
	if cfg.refillInterval < time.Minute {
		cfg.refillInterval = time.Minute
	}
	if cfg.targetTotal == 0 {
		cfg.enabled = false
	}
	return cfg
}

// WotLK playable races (race id -> starting zone id). Death knights are
// excluded because managed bots are always created at the level-1 starting
// zone; the core remains the authority and revalidates every combination.
var raceStartZones = map[uint32]uint32{
	1:  12,   // Human -> Elwynn Forest
	2:  14,   // Orc -> Durotar
	3:  1,    // Dwarf -> Dun Morogh
	4:  141,  // Night Elf -> Teldrassil
	5:  85,   // Undead -> Tirisfal Glades
	6:  215,  // Tauren -> Mulgore
	7:  1,    // Gnome -> Dun Morogh
	8:  14,   // Troll -> Durotar
	10: 3430, // Blood Elf -> Eversong Woods
	11: 3483, // Draenei -> Azuremyst Isle
}

var validClassRaces = map[uint32][]uint32{
	1:  {1, 2, 3, 4, 5, 6, 7, 8, 11}, // Warrior (Blood Elves cannot be warriors in WotLK)
	2:  {1, 3, 10, 11},               // Paladin
	3:  {2, 3, 4, 6, 8, 10, 11},      // Hunter
	4:  {1, 2, 3, 4, 7, 8, 10},       // Rogue
	5:  {1, 3, 4, 5, 8, 10, 11},      // Priest
	7:  {2, 6, 8, 11},                // Shaman
	8:  {1, 5, 7, 8, 10, 11},         // Mage
	9:  {1, 2, 5, 7, 10},             // Warlock
	11: {4, 6},                       // Druid
}

var validRaceClasses = func() map[uint32][]uint32 {
	result := make(map[uint32][]uint32)
	for class, races := range validClassRaces {
		for _, race := range races {
			result[race] = append(result[race], class)
		}
	}
	return result
}()

var classRoles = map[uint32][]string{
	1:  {"tank", "dps"},
	2:  {"tank", "healer", "dps"},
	3:  {"dps"},
	4:  {"dps"},
	5:  {"healer", "dps"},
	7:  {"healer", "dps"},
	8:  {"dps"},
	9:  {"dps"},
	11: {"tank", "healer", "dps"},
}

var raceNames = map[uint32]string{
	1: "human", 2: "orc", 3: "dwarf", 4: "night elf", 5: "undead",
	6: "tauren", 7: "gnome", 8: "troll", 10: "blood elf", 11: "draenei",
}

var classNames = map[uint32]string{
	1: "warrior", 2: "paladin", 3: "hunter", 4: "rogue", 5: "priest",
	7: "shaman", 8: "mage", 9: "warlock", 11: "druid",
}

type populationCounts struct {
	SnapshotAt  int64                        `json:"snapshot_at"`
	Total       uint64                       `json:"total"`
	ByRace      map[uint32]uint64            `json:"by_race,omitempty"`
	ByClass     map[uint32]uint64            `json:"by_class,omitempty"`
	ByRaceClass map[uint32]map[uint32]uint64 `json:"by_race_class,omitempty"`
	Zones       map[uint32]uint64            `json:"zones,omitempty"`
}

type populationReservation struct {
	ConfirmedAt int64  `json:"confirmed_at,omitempty"`
	Race        uint32 `json:"race"`
	Class       uint32 `json:"class"`
	Role        string `json:"role"`
	Zone        uint32 `json:"zone"`
	Reason      string `json:"reason"`
	CreatedUTC  int64  `json:"created_utc"`
}

type botRecord struct {
	GUID       uint32 `json:"guid"`
	Name       string `json:"name"`
	Race       uint32 `json:"race"`
	Class      uint32 `json:"class"`
	Role       string `json:"role,omitempty"`
	Zone       uint32 `json:"zone,omitempty"`
	Profile    string `json:"profile"`
	Reason     string `json:"reason,omitempty"`
	Level      uint32 `json:"level"`
	CreatedUTC int64  `json:"created_utc"`
}

type populationEventResult struct {
	Timestamp     int64    `json:"-"`
	Status        string   `json:"status"`
	GUID          uint32   `json:"guid"`
	Name          string   `json:"name"`
	Reason        string   `json:"reason"`
	Level         uint32   `json:"level"`
	RejectedGUIDs []uint32 `json:"rejected_guids"`
}

type populationManager struct {
	owner *controller
	cfg   populationConfig

	// policy holds the optional policy-mode rule set; nil keeps legacy
	// refill behaviour. Both modes never run concurrently.
	policy *policyRuntime

	pendingMu       sync.Mutex
	pending         map[string]chan populationEventResult
	pendingCensusMu sync.Mutex
	pendingCensus   map[string]chan policyCensus
	createSlot      chan struct{}
	policyKick      chan struct{}
	policyStoreMu   sync.Mutex
	stop            chan struct{}
	stopOnce        sync.Once
	ownerMu         sync.Mutex
	ownerToken      string
	ownerSeen       time.Time
}

func newPopulationManager(owner *controller, cfg populationConfig) *populationManager {
	if cfg.maxConcurrent < 1 {
		cfg.maxConcurrent = 1
	}
	return &populationManager{
		owner: owner, cfg: cfg, policy: loadPolicyRuntime(),
		pending:       make(map[string]chan populationEventResult),
		pendingCensus: make(map[string]chan policyCensus),
		createSlot:    make(chan struct{}, cfg.maxConcurrent),
		policyKick:    make(chan struct{}, 1),
		stop:          make(chan struct{}),
	}
}

func (m *populationManager) prefixKey(suffix string) string {
	return "playerbot-agent:" + m.owner.cfg.subjectPrefix + ":" + suffix
}

func (m *populationManager) shutdown() {
	m.stopOnce.Do(func() { close(m.stop) })
}

// BotProfile returns the pre-generated profile for a managed bot, if one
// exists. Actors adopt it on first load so LLM-generated identities survive.
func (m *populationManager) BotProfile(botGUID string) string {
	if m == nil || !m.cfg.enabled {
		return ""
	}
	// Older bridges use the display GUID as their stable routing token.
	if _, low, found := strings.Cut(botGUID, "_Low__"); found {
		botGUID = low
	}
	guid, err := strconv.ParseUint(botGUID, 10, 32)
	if err != nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	data, err := m.owner.redis.Get(ctx, m.prefixKey("bot:"+strconv.FormatUint(guid, 10))).Bytes()
	if err != nil {
		return ""
	}
	var record botRecord
	if json.Unmarshal(data, &record) != nil {
		return ""
	}
	return record.Profile
}

func (m *populationManager) run() {
	if err := m.subscribePopulationEvents(); err != nil {
		log.Printf("population event subscription failed: %v", err)
		return
	}
	go m.eventConsumerLoop()

	snapshotTicker := time.NewTicker(m.cfg.snapshotInterval)
	defer snapshotTicker.Stop()
	reconcileInterval := m.cfg.refillInterval
	if m.policy != nil {
		reconcileInterval = m.policy.reconcileInterval
	}
	refillTicker := time.NewTicker(reconcileInterval)
	defer refillTicker.Stop()
	leaderTicker := time.NewTicker(20 * time.Second)
	defer leaderTicker.Stop()

	m.requestSnapshot()
	for {
		select {
		case <-m.stop:
			return
		case <-snapshotTicker.C:
			m.requestSnapshot()
		case <-refillTicker.C:
			if m.leaderLeaseHeld() || m.acquireLeaderLease() {
				if m.policy != nil {
					m.policyReconcile()
				} else {
					m.reconcile()
				}
			}
		case <-m.policyKick:
			if m.leaderLeaseHeld() || m.acquireLeaderLease() {
				if m.policy != nil {
					m.policyReconcile()
				}
			}
		case <-leaderTicker.C:
			if m.leaderLeaseHeld() {
				m.renewLeaderLease()
			}
		}
	}
}

func (m *populationManager) subscribePopulationEvents() error {
	stream := eventStreamName(m.owner.cfg.subjectPrefix)
	subject := m.owner.cfg.subjectPrefix + ".population.events.>"
	durable := stream + "_POP"
	consumer := &nats.ConsumerConfig{
		Durable: durable, Name: durable, AckPolicy: nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverNewPolicy, ReplayPolicy: nats.ReplayInstantPolicy,
		AckWait: actorLeaseTTL, MaxDeliver: -1, MaxAckPending: 128, FilterSubject: subject,
	}
	if _, err := m.owner.jetstream.AddConsumer(stream, consumer); err != nil {
		if _, infoErr := m.owner.jetstream.ConsumerInfo(stream, durable); infoErr != nil {
			return fmt.Errorf("create population consumer: %w", err)
		}
	}
	return nil
}

func (m *populationManager) eventConsumerLoop() {
	stream := eventStreamName(m.owner.cfg.subjectPrefix)
	durable := stream + "_POP"
	subject := m.owner.cfg.subjectPrefix + ".population.events.>"
	subscription, err := m.owner.jetstream.PullSubscribe(subject, durable, nats.Bind(stream, durable))
	if err != nil {
		log.Printf("population pull subscription failed: %v", err)
		return
	}
	for {
		messages, err := subscription.Fetch(32, nats.MaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			log.Printf("population fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			if err := m.handlePopulationEvent(message.Data); err != nil {
				log.Printf("population event handling failed: %v", err)
			}
			_ = message.Ack()
		}
	}
}

func (m *populationManager) handlePopulationEvent(data []byte) error {
	// A bounded whole-pool census is larger than a single bot snapshot.
	// Keep its own ceiling rather than dropping valid populations above ~300.
	const maxPopulationEventBytes = 1024 * 1024
	if len(data) > maxPopulationEventBytes {
		log.Printf("population event exceeds limit: bytes=%d", len(data))
		return nil
	}
	var incoming event
	if err := json.Unmarshal(data, &incoming); err != nil {
		return nil
	}
	switch incoming.Type {
	case "population_owner_online":
		seen := time.UnixMilli(incoming.Timestamp)
		age := time.Since(seen)
		if incoming.Version != 1 || incoming.BotGUID != "population" || incoming.OwnerToken == "" ||
			age < -30*time.Second || age > 2*time.Minute {
			return nil
		}
		m.ownerMu.Lock()
		if seen.After(m.ownerSeen) {
			m.ownerToken, m.ownerSeen = incoming.OwnerToken, seen
		}
		m.ownerMu.Unlock()
		// Keep controller heartbeats flowing while no bot is online; the native
		// login gate needs them and bot events cannot provide the registration.
		m.owner.noteOwnedShards(incoming.OwnerToken)
	case "player_first_entry":
		m.handlePlayerFirstEntry(incoming)
	case "player_entered_world":
		// Every human world entry wakes the policy director; the kick is
		// coalesced so a login storm cannot queue repeated reconciles.
		if m.policy != nil {
			select {
			case m.policyKick <- struct{}{}:
			default:
			}
		}
	case "population_result":
		m.handlePopulationResult(incoming)
	}
	return nil
}

func (m *populationManager) handlePlayerFirstEntry(incoming event) {
	if m.policy != nil {
		// Policy mode owns population planning; a first entry is just
		// another demand trigger there.
		select {
		case m.policyKick <- struct{}{}:
		default:
		}
		return
	}
	if !m.cfg.cohortEnabled {
		return
	}
	var payload struct {
		PlayerGUID string `json:"player_guid"`
		ZoneID     uint32 `json:"zone_id"`
	}
	if json.Unmarshal(incoming.Payload, &payload) != nil || payload.PlayerGUID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := m.owner.redis.SetNX(ctx, m.prefixKey("cohort:"+payload.PlayerGUID), m.owner.instanceID,
		populationCohortTTL).Result()
	if err != nil {
		log.Printf("cohort dedup check failed: %v", err)
		return
	}
	if !ok {
		return
	}

	size := m.cfg.cohortSize
	if size == 0 {
		size = 1
	}
	zone := payload.ZoneID
	log.Printf("player %s entered the world in zone %d; creating a cohort of %d bots",
		payload.PlayerGUID, zone, size)
	for index := uint32(0); index < size; index++ {
		m.createBot(zone, "player_cohort")
	}
}

func (m *populationManager) handlePopulationResult(incoming event) {
	if census := parsePolicyCensus(incoming.Payload); census != nil {
		m.pendingCensusMu.Lock()
		wait := m.pendingCensus[incoming.RequestID]
		m.pendingCensusMu.Unlock()
		if wait != nil {
			select {
			case wait <- *census:
			default:
			}
		}
		return
	}
	if snapshot := parsePopulationSnapshot(incoming.Payload); snapshot != nil {
		snapshot.SnapshotAt = incoming.Timestamp
		m.storeCounts(*snapshot)
	}
	var result populationEventResult
	if json.Unmarshal(incoming.Payload, &result) != nil {
		return
	}
	m.pendingMu.Lock()
	wait := m.pending[incoming.RequestID]
	m.pendingMu.Unlock()
	if wait == nil {
		return
	}
	select {
	case wait <- populationEventResult{Status: result.Status, GUID: result.GUID, Name: result.Name,
		Reason: result.Reason, Level: result.Level, Timestamp: incoming.Timestamp, RejectedGUIDs: result.RejectedGUIDs}:
	default:
	}
}

// parsePopulationSnapshot recognizes a population_snapshot result payload and
// aggregates it into race/class/zone counts.
func parsePopulationSnapshot(payload json.RawMessage) *populationCounts {
	var decoded struct {
		Status string `json:"status"`
		Total  uint64 `json:"total"`
		Counts []struct {
			Race      uint32 `json:"race"`
			Class     uint32 `json:"class"`
			LevelBand uint32 `json:"level_band"`
			Count     uint64 `json:"count"`
		} `json:"counts"`
		Zones []struct {
			ZoneID   uint32 `json:"zone_id"`
			LowLevel uint64 `json:"low_level"`
		} `json:"zones"`
	}
	if json.Unmarshal(payload, &decoded) != nil || decoded.Status != "completed" ||
		decoded.Counts == nil || decoded.Zones == nil {
		return nil
	}
	counts := &populationCounts{Total: decoded.Total, ByRace: map[uint32]uint64{},
		ByClass: map[uint32]uint64{}, ByRaceClass: map[uint32]map[uint32]uint64{}, Zones: map[uint32]uint64{}}
	for _, row := range decoded.Counts {
		counts.ByRace[row.Race] += row.Count
		counts.ByClass[row.Class] += row.Count
		if counts.ByRaceClass[row.Race] == nil {
			counts.ByRaceClass[row.Race] = map[uint32]uint64{}
		}
		counts.ByRaceClass[row.Race][row.Class] += row.Count
	}
	if counts.Total == 0 {
		for _, row := range decoded.Counts {
			counts.Total += row.Count
		}
	}
	for _, row := range decoded.Zones {
		counts.Zones[row.ZoneID] = row.LowLevel
	}
	return counts
}

func (m *populationManager) storeCounts(counts populationCounts) {
	data, err := json.Marshal(map[string]any{"counts": counts, "at": time.Now().UTC().Unix()})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.owner.redis.Set(ctx, m.prefixKey("population"), data, 0).Err(); err != nil {
		log.Printf("persist population counts: %v", err)
	}
}

func (m *populationManager) requestSnapshot() {
	owner := m.activeOwnerToken()
	if owner == "" {
		return
	}
	requestID := newID()
	body, err := json.Marshal(map[string]any{
		"version": 1, "owner_token": owner, "request_id": requestID,
		"operation": "population_snapshot", "arguments": map[string]any{},
	})
	if err != nil {
		return
	}
	if err := m.owner.nats.Publish(m.owner.cfg.subjectPrefix+".population.commands."+owner, body); err != nil {
		log.Printf("publish population_snapshot: %v", err)
	}
}

// Population heartbeats discover a gameserver even before its first bot exists.
// Keep this routing separate from bot-shard heartbeats.
func (m *populationManager) activeOwnerToken() string {
	m.ownerMu.Lock()
	defer m.ownerMu.Unlock()
	if m.ownerToken != "" && time.Since(m.ownerSeen) <= 2*time.Minute {
		return m.ownerToken
	}
	return m.owner.activeOwnerToken()
}

// activeOwnerToken returns the most recently observed worldserver owner token
// so population commands reach a live gameserver pod.
func (c *controller) activeOwnerToken() string {
	c.ownersMu.Lock()
	defer c.ownersMu.Unlock()
	var bestToken string
	var bestSeen time.Time
	for ownerToken, shards := range c.owners {
		for _, lastSeen := range shards {
			if time.Since(lastSeen) <= 2*time.Minute && lastSeen.After(bestSeen) {
				bestSeen = lastSeen
				bestToken = ownerToken
			}
		}
	}
	return bestToken
}

func (m *populationManager) cachedCounts() *populationCounts {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	data, err := m.owner.redis.Get(ctx, m.prefixKey("population")).Bytes()
	if err != nil {
		return nil
	}
	var stored struct {
		Counts populationCounts `json:"counts"`
		At     int64            `json:"at"`
	}
	if json.Unmarshal(data, &stored) != nil {
		return nil
	}
	return &stored.Counts
}

// effectiveCounts includes creations not yet reflected in the authoritative snapshot.
func (m *populationManager) effectiveCounts() *populationCounts {
	counts := m.cachedCounts()
	if counts == nil || counts.SnapshotAt == 0 || time.Since(time.UnixMilli(counts.SnapshotAt)) > 10*time.Minute {
		return nil
	}
	if counts.ByRace == nil {
		counts.ByRace = make(map[uint32]uint64)
	}
	if counts.ByClass == nil {
		counts.ByClass = make(map[uint32]uint64)
	}
	if counts.Zones == nil {
		counts.Zones = make(map[uint32]uint64)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := m.owner.redis.Scan(ctx, cursor, m.prefixKey("resv:*"), 100).Result()
		if err != nil {
			return nil
		}
		for _, key := range keys {
			data, err := m.owner.redis.Get(ctx, key).Bytes()
			if err == redis.Nil {
				continue
			}
			var r populationReservation
			if err != nil || json.Unmarshal(data, &r) != nil {
				return nil
			}
			if r.ConfirmedAt != 0 && r.ConfirmedAt <= counts.SnapshotAt {
				continue
			}
			counts.Total++
			counts.ByRace[r.Race]++
			counts.ByClass[r.Class]++
			if counts.ByRaceClass != nil {
				if counts.ByRaceClass[r.Race] == nil {
					counts.ByRaceClass[r.Race] = map[uint32]uint64{}
				}
				counts.ByRaceClass[r.Race][r.Class]++
			}
			counts.Zones[r.Zone]++
		}
		cursor = next
		if cursor == 0 {
			return counts
		}
	}
}

func (m *populationManager) reconcile() {
	counts := m.effectiveCounts()
	if counts == nil {
		m.requestSnapshot()
		return
	}

	// Fill starter-zone shortages first; then fill the remaining realm deficit.
	m.refillZones(counts)
	counts = m.effectiveCounts()
	if counts == nil {
		return
	}
	var totalDeficit uint32
	if counts.Total < uint64(m.cfg.targetTotal) {
		totalDeficit = uint32(uint64(m.cfg.targetTotal) - counts.Total)
	}
	if totalDeficit == 0 {
		return
	}

	budget := m.hourlyBudget()
	for i := uint32(0); i < totalDeficit && i < budget; i++ {
		m.createBot(0, "population_refill")
	}
}

// refillZones tops up under-populated starting zones with new level-1 bots.
func (m *populationManager) refillZones(counts *populationCounts) {
	for zone, target := range m.cfg.zoneTargets {
		present := counts.Zones[zone]
		if present >= uint64(target) {
			continue
		}
		deficit := target - uint32(present)
		budget := m.hourlyBudget()
		for i := uint32(0); i < deficit && i < budget; i++ {
			m.createBot(zone, "zone_refill")
		}
	}
}

func (m *populationManager) hourlyBudget() uint32 {
	if m.cfg.maxPerHour == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hour := time.Now().UTC().Format("2006010215")
	key := m.prefixKey("rate:" + hour)
	value, err := m.owner.redis.Get(ctx, key).Uint64()
	if err != nil {
		return m.cfg.maxPerHour
	}
	if value >= uint64(m.cfg.maxPerHour) {
		return 0
	}
	return uint32(uint64(m.cfg.maxPerHour) - value)
}

func (m *populationManager) consumeHourlyBudget() bool {
	if m.cfg.maxPerHour == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hour := time.Now().UTC().Format("2006010215")
	key := m.prefixKey("rate:" + hour)
	value, err := m.owner.redis.Incr(ctx, key).Result()
	if err != nil {
		return false
	}
	if value == 1 {
		m.owner.redis.Expire(ctx, key, populationRateKeyTTL)
	}
	return value <= int64(m.cfg.maxPerHour)
}

func (m *populationManager) chooseRaceClass(zone uint32, counts *populationCounts) (race, class uint32, ok bool) {
	if counts == nil {
		return 0, 0, false
	}
	allowedRaces := func(race uint32) bool {
		if zone == 0 {
			return true
		}
		return raceStartZones[race] == zone
	}

	targetOf := func(weights map[uint32]float64, key uint32, keys []uint32) float64 {
		weightSum := 0.0
		for _, k := range keys {
			weightSum += weights[k]
		}
		if weightSum <= 0 {
			return float64(m.cfg.targetTotal) / float64(len(keys))
		}
		share := weights[key] / weightSum
		return share * float64(m.cfg.targetTotal)
	}

	races := make([]uint32, 0, len(raceStartZones))
	for race := range raceStartZones {
		if allowedRaces(race) {
			races = append(races, race)
		}
	}
	classes := make([]uint32, 0, len(validClassRaces))
	for class := range validClassRaces {
		classes = append(classes, class)
	}

	raceDeficit := func(race uint32) float64 {
		target := targetOf(m.cfg.raceWeights, race, races)
		if target <= 0 {
			return 0
		}
		return (target - float64(counts.ByRace[race])) / target
	}
	classDeficit := func(class uint32) float64 {
		target := targetOf(m.cfg.classWeights, class, classes)
		if target <= 0 {
			return 0
		}
		return (target - float64(counts.ByClass[class])) / target
	}
	localClassDeficit := func(race, class uint32) float64 {
		if zone == 0 || counts.ByRaceClass == nil {
			return classDeficit(class) // Compatibility with cached pre-upgrade snapshots.
		}
		eligible := validRaceClasses[race]
		weightSum := 0.0
		for _, candidate := range eligible {
			weightSum += m.cfg.classWeights[candidate]
		}
		share := 1.0 / float64(len(eligible))
		if weightSum > 0 {
			share = m.cfg.classWeights[class] / weightSum
		}
		if share <= 0 {
			return math.Inf(-1)
		}
		// A starter cohort must stay diverse within its race even when
		// realm-wide deficits strongly favor one class. Include the next
		// allocation in the target to avoid zero denominators for an empty race.
		target := share * float64(counts.ByRace[race]+1)
		return (target - float64(counts.ByRaceClass[race][class])) / target
	}

	worstRace, worstRaceValue := uint32(0), math.Inf(-1)
	for _, race := range races {
		if value := raceDeficit(race); value > worstRaceValue {
			worstRace, worstRaceValue = race, value
		}
	}
	worstClass, worstClassValue := uint32(0), math.Inf(-1)
	for _, class := range classes {
		feasible := false
		for _, race := range validClassRaces[class] {
			feasible = feasible || allowedRaces(race)
		}
		if !feasible {
			continue
		}
		if value := classDeficit(class); value > worstClassValue {
			worstClass, worstClassValue = class, value
		}
	}
	if worstRace == 0 || worstClass == 0 {
		return 0, 0, false
	}

	if zone != 0 || worstRaceValue >= worstClassValue {
		bestClass, bestValue := uint32(0), math.Inf(-1)
		for _, class := range validRaceClasses[worstRace] {
			if value := localClassDeficit(worstRace, class); value > bestValue {
				bestClass, bestValue = class, value
			}
		}
		if bestClass == 0 {
			return 0, 0, false
		}
		return worstRace, bestClass, true
	}

	bestRace, bestValue := uint32(0), math.Inf(-1)
	for _, race := range validClassRaces[worstClass] {
		if !allowedRaces(race) {
			continue
		}
		if value := raceDeficit(race); value > bestValue {
			bestRace, bestValue = race, value
		}
	}
	if bestRace == 0 {
		return 0, 0, false
	}
	return bestRace, worstClass, true
}

func (m *populationManager) chooseRole(class uint32, counts *populationCounts) string {
	roles := classRoles[class]
	if len(roles) == 0 {
		return "dps"
	}
	roleCount := func(role string) uint64 {
		if m.owner == nil || m.owner.redis == nil {
			return 0
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		count, _ := m.owner.redis.Get(ctx, m.prefixKey("role:"+role)).Uint64()
		return count
	}
	best, bestValue := roles[0], -1.0
	for _, role := range roles {
		weight := m.cfg.roleWeights[role]
		if weight <= 0 {
			weight = 0.1
		}
		target := weight * float64(m.cfg.targetTotal)
		value := (target - float64(roleCount(role))) / target
		if value > bestValue {
			best, bestValue = role, value
		}
	}
	return best
}

// createBot runs one provisioning workflow: reserve a population slot,
// generate the profile, dispatch the create command to a live worldserver,
// and record the created character.
func (m *populationManager) createBot(zone uint32, reason string) {
	select {
	case m.createSlot <- struct{}{}:
	case <-m.stop:
		return
	case <-time.After(30 * time.Second):
		return
	}
	defer func() { <-m.createSlot }()
	requestID, reservation, ok := m.reserve(zone, reason)
	if !ok {
		return
	}
	m.executeCreation(requestID, reservation, reason)
}

func (m *populationManager) reserve(zone uint32, reason string) (string, populationReservation, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	lockKey, lockID := m.prefixKey("allocation-lock"), newID()
	if !m.owner.redis.SetNX(ctx, lockKey, lockID, 10*time.Second).Val() {
		cancel()
		return "", populationReservation{}, false
	}
	unlock := func() {
		m.owner.redis.Eval(ctx, `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`, []string{lockKey}, lockID)
		cancel()
	}
	counts := m.effectiveCounts()
	if counts == nil {
		log.Printf("population: no usable population counts; refusing %s creation", reason)
		unlock()
		return "", populationReservation{}, false
	}
	if reason != "player_cohort" && !strings.HasPrefix(reason, "policy:") &&
		m.cfg.targetTotal != 0 && counts.Total >= uint64(m.cfg.targetTotal) {
		unlock()
		return "", populationReservation{}, false
	}
	race, class, ok := m.chooseRaceClass(zone, counts)
	if !ok {
		unlock()
		log.Printf("population: no valid race/class allocation (zone %d, reason %s)", zone, reason)
		return "", populationReservation{}, false
	}
	if !m.consumeHourlyBudget() {
		unlock()
		return "", populationReservation{}, false
	}
	role := m.chooseRole(class, counts)

	requestID := newID()
	reservation := populationReservation{Race: race, Class: class, Role: role, Zone: zone,
		Reason: reason, CreatedUTC: time.Now().UTC().Unix()}
	if reservation.Zone == 0 {
		reservation.Zone = raceStartZones[race]
	}
	reservationData, err := json.Marshal(reservation)
	if err != nil {
		unlock()
		return "", populationReservation{}, false
	}
	if err := m.owner.redis.Set(ctx, m.prefixKey("resv:"+requestID), reservationData,
		populationReservationTTL).Err(); err != nil {
		unlock()
		log.Printf("population reservation failed: %v", err)
		return "", populationReservation{}, false
	}
	unlock()
	return requestID, reservation, true
}

func (m *populationManager) executeCreation(requestID string, reservation populationReservation, reason string) {
	profile := deterministicProfile(reservation.Race, reservation.Class, reservation.Role, reason)
	select {
	case <-m.stop:
		m.releasePolicyCreate(reason)
		return
	default:
	}

	owner := m.activeOwnerToken()
	if owner == "" {
		log.Printf("population: no active worldserver owner; dropping %s creation", reason)
		m.releasePolicyCreate(reason)
		return
	}

	wait := make(chan populationEventResult, 1)
	m.pendingMu.Lock()
	m.pending[requestID] = wait
	m.pendingMu.Unlock()
	defer func() {
		m.pendingMu.Lock()
		delete(m.pending, requestID)
		m.pendingMu.Unlock()
	}()

	body, err := json.Marshal(map[string]any{
		"version": 1, "owner_token": owner, "request_id": requestID,
		"deadline_unix_ms": time.Now().Add(populationCreateTimeout).UnixMilli(),
		"operation":        "create_bot_character",
		"arguments": map[string]any{
			"race": reservation.Race, "class": reservation.Class, "role": reservation.Role, "source": reason,
			"zone": reservation.Zone, "profile": profile,
		},
	})
	if err != nil {
		m.releasePolicyCreate(reason)
		return
	}
	if err := m.owner.nats.Publish(m.owner.cfg.subjectPrefix+".population.commands."+owner, body); err != nil {
		log.Printf("publish create_bot_character: %v", err)
		m.releasePolicyCreate(reason)
		return
	}

	select {
	case result := <-wait:
		m.finishCreation(requestID, reservation, result, profile, reason)
	case <-time.After(populationCreateTimeout):
		log.Printf("population: create_bot_character timed out (request %s); reservation retained", requestID)
	case <-m.stop:
	}
}

func (m *populationManager) finishCreation(requestID string, reservation populationReservation,
	result populationEventResult, profile, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if result.Status != "created" || result.GUID == 0 {
		_ = m.owner.redis.Del(ctx, m.prefixKey("resv:"+requestID)).Err()
		log.Printf("population: creation rejected (%s): %s", result.Status, result.Reason)
		m.releasePolicyCreate(reason)
		return
	}
	reservation.ConfirmedAt = result.Timestamp
	if reservation.ConfirmedAt == 0 {
		reservation.ConfirmedAt = time.Now().UnixMilli()
	}
	if data, err := json.Marshal(reservation); err == nil {
		if err := m.owner.redis.Set(ctx, m.prefixKey("resv:"+requestID), data, populationReservationTTL).Err(); err != nil {
			log.Printf("confirm population reservation %s: %v", requestID, err)
		}
	}

	record := botRecord{GUID: result.GUID, Name: result.Name, Race: reservation.Race,
		Class: reservation.Class, Role: reservation.Role, Zone: reservation.Zone,
		Profile: profile, Reason: reason, Level: result.Level, CreatedUTC: time.Now().UTC().Unix()}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	if err := m.owner.redis.Set(ctx, m.prefixKey("bot:"+strconv.FormatUint(uint64(result.GUID), 10)),
		data, 0).Err(); err != nil {
		log.Printf("persist bot record %d: %v", result.GUID, err)
	}
	m.owner.redis.Incr(ctx, m.prefixKey("role:"+reservation.Role))

	// Policy-mode creations record the native receipt on their commitment.
	m.finishPolicyCreate(reason, result.GUID)

	log.Printf("population: created bot %d (%s) race %d class %d role %s zone %d reason %s",
		result.GUID, result.Name, reservation.Race, reservation.Class, reservation.Role,
		reservation.Zone, reason)
}

// Leader election so only one controller replica runs the refill loop.
func (m *populationManager) leaderKey() string {
	return m.prefixKey("population-leader")
}

func (m *populationManager) acquireLeaderLease() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return m.owner.redis.SetNX(ctx, m.leaderKey(), m.owner.instanceID, populationLeaderTTL).Val()
}

func (m *populationManager) leaderLeaseHeld() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return m.owner.redis.Get(ctx, m.leaderKey()).Val() == m.owner.instanceID
}

func (m *populationManager) renewLeaderLease() {
	const script = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := m.owner.redis.Eval(ctx, script, []string{m.leaderKey()}, m.owner.instanceID,
		populationLeaderTTL.Milliseconds()).Result(); err != nil {
		log.Printf("renew population leader lease: %v", err)
	}
}

func envJSONUintMap(key string) map[uint32]uint32 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return map[uint32]uint32{}
	}
	decoded := map[string]uint32{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		log.Printf("invalid %s JSON: %v", key, err)
		return map[uint32]uint32{}
	}
	result := make(map[uint32]uint32, len(decoded))
	for k, v := range decoded {
		parsed, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			continue
		}
		result[uint32(parsed)] = v
	}
	return result
}

func envJSONFloatMap(key string) map[uint32]float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return map[uint32]float64{}
	}
	decoded := map[string]float64{}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		log.Printf("invalid %s JSON: %v", key, err)
		return map[uint32]float64{}
	}
	result := make(map[uint32]float64, len(decoded))
	for k, v := range decoded {
		parsed, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			continue
		}
		result[uint32(parsed)] = v
	}
	return result
}
