package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

const (
	maxNATSMessageBytes    = 64 * 1024
	maxLLMResponseBytes    = 1024 * 1024
	maxMemoryBytes         = 8192
	maxEvents              = 20
	eventShardCount        = 256
	actorLeaseTTL          = 2 * time.Minute
	actorLeaseRenew        = 30 * time.Second
	taskMonitorInterval    = 15 * time.Second
	rescanInterval         = 10 * time.Second
	minDecisionGap         = 5 * time.Second
	initialDecisionSpread  = 5 * time.Minute
	maxRealtimeEventAge    = 2 * time.Minute
	combatOperationTimeout = 10 * time.Minute
	travelOperationTimeout = 30 * time.Minute
)

type config struct {
	natsURL          string
	redisURL         string
	subjectPrefix    string
	shardID          uint32
	shardCount       uint32
	modelEndpoint    string
	modelAPIKey      string
	modelName        string
	requestTimeout   time.Duration
	decisionInterval time.Duration
	maxTokens        uint32
	reasoningEffort  string
}

func loadConfig() (config, error) {
	cfg := config{
		natsURL:          envOr("TC9_NATS_URL", "nats://127.0.0.1:4222"),
		redisURL:         envOr("AGENT_STATE_REDIS_URL", "redis://127.0.0.1:6379/0"),
		subjectPrefix:    envOr("AGENT_SUBJECT_PREFIX", "playerbots.v1"),
		shardID:          envUint("AGENT_SHARD_ID", statefulOrdinal(os.Getenv("HOSTNAME"))),
		shardCount:       envUint("AGENT_SHARD_COUNT", 1),
		modelEndpoint:    strings.TrimSpace(os.Getenv("LLM_ENDPOINT")),
		modelAPIKey:      strings.TrimSpace(os.Getenv("LLM_API_KEY")),
		modelName:        strings.TrimSpace(os.Getenv("LLM_MODEL")),
		requestTimeout:   envDuration("LLM_REQUEST_TIMEOUT", 30*time.Second),
		decisionInterval: envDuration("AGENT_DECISION_INTERVAL", 30*time.Minute),
		maxTokens:        envUint("LLM_MAX_TOKENS", 512),
		reasoningEffort:  strings.TrimSpace(os.Getenv("LLM_REASONING_EFFORT")),
	}
	if cfg.modelEndpoint == "" || cfg.modelName == "" {
		return config{}, errors.New("LLM_ENDPOINT and LLM_MODEL are required")
	}
	if cfg.shardCount == 0 || cfg.shardCount > eventShardCount || cfg.shardID >= cfg.shardCount {
		return config{}, errors.New("AGENT_SHARD_ID/AGENT_SHARD_COUNT must define a valid shard assignment")
	}
	if cfg.maxTokens < 128 {
		cfg.maxTokens = 128
	}
	if cfg.maxTokens > 2048 {
		cfg.maxTokens = 2048
	}
	return cfg, nil
}

func statefulOrdinal(hostname string) uint32 {
	last := strings.LastIndexByte(hostname, '-')
	if last < 0 || last == len(hostname)-1 {
		return 0
	}
	ordinal, err := strconv.ParseUint(hostname[last+1:], 10, 32)
	if err != nil {
		return 0
	}
	return uint32(ordinal)
}

func playerbotEventShard(botGUID string) uint32 {
	var hash uint32 = 2166136261
	for index := 0; index < len(botGUID); index++ {
		hash ^= uint32(botGUID[index])
		hash *= 16777619
	}
	return hash % eventShardCount
}

func decisionJitter(botGUID string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(botGUID))
	return time.Duration(hash.Sum64() % uint64(interval))
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < time.Second {
		return fallback
	}
	return parsed
}

func envUint(key string, fallback uint32) uint32 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(parsed)
}

type event struct {
	Version    int             `json:"version"`
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	BotGUID    string          `json:"bot_guid"`
	OwnerToken string          `json:"owner_token"`
	OwnerEpoch string          `json:"owner_epoch"`
	Timestamp  int64           `json:"timestamp_unix_ms"`
	Shard      uint32          `json:"-"`
	RequestID  string          `json:"request_id"`
	Payload    json.RawMessage `json:"payload"`
	decision   *decisionResult `json:"-"`
	ack        func() error    `json:"-"`
	nak        func() error    `json:"-"`
}

type decisionJob struct {
	actor    *actor
	revision uint64
	profile  string
	memories []string
	state    snapshot
	events   []recentEvent
	task     *task
	trigger  string
}

type decisionResult struct {
	revision uint64
	call     *toolCall
	err      error
}

type command struct {
	Version      int             `json:"version"`
	BotGUID      string          `json:"bot_guid"`
	OwnerToken   string          `json:"owner_token"`
	OwnerEpoch   string          `json:"owner_epoch"`
	DeadlineUnix int64           `json:"deadline_unix_ms"`
	RequestID    string          `json:"request_id"`
	OperationID  string          `json:"operation_id"`
	Operation    string          `json:"operation"`
	Arguments    json.RawMessage `json:"arguments"`
}

type questObjective struct {
	Index    uint32 `json:"index"`
	Entry    int32  `json:"entry"`
	ItemID   uint32 `json:"item_id"`
	Count    uint32 `json:"count"`
	Required uint32 `json:"required"`
}

type questInfo struct {
	QuestID          uint32           `json:"quest_id"`
	Title            string           `json:"title"`
	Status           uint32           `json:"status"`
	StatusName       string           `json:"status_name"`
	Type             uint32           `json:"type"`
	SuggestedPlayers uint32           `json:"suggested_players"`
	Objectives       []questObjective `json:"objectives"`
}

type playerInfo struct {
	GUID     string    `json:"guid"`
	GUIDRaw  string    `json:"guid_raw"`
	Name     string    `json:"name"`
	Level    uint32    `json:"level"`
	Distance float64   `json:"distance"`
	IsBot    bool      `json:"is_bot"`
	MapID    uint32    `json:"map_id"`
	Position []float64 `json:"position"`
}

type snapshot struct {
	SchemaVersion int `json:"schema_version"`
	Bot           struct {
		GUID               string    `json:"guid"`
		Name               string    `json:"name"`
		ClassID            uint32    `json:"class_id"`
		RaceID             uint32    `json:"race_id"`
		TeamID             uint32    `json:"team_id"`
		Level              uint32    `json:"level"`
		HealthPct          uint32    `json:"health_pct"`
		InCombat           bool      `json:"in_combat"`
		MapID              uint32    `json:"map_id"`
		ZoneID             uint32    `json:"zone_id"`
		Position           []float64 `json:"position"`
		GroupSize          uint32    `json:"group_size"`
		GroupLeaderGUID    string    `json:"group_leader_guid"`
		PendingGroupInvite bool      `json:"pending_group_invite"`
		HasAvailableLoot   bool      `json:"has_available_loot"`
		CurrentTarget      *struct {
			GUID         string `json:"guid"`
			Entry        uint32 `json:"entry"`
			Name         string `json:"name"`
			Alive        bool   `json:"alive"`
			HealthPct    uint32 `json:"health_pct"`
			LootPossible bool   `json:"loot_possible"`
		} `json:"current_target"`
		GroupMembers []struct {
			GUID  string `json:"guid"`
			Name  string `json:"name"`
			Level uint32 `json:"level"`
			IsBot bool   `json:"is_bot"`
		} `json:"group_members"`
	} `json:"bot"`
	Professions map[string]uint32 `json:"professions"`
	Inventory   struct {
		MoneyCopper uint32 `json:"money_copper"`
		Items       []struct {
			ItemID uint32 `json:"item_id"`
			Count  uint32 `json:"count"`
			Name   string `json:"name"`
		} `json:"items"`
	} `json:"inventory"`
	Quests          []questInfo `json:"quests"`
	NearbyCreatures []struct {
		GUID      string    `json:"guid"`
		Entry     uint32    `json:"entry"`
		Name      string    `json:"name"`
		HealthPct uint32    `json:"health_pct"`
		Distance  float64   `json:"distance"`
		Position  []float64 `json:"position"`
	} `json:"nearby_creatures"`
	NearbyGameObjects []struct {
		GUID        string    `json:"guid"`
		Entry       uint32    `json:"entry"`
		Name        string    `json:"name"`
		GatherSkill uint32    `json:"gather_skill"`
		CanGather   bool      `json:"can_gather"`
		Distance    float64   `json:"distance"`
		Position    []float64 `json:"position"`
	} `json:"nearby_game_objects"`
	NearbyPlayers []playerInfo `json:"nearby_players"`
	NearbyNPCs    []struct {
		GUID       string  `json:"guid"`
		Entry      uint32  `json:"entry"`
		QuestGiver bool    `json:"quest_giver"`
		Distance   float64 `json:"distance"`
	} `json:"nearby_npcs"`
	TravelTarget *struct {
		DestinationName string    `json:"destination_name"`
		IsTraveling     bool      `json:"is_traveling"`
		IsWorking       bool      `json:"is_working"`
		Position        []float64 `json:"position"`
		MapID           uint32    `json:"map_id"`
	} `json:"travel_target"`
}

type task struct {
	ID                  string    `json:"id"`
	Kind                string    `json:"kind"`
	TargetName          string    `json:"target_name,omitempty"`
	TargetGUID          string    `json:"target_guid,omitempty"`
	DestinationName     string    `json:"destination_name,omitempty"`
	GoalDistance        float64   `json:"goal_distance,omitempty"`
	QuestID             uint32    `json:"quest_id,omitempty"`
	CreatureEntry       uint32    `json:"creature_entry,omitempty"`
	Profession          string    `json:"profession,omitempty"`
	GoalCount           uint32    `json:"goal_count"`
	Completed           uint32    `json:"completed"`
	Radius              float64   `json:"radius,omitempty"`
	MapID               uint32    `json:"map_id,omitempty"`
	AreaCenter          []float64 `json:"area_center,omitempty"`
	ObjectiveIndex      uint32    `json:"objective_index,omitempty"`
	ObjectiveCount      uint32    `json:"objective_count,omitempty"`
	Phase               string    `json:"phase"`
	OperationID         string    `json:"operation_id,omitempty"`
	Retries             uint32    `json:"retries,omitempty"`
	LastProgressUTC     time.Time `json:"last_progress_utc"`
	LastScanUTC         time.Time `json:"last_scan_utc,omitempty"`
	LastTargetHealthPct uint32    `json:"last_target_health_pct,omitempty"`
}

type persistedAgent struct {
	Profile               string        `json:"profile"`
	Memories              []string      `json:"memories"`
	Task                  *task         `json:"task,omitempty"`
	LastEventID           string        `json:"last_event_id,omitempty"`
	OwnerToken            string        `json:"owner_token,omitempty"`
	OwnerEpoch            string        `json:"owner_epoch,omitempty"`
	RecentEvents          []recentEvent `json:"recent_events,omitempty"`
	PendingDecisionReason string        `json:"pending_decision_reason,omitempty"`
	DecisionPending       bool          `json:"decision_pending,omitempty"`
}

type recentEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	New     bool            `json:"new"`
}

type controller struct {
	cfg        config
	instanceID string
	nats       *nats.Conn
	jetstream  nats.JetStreamContext
	redis      *redis.Client
	model      *modelClient
	population *populationManager
	actorsMu   sync.Mutex
	actors     map[string]*actor
	modelSlots chan struct{}
	modelJobs  chan decisionJob
	shards     []uint32
	ownersMu   sync.Mutex
	owners     map[string]map[uint32]time.Time
}

type actor struct {
	owner                 *controller
	botGUID               string
	ownerToken            string
	bridgeActive          bool
	inbox                 chan event
	leaseToken            string
	leaseHeld             bool
	stop                  chan struct{}
	state                 persistedAgent
	revision              uint64
	decisionPending       bool
	latest                snapshot
	hasSnapshot           bool
	snapshotUpdatedAt     time.Time
	lastBotEventAt        time.Time
	recent                []recentEvent
	pendingSnapshotID     string
	pendingSnapshotAt     time.Time
	pendingDecisionReason string
	lastDecisionAt        time.Time
	nextIdleDecision      time.Time
	online                bool
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	redisOptions, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		log.Fatalf("invalid Redis URL: %v", err)
	}
	redisClient := redis.NewClient(redisOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := redisClient.Ping(ctx).Err(); err != nil {
		cancel()
		log.Fatalf("Redis connection failed: %v", err)
	}
	cancel()

	natsConn, err := nats.Connect(cfg.natsURL, nats.Name(fmt.Sprintf("playerbot-agent-shard-%d", cfg.shardID)),
		nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}
	jetstream, err := natsConn.JetStream()
	if err != nil {
		log.Fatalf("NATS JetStream initialization failed: %v", err)
	}
	workers := envUint("AGENT_MODEL_WORKERS", 16)
	if workers == 0 {
		workers = 1
	}
	queueCapacity := envUint("AGENT_DECISION_QUEUE_CAPACITY", 4096)
	if queueCapacity == 0 {
		queueCapacity = 1
	}
	c := &controller{
		cfg: cfg, instanceID: envOr("HOSTNAME", "agent-controller"),
		nats: natsConn, jetstream: jetstream, redis: redisClient, model: newModelClient(cfg),
		actors: make(map[string]*actor), modelSlots: make(chan struct{}, int(workers)),
		modelJobs: make(chan decisionJob, int(queueCapacity)),
		owners:    make(map[string]map[uint32]time.Time),
	}
	c.startModelWorkers(workers)
	if populationCfg := loadPopulationConfig(); populationCfg.enabled {
		c.population = newPopulationManager(c, populationCfg)
		go c.population.run()
		log.Printf("population manager started target=%d zones=%d cohort=%v",
			populationCfg.targetTotal, len(populationCfg.zoneTargets), populationCfg.cohortEnabled)
	}
	if err := c.subscribeToShards(); err != nil {
		log.Fatalf("NATS subscribe failed: %v", err)
	}
	if err := natsConn.FlushTimeout(5 * time.Second); err != nil {
		log.Fatalf("NATS subscription flush failed: %v", err)
	}

	go c.heartbeatOwnersLoop()
	go c.startHealthServer()
	log.Printf("playerbot agent-controller started shard=%d/%d", cfg.shardID, cfg.shardCount)
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	<-signalContext.Done()
	stop()
	c.shutdown()
	if c.population != nil {
		c.population.shutdown()
	}
	_ = natsConn.Drain()
	_ = redisClient.Close()
}

func (c *controller) startModelWorkers(count uint32) {
	for worker := uint32(0); worker < count; worker++ {
		go func() {
			for job := range c.modelJobs {
				call, err := c.model.decide(c.modelSlots, job.profile, job.memories,
					job.state, job.events, job.task, job.trigger)
				result := event{Type: "internal_decision", decision: &decisionResult{
					revision: job.revision, call: call, err: err,
				}}
				select {
				case job.actor.inbox <- result:
				case <-job.actor.stop:
				}
			}
		}()
	}
}

func (c *controller) subscribeToShards() error {
	stream := eventStreamName(c.cfg.subjectPrefix)
	if err := c.ensureEventStream(stream); err != nil {
		return err
	}
	// Population events live in their own subject tree on the same stream.
	if err := c.ensurePopulationStreamSubject(stream); err != nil {
		return err
	}
	for shard := c.cfg.shardID; shard < eventShardCount; shard += c.cfg.shardCount {
		subject := fmt.Sprintf("%s.events.%d.>", c.cfg.subjectPrefix, shard)
		durable := fmt.Sprintf("%s_S%03d", stream, shard)
		consumer := &nats.ConsumerConfig{
			Durable: durable, Name: durable, AckPolicy: nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverNewPolicy, ReplayPolicy: nats.ReplayInstantPolicy,
			AckWait: actorLeaseTTL, MaxDeliver: -1, MaxAckPending: 256, FilterSubject: subject,
		}
		if _, err := c.jetstream.AddConsumer(stream, consumer); err != nil {
			if _, infoErr := c.jetstream.ConsumerInfo(stream, durable); infoErr != nil {
				return fmt.Errorf("create durable consumer %s: %w", durable, err)
			}
		}
		subscription, err := c.jetstream.PullSubscribe(subject, durable, nats.Bind(stream, durable))
		if err != nil {
			return err
		}
		c.shards = append(c.shards, shard)
		go c.consumeShard(shard, subscription)
	}
	log.Printf("subscribed to %d/%d playerbot event shards", len(c.shards), eventShardCount)
	return nil
}

func eventStreamName(prefix string) string {
	var name strings.Builder
	for _, character := range strings.ToUpper(prefix) {
		if character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' {
			name.WriteRune(character)
		} else {
			name.WriteByte('_')
		}
	}
	return "PBA_" + name.String()
}

func (c *controller) ensureEventStream(name string) error {
	if _, err := c.jetstream.StreamInfo(name); err == nil {
		return nil
	}
	config := &nats.StreamConfig{
		Name: name, Subjects: []string{c.cfg.subjectPrefix + ".events.>", c.cfg.subjectPrefix + ".population.events.>"},
		Retention: nats.LimitsPolicy, Storage: nats.FileStorage,
		MaxAge: time.Hour, MaxBytes: 8 << 30, MaxMsgs: -1, MaxMsgsPerSubject: -1,
		Discard: nats.DiscardOld, Replicas: 1,
	}
	if _, err := c.jetstream.AddStream(config); err != nil {
		if _, infoErr := c.jetstream.StreamInfo(name); infoErr != nil {
			return fmt.Errorf("create JetStream event stream %s: %w", name, err)
		}
	}
	return nil
}

// ensurePopulationStreamSubject adds the population subject tree to an
// existing event stream that predates population management.
func (c *controller) ensurePopulationStreamSubject(stream string) error {
	info, err := c.jetstream.StreamInfo(stream)
	if err != nil {
		return err
	}
	populationSubject := c.cfg.subjectPrefix + ".population.events.>"
	for _, subject := range info.Config.Subjects {
		if subject == populationSubject {
			return nil
		}
	}
	info.Config.Subjects = append(info.Config.Subjects, populationSubject)
	if _, err := c.jetstream.UpdateStream(&info.Config); err != nil {
		return fmt.Errorf("add population subjects to stream %s: %w", stream, err)
	}
	return nil
}

func (c *controller) consumeShard(shard uint32, subscription *nats.Subscription) {
	for {
		messages, err := subscription.Fetch(64, nats.MaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			log.Printf("fetch playerbot event shard %d: %v", shard, err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			c.handleNATSMessage(message)
		}
	}
}

func (c *controller) noteOwner(ownerToken string, shard uint32) {
	c.ownersMu.Lock()
	defer c.ownersMu.Unlock()
	shards := c.owners[ownerToken]
	if shards == nil {
		shards = make(map[uint32]time.Time)
		c.owners[ownerToken] = shards
	}
	shards[shard] = time.Now().UTC()
}

func (c *controller) heartbeatOwnersLoop() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.ownersMu.Lock()
		now := time.Now().UTC()
		owners := make(map[string][]uint32, len(c.owners))
		for ownerToken, shards := range c.owners {
			for shard, lastSeen := range shards {
				if now.Sub(lastSeen) > 2*time.Minute {
					delete(shards, shard)
					continue
				}
				owners[ownerToken] = append(owners[ownerToken], shard)
			}
			if len(shards) == 0 {
				delete(c.owners, ownerToken)
			}
		}
		c.ownersMu.Unlock()

		for ownerToken, shards := range owners {
			for _, shard := range shards {
				payload, err := json.Marshal(map[string]any{
					"version": 1, "owner_token": ownerToken,
					"operation": "controller_heartbeat", "shard_id": shard,
				})
				if err != nil {
					continue
				}
				subject := c.cfg.subjectPrefix + ".commands." + ownerToken
				if err := c.nats.Publish(subject, payload); err != nil {
					log.Printf("publish controller heartbeat owner=%s shard=%d: %v", ownerToken, shard, err)
				}
			}
		}
	}
}

func (c *controller) handleNATSMessage(message *nats.Msg) {
	if len(message.Data) > maxNATSMessageBytes {
		log.Printf("dropping oversized event (%d bytes)", len(message.Data))
		_ = message.Term()
		return
	}
	var incoming event
	if err := json.Unmarshal(message.Data, &incoming); err != nil {
		log.Printf("dropping malformed event: %v", err)
		_ = message.Term()
		return
	}
	if incoming.BotGUID == "" || incoming.OwnerToken == "" {
		_ = message.Term()
		return
	}
	prefix := c.cfg.subjectPrefix + ".events."
	partitionAndOwner := strings.TrimPrefix(message.Subject, prefix)
	parts := strings.SplitN(partitionAndOwner, ".", 3)
	if len(parts) != 3 {
		_ = message.Term()
		return
	}
	partition, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil || uint32(partition) != playerbotEventShard(incoming.BotGUID) ||
		parts[1] != incoming.OwnerToken || parts[2] != incoming.BotGUID {
		_ = message.Term()
		return
	}
	if incoming.Timestamp > 0 && time.Since(time.UnixMilli(incoming.Timestamp)) > maxRealtimeEventAge {
		_ = message.Ack()
		return
	}
	incoming.Shard = uint32(partition)
	incoming.ack = func() error { return message.Ack() }
	incoming.nak = func() error { return message.NakWithDelay(5 * time.Second) }
	c.noteOwner(incoming.OwnerToken, incoming.Shard)
	a := c.getActor(incoming.BotGUID, incoming.OwnerToken)
	select {
	case a.inbox <- incoming:
	default:
		_ = message.NakWithDelay(2 * time.Second)
		log.Printf("actor inbox full; event %s for bot %s will be redelivered", incoming.EventID, incoming.BotGUID)
	}
}

func (c *controller) getActor(botGUID, ownerToken string) *actor {
	c.actorsMu.Lock()
	defer c.actorsMu.Unlock()
	if current := c.actors[botGUID]; current != nil {
		return current
	}
	a := &actor{
		owner: c, botGUID: botGUID, ownerToken: ownerToken,
		inbox: make(chan event, 128), leaseToken: c.instanceID + ":" + newID(), stop: make(chan struct{}),
	}
	c.actors[botGUID] = a
	go a.run()
	return a
}

func (a *actor) run() {
	if !a.acquireLease() {
		a.owner.removeActor(a)
		return
	}
	a.leaseHeld = true
	a.load()
	a.nextIdleDecision = time.Now().Add(decisionJitter(a.botGUID, initialDecisionSpread))
	taskTimer := time.NewTicker(taskMonitorInterval)
	defer taskTimer.Stop()
	leaseTimer := time.NewTicker(actorLeaseRenew)
	defer leaseTimer.Stop()
	for {
		select {
		case incoming := <-a.inbox:
			a.handleEvent(incoming)
			if incoming.ack != nil {
				if a.persist() {
					_ = incoming.ack()
				} else if incoming.nak != nil {
					_ = incoming.nak()
				}
			}
			if incoming.Type == "bot_offline" {
				a.releaseLease()
				a.owner.removeActor(a)
				return
			}
		case <-taskTimer.C:
			if a.online && a.bridgeActive {
				if a.state.Task != nil {
					if a.pendingSnapshotID == "" {
						a.requestSnapshot()
					}
				}
				if a.pendingDecisionReason != "" && !a.decisionPending && time.Since(a.lastDecisionAt) >= minDecisionGap {
					if a.pendingSnapshotID == "" {
						a.requestSnapshot()
					}
				} else if a.state.Task == nil && time.Now().After(a.nextIdleDecision) {
					a.pendingDecisionReason = "periodic_check"
					a.nextIdleDecision = time.Now().Add(a.owner.cfg.decisionInterval +
						decisionJitter(a.botGUID, a.owner.cfg.decisionInterval))
					a.requestSnapshot()
				}
			}
		case <-leaseTimer.C:
			if !a.renewLease() {
				a.leaseHeld = false
				a.owner.removeActor(a)
				return
			}
		case <-a.stop:
			a.releaseLease()
			a.owner.removeActor(a)
			return
		}
	}
}

func (c *controller) removeActor(a *actor) {
	c.actorsMu.Lock()
	defer c.actorsMu.Unlock()
	if c.actors[a.botGUID] == a {
		delete(c.actors, a.botGUID)
	}
}

func (c *controller) shutdown() {
	c.actorsMu.Lock()
	actors := make([]*actor, 0, len(c.actors))
	for _, a := range c.actors {
		actors = append(actors, a)
	}
	c.actorsMu.Unlock()
	for _, a := range actors {
		close(a.stop)
	}
}

func (a *actor) leaseKey() string {
	return "playerbot-agent:" + a.owner.cfg.subjectPrefix + ":lease:" + a.botGUID
}

func (a *actor) stateKey() string {
	return "playerbot-agent:" + a.owner.cfg.subjectPrefix + ":state:" + a.botGUID
}

func (a *actor) acquireLease() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return a.owner.redis.SetNX(ctx, a.leaseKey(), a.leaseToken, actorLeaseTTL).Val()
}

func (a *actor) renewLease() bool {
	const script = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := a.owner.redis.Eval(ctx, script, []string{a.leaseKey()}, a.leaseToken,
		actorLeaseTTL.Milliseconds()).Int()
	return err == nil && result == 1
}

func (a *actor) releaseLease() {
	if !a.leaseHeld {
		return
	}
	const script = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = a.owner.redis.Eval(ctx, script, []string{a.leaseKey()}, a.leaseToken).Result()
	a.leaseHeld = false
}

func (a *actor) handleEvent(incoming event) {
	if incoming.decision != nil {
		a.handleDecisionResult(*incoming.decision)
		return
	}
	if a.state.OwnerToken != "" && incoming.OwnerToken != a.state.OwnerToken && incoming.Type != "bot_online" {
		return
	}
	if incoming.OwnerEpoch != "" && a.state.OwnerEpoch != "" && incoming.OwnerEpoch != a.state.OwnerEpoch &&
		ownerEpochOlder(incoming.OwnerEpoch, a.state.OwnerEpoch) {
		return
	}
	if incoming.EventID != "" && incoming.EventID == a.state.LastEventID {
		return
	}
	if incoming.EventID != "" {
		a.state.LastEventID = incoming.EventID
	}
	if incoming.Type != "primitive_progress" && incoming.Type != "bot_heartbeat" {
		a.revision++
	}
	if incoming.OwnerToken != a.state.OwnerToken ||
		(incoming.OwnerEpoch != "" && a.state.OwnerEpoch != "" && incoming.OwnerEpoch != a.state.OwnerEpoch) {
		if a.state.Task != nil {
			a.state.Task.OperationID = ""
			a.state.Task.Phase = "select"
			a.state.Task.LastProgressUTC = time.Now().UTC()
		}
		a.pendingSnapshotID = ""
	}
	if incoming.OwnerToken != "" {
		a.state.OwnerToken = incoming.OwnerToken
	}
	if incoming.OwnerEpoch != "" {
		a.state.OwnerEpoch = incoming.OwnerEpoch
	}
	if incoming.OwnerToken != "" {
		a.ownerToken = incoming.OwnerToken
	}
	if (incoming.Type == "chat_received" || incoming.Type == "group_invite_received") &&
		incoming.Timestamp > 0 && time.Since(time.UnixMilli(incoming.Timestamp)) > 2*time.Minute {
		a.online = true
		return
	}
	if snapshotBytes := snapshotFromEvent(incoming); len(snapshotBytes) > 0 {
		var current snapshot
		if err := json.Unmarshal(snapshotBytes, &current); err == nil {
			a.latest = current
			a.hasSnapshot = true
			a.snapshotUpdatedAt = time.Now().UTC()
			if a.state.Profile == "" && current.Bot.Name != "" {
				a.state.Profile = a.owner.population.BotProfile(a.botGUID)
				if a.state.Profile == "" {
					a.state.Profile = fmt.Sprintf("%s is a level %d playerbot (class id %d, race id %d). Develop a distinct, steady personality from ongoing interactions.",
						current.Bot.Name, current.Bot.Level, current.Bot.ClassID, current.Bot.RaceID)
				}
				a.persist()
			}
		}
	}
	switch incoming.Type {
	case "snapshot", "primitive_progress", "bot_heartbeat":
	default:
		a.rememberEvent(incoming)
	}

	switch incoming.Type {
	case "bot_online":
		a.online = true
		a.persist()
		if a.bridgeActive && a.state.Task != nil {
			a.requestSnapshot()
		}
	case "bot_offline":
		a.online = false
		a.bridgeActive = false
		a.persist()
	case "agent_control_active":
		a.bridgeActive = true
		if a.state.Task != nil || a.pendingDecisionReason != "" {
			a.requestSnapshot()
		}
	case "agent_control_inactive":
		a.bridgeActive = false
		a.pendingSnapshotID = ""
		if a.state.Task != nil {
			a.state.Task.OperationID = ""
			a.state.Task.Phase = "select"
			a.state.Task.LastProgressUTC = time.Now().UTC()
			a.persist()
		}
	case "bot_heartbeat":
		a.online = true
		var heartbeat struct {
			BridgeActive bool `json:"bridge_active"`
		}
		_ = json.Unmarshal(incoming.Payload, &heartbeat)
		a.bridgeActive = heartbeat.BridgeActive
		if a.bridgeActive && (a.state.Task != nil || a.pendingDecisionReason != "") &&
			(!a.hasSnapshot || time.Since(a.snapshotUpdatedAt) > time.Minute) {
			a.requestSnapshot()
		}
	case "snapshot":
		if incoming.RequestID != "" && incoming.RequestID == a.pendingSnapshotID {
			a.pendingSnapshotID = ""
			a.pendingSnapshotAt = time.Time{}
			if a.state.Task != nil {
				a.advanceTask()
			}
			if a.online && a.pendingDecisionReason != "" {
				reason := a.pendingDecisionReason
				a.pendingDecisionReason = ""
				a.decide(reason)
			}
		}
	case "chat_received", "group_invite_received":
		a.online = true
		a.pendingDecisionReason = incoming.Type
		if a.bridgeActive {
			a.requestSnapshot()
		}
	case "operation_result":
		if !a.bridgeActive {
			return
		}
		if a.state.Task != nil {
			a.handleTaskOperation(incoming)
		} else if operationStatus(incoming) != "accepted" && !isNonDeliberativePrimitive(incoming) {
			a.pendingDecisionReason = "primitive_completed"
			a.requestSnapshot()
		}
	case "primitive_progress":
		if a.state.Task != nil && eventOperationID(incoming) == a.state.Task.OperationID {
			a.state.Task.LastProgressUTC = time.Now().UTC()
			a.persist()
		}
	case "group_changed", "location_changed":
		if !a.bridgeActive {
			return
		}
		a.pendingDecisionReason = incoming.Type
		a.requestSnapshot()
	case "quest_progress":
		if !a.bridgeActive {
			return
		}
		if a.state.Task == nil {
			a.pendingDecisionReason = incoming.Type
		}
		a.requestSnapshot()
	}
}

func ownerEpochOlder(candidate, current string) bool {
	candidateValue, candidateErr := strconv.ParseUint(candidate, 10, 64)
	currentValue, currentErr := strconv.ParseUint(current, 10, 64)
	return candidateErr == nil && currentErr == nil && candidateValue < currentValue
}

func snapshotFromEvent(incoming event) json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(incoming.Payload, &object) != nil {
		return nil
	}
	if raw := object["snapshot"]; len(raw) != 0 {
		return raw
	}
	if object["schema_version"] != nil {
		return incoming.Payload
	}
	return nil
}

func (a *actor) rememberEvent(incoming event) {
	a.recent = append(a.recent, recentEvent{Type: incoming.Type, Payload: append(json.RawMessage(nil), incoming.Payload...), New: true})
	if len(a.recent) > maxEvents {
		a.recent = a.recent[len(a.recent)-maxEvents:]
	}
}

func (a *actor) cancelTaskPrimitive() {
	if a.state.Task != nil {
		a.sendPrimitive("cancel", map[string]any{})
	}
}

func (a *actor) decide(reason string) {
	if !a.online || !a.bridgeActive || !a.hasSnapshot || a.owner.cfg.modelEndpoint == "" || a.owner.cfg.modelName == "" {
		return
	}
	if a.decisionPending {
		a.pendingDecisionReason = reason
		return
	}
	if !a.lastDecisionAt.IsZero() && time.Since(a.lastDecisionAt) < minDecisionGap {
		a.pendingDecisionReason = reason
		return
	}
	job := decisionJob{
		actor: a, revision: a.revision, profile: a.state.Profile,
		memories: append([]string(nil), a.state.Memories...), state: a.latest,
		events: append([]recentEvent(nil), a.recent...), task: cloneTask(a.state.Task), trigger: reason,
	}
	select {
	case a.owner.modelJobs <- job:
		a.decisionPending = true
		a.lastDecisionAt = time.Now().UTC()
		a.consumeRecentEvents()
	default:
		a.pendingDecisionReason = reason
		log.Printf("agent decision queue full; defer bot %s", a.botGUID)
	}
}

func cloneTask(value *task) *task {
	if value == nil {
		return nil
	}
	copy := *value
	copy.AreaCenter = append([]float64(nil), value.AreaCenter...)
	return &copy
}

func (a *actor) handleDecisionResult(result decisionResult) {
	a.decisionPending = false
	if result.revision != a.revision {
		if a.hasNewEvents() {
			a.decide("newer_event")
		}
		a.persist()
		return
	}
	if result.err != nil {
		log.Printf("LLM decision failed for bot %s: %v", a.botGUID, result.err)
		a.pendingDecisionReason = "provider_error"
		a.persist()
		return
	}
	if result.call != nil {
		a.revision++
		a.applyTool(*result.call)
	}
	a.persist()
}

func (a *actor) hasNewEvents() bool {
	for _, item := range a.recent {
		if item.New {
			return true
		}
	}
	return false
}

func (a *actor) consumeRecentEvents() {
	for index := range a.recent {
		a.recent[index].New = false
	}
}

type toolCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func (a *actor) applyTool(call toolCall) {
	var args map[string]json.RawMessage
	if err := json.Unmarshal([]byte(call.Arguments), &args); err != nil {
		return
	}
	switch call.Name {
	case "send_chat":
		a.sendPrimitive("send_chat", rawMapToAny(args))
	case "invite_to_group", "accept_group_invite", "decline_group_invite":
		a.sendPrimitive(call.Name, rawMapToAny(args))
	case "navigate_to_player", "follow_player":
		a.startPlayerNavigationTask(call.Name, args)
	case "navigate_to_destination":
		a.startDestinationTask(args)
	case "work_on_quest":
		a.startQuestTask(args)
	case "kill_count":
		a.startKillTask(args)
	case "gather_resources":
		a.startGatherTask(args)
	case "stop_task":
		if a.state.Task != nil {
			reason := "cancelled by the agent"
			_ = json.Unmarshal(args["reason"], &reason)
			a.finishTask("cancelled", reason)
		} else {
			a.sendPrimitive("cancel", map[string]any{})
		}
	case "remember":
		var memory string
		_ = json.Unmarshal(args["memory"], &memory)
		if memory != "" {
			a.state.Memories = append(a.state.Memories, trimUTF8(memory, 500))
			if len(a.state.Memories) > 32 {
				a.state.Memories = a.state.Memories[len(a.state.Memories)-32:]
			}
			for memoryBytes(a.state.Memories) > maxMemoryBytes && len(a.state.Memories) > 1 {
				a.state.Memories = a.state.Memories[1:]
			}
			a.persist()
		}
	case "set_identity":
		var profile string
		_ = json.Unmarshal(args["profile"], &profile)
		if profile != "" {
			a.state.Profile = trimUTF8(profile, 1000)
			a.persist()
		}
	default:
		log.Printf("unknown model tool %q for bot %s", call.Name, a.botGUID)
	}
}

func rawMapToAny(input map[string]json.RawMessage) map[string]any {
	result := make(map[string]any, len(input))
	for key, raw := range input {
		var value any
		if err := json.Unmarshal(raw, &value); err == nil {
			result[key] = value
		}
	}
	return result
}

func (a *actor) startKillTask(args map[string]json.RawMessage) {
	var target uint32
	var count uint32
	var radius float64
	_ = json.Unmarshal(args["creature_entry"], &target)
	_ = json.Unmarshal(args["count"], &count)
	_ = json.Unmarshal(args["radius"], &radius)
	if target == 0 || count == 0 {
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "kill_count" &&
		a.state.Task.CreatureEntry == target && a.state.Task.GoalCount == count {
		return
	}
	a.cancelTaskPrimitive()
	if radius <= 0 || radius > 1000 {
		radius = 80
	}
	a.state.Task = &task{ID: newID(), Kind: "kill_count", CreatureEntry: target, GoalCount: count,
		Radius: radius, MapID: a.latest.Bot.MapID, AreaCenter: append([]float64(nil), a.latest.Bot.Position...),
		Phase: "select", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) startGatherTask(args map[string]json.RawMessage) {
	var profession string
	var count uint32
	var radius float64
	_ = json.Unmarshal(args["profession"], &profession)
	_ = json.Unmarshal(args["count"], &count)
	_ = json.Unmarshal(args["radius"], &radius)
	if profession == "" || count == 0 {
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "gather_resources" &&
		a.state.Task.Profession == profession && a.state.Task.GoalCount == count {
		return
	}
	a.cancelTaskPrimitive()
	if radius <= 0 || radius > 1000 {
		radius = 300
	}
	a.state.Task = &task{ID: newID(), Kind: "gather_resources", Profession: profession, GoalCount: count,
		Radius: radius, MapID: a.latest.Bot.MapID, AreaCenter: append([]float64(nil), a.latest.Bot.Position...),
		Phase: "select", LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) startQuestTask(args map[string]json.RawMessage) {
	var questID uint32
	_ = json.Unmarshal(args["quest_id"], &questID)
	if questID == 0 {
		a.rejectTool("work_on_quest", "quest_id is required")
		return
	}
	var quest *questInfo
	for index := range a.latest.Quests {
		if a.latest.Quests[index].QuestID == questID {
			quest = &a.latest.Quests[index]
			break
		}
	}
	if quest == nil || (quest.StatusName != "incomplete" && quest.StatusName != "complete") {
		a.rejectTool("work_on_quest", "quest is not in the bot's quest log")
		return
	}
	if quest.Type != 0 || quest.SuggestedPlayers >= 2 {
		a.rejectTool("work_on_quest", "the external quest loop currently supports solo quests only")
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "quest" && a.state.Task.QuestID == questID {
		return
	}
	a.cancelTaskPrimitive()
	a.state.Task = &task{ID: newID(), Kind: "quest", QuestID: questID, Phase: "select",
		LastProgressUTC: time.Now().UTC()}
	a.persist()
	a.advanceTask()
}

func (a *actor) rejectTool(name, reason string) {
	payload := mustJSON(map[string]string{"tool": name, "reason": reason})
	a.recent = append(a.recent, recentEvent{Type: "tool_rejected", Payload: payload, New: true})
	if len(a.recent) > maxEvents {
		a.recent = a.recent[len(a.recent)-maxEvents:]
	}
	a.decide("tool_rejected")
}

func (a *actor) advanceTask() {
	current := a.state.Task
	if current == nil || !a.online || !a.bridgeActive || !a.hasSnapshot {
		return
	}
	if current.OperationID != "" {
		timeout := 2 * time.Minute
		if current.Phase == "combat" {
			timeout = combatOperationTimeout
		} else if current.Phase == "travel" || current.Phase == "turnin_travel" {
			timeout = travelOperationTimeout
		}
		if !current.LastProgressUTC.IsZero() && time.Since(current.LastProgressUTC) > timeout {
			a.sendPrimitive("cancel", map[string]any{})
			current.OperationID = ""
			current.Retries++
			if current.Retries >= 3 {
				a.finishTask("blocked", "core primitive timed out repeatedly")
				return
			}
			current.Phase = "select"
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			a.requestSnapshot()
		}
		return
	}
	switch current.Kind {
	case "kill_count":
		a.advanceKillTask(current)
	case "gather_resources":
		a.advanceGatherTask(current)
	case "quest":
		a.advanceQuestTask(current)
	case "navigate_player", "navigate_to_player", "follow_player":
		a.advancePlayerNavigationTask(current)
	case "navigate_destination":
		a.advanceDestinationTask(current)
	default:
		a.finishTask("failed", "unsupported task type")
	}
}

func (a *actor) startPlayerNavigationTask(kind string, args map[string]json.RawMessage) {
	if kind == "navigate_to_player" {
		kind = "navigate_player"
	}
	var targetName string
	_ = json.Unmarshal(args["player_name"], &targetName)
	var targetGUID string
	_ = json.Unmarshal(args["target_guid"], &targetGUID)
	if targetName == "" && targetGUID == "" {
		a.rejectTool(kind, "player_name or target_guid is required")
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == kind &&
		(a.state.Task.TargetName == targetName || a.state.Task.TargetGUID == targetGUID) {
		return
	}
	a.cancelTaskPrimitive()
	goalDistance := 5.0
	_ = json.Unmarshal(args["distance"], &goalDistance)
	current := &task{ID: newID(), Kind: kind, TargetName: targetName, TargetGUID: targetGUID,
		GoalDistance: goalDistance, Phase: "moving", MapID: a.latest.Bot.MapID,
		LastProgressUTC: time.Now().UTC()}
	a.state.Task = current
	current.OperationID = a.sendPrimitive("navigate_to_player", map[string]any{
		"player_name": targetName, "target_guid": targetGUID,
	})
	a.persist()
}

func (a *actor) startDestinationTask(args map[string]json.RawMessage) {
	var destination string
	_ = json.Unmarshal(args["destination"], &destination)
	if destination == "" {
		a.rejectTool("navigate_to_destination", "destination is required")
		return
	}
	if a.state.Task != nil && a.state.Task.Kind == "navigate_destination" &&
		a.state.Task.DestinationName == destination {
		return
	}
	a.cancelTaskPrimitive()
	current := &task{ID: newID(), Kind: "navigate_destination", DestinationName: destination,
		Phase: "travel", MapID: a.latest.Bot.MapID, LastProgressUTC: time.Now().UTC()}
	a.state.Task = current
	current.OperationID = a.sendPrimitive("navigate_to_destination", map[string]any{"destination": destination})
	a.persist()
}

func (a *actor) advancePlayerNavigationTask(current *task) {
	var target *playerInfo
	for index := range a.latest.NearbyPlayers {
		player := &a.latest.NearbyPlayers[index]
		if (current.TargetGUID != "" &&
			(player.GUID == current.TargetGUID || player.GUIDRaw == current.TargetGUID)) ||
			(current.TargetName != "" && player.Name == current.TargetName) {
			target = player
			break
		}
	}
	if target != nil {
		if target.MapID != 0 && target.MapID != a.latest.Bot.MapID {
			a.finishTask("blocked", "player moved to another map")
			return
		}
		current.TargetGUID = target.GUIDRaw
		distance := target.Distance
		if len(target.Position) >= 3 && len(a.latest.Bot.Position) >= 3 {
			distance = positionDistance(a.latest.Bot.Position, target.Position)
		}
		if (current.Kind == "navigate_player" || current.Kind == "navigate_to_player") && distance <= current.GoalDistance {
			a.finishTask("completed", "arrived at player")
			return
		}
		if current.Kind == "follow_player" && distance <= current.GoalDistance+3.0 {
			current.LastProgressUTC = time.Now().UTC()
			return
		}
	}
	if !current.LastProgressUTC.IsZero() && time.Since(current.LastProgressUTC) < rescanInterval {
		return
	}
	current.OperationID = a.sendPrimitive("navigate_to_player", map[string]any{
		"player_name": current.TargetName, "target_guid": current.TargetGUID,
	})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func positionDistance(left, right []float64) float64 {
	if len(left) < 3 || len(right) < 3 {
		return math.MaxFloat64
	}
	dx, dy, dz := left[0]-right[0], left[1]-right[1], left[2]-right[2]
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

func (a *actor) advanceDestinationTask(current *task) {
	travel := a.latest.TravelTarget
	if travel != nil {
		if travel.IsWorking {
			a.finishTask("completed", "arrived at destination")
			return
		}
		if travel.IsTraveling {
			current.Retries = 0
			current.LastProgressUTC = time.Now().UTC()
			return
		}
	}
	if !current.LastProgressUTC.IsZero() && time.Since(current.LastProgressUTC) < rescanInterval {
		return
	}
	current.Retries++
	if current.Retries >= 3 {
		a.finishTask("blocked", "destination could not be reached")
		return
	}
	current.OperationID = a.sendPrimitive("navigate_to_destination",
		map[string]any{"destination": current.DestinationName})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func targetStatus(state snapshot, guid string) (found, alive, lootPossible bool, healthPct uint32) {
	if state.Bot.CurrentTarget != nil && state.Bot.CurrentTarget.GUID == guid {
		return true, state.Bot.CurrentTarget.Alive, state.Bot.CurrentTarget.LootPossible,
			state.Bot.CurrentTarget.HealthPct
	}
	for _, creature := range state.NearbyCreatures {
		if creature.GUID == guid {
			return true, true, false, creature.HealthPct
		}
	}
	return false, false, false, 0
}

func (a *actor) advanceKillTask(current *task) {
	if a.latest.Bot.MapID != current.MapID {
		a.finishTask("blocked", "bot left the kill area")
		return
	}
	if current.Completed >= current.GoalCount {
		a.finishTask("completed", "kill and loot target reached")
		return
	}
	if current.Phase == "combat" {
		found, alive, lootPossible, healthPct := targetStatus(a.latest, current.TargetGUID)
		if found && alive {
			if healthPct < current.LastTargetHealthPct {
				current.LastTargetHealthPct = healthPct
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
			}
			if time.Since(current.LastProgressUTC) > combatOperationTimeout {
				a.finishTask("blocked", "combat made no progress against the selected target")
			}
			return
		}
		if a.latest.Bot.HasAvailableLoot || lootPossible {
			current.Phase = "loot"
			current.LastScanUTC = time.Time{}
			current.OperationID = a.sendPrimitive("loot_target", map[string]any{"target_guid": current.TargetGUID})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
		current.Completed++
		current.TargetGUID = ""
		current.Phase = "select"
		current.LastScanUTC = time.Time{}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		if current.Completed >= current.GoalCount {
			a.finishTask("completed", "kill and loot target reached")
			return
		}
	}
	if current.Phase == "loot" {
		_, _, lootPossible, _ := targetStatus(a.latest, current.TargetGUID)
		if a.latest.Bot.HasAvailableLoot || lootPossible {
			if current.LastScanUTC.IsZero() || time.Since(current.LastScanUTC) >= 5*time.Second {
				current.LastScanUTC = time.Now().UTC()
				current.OperationID = a.sendPrimitive("loot_target", map[string]any{"target_guid": current.TargetGUID})
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
			}
			return
		}
		current.Completed++
		current.TargetGUID = ""
		current.Phase = "select"
		current.LastScanUTC = time.Time{}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		if current.Completed >= current.GoalCount {
			a.finishTask("completed", "kill and loot target reached")
			return
		}
	}
	for _, creature := range a.latest.NearbyCreatures {
		if creature.Entry != current.CreatureEntry || creature.GUID == "" || creature.Distance > current.Radius ||
			!withinArea(creature.Position, current.AreaCenter, current.Radius) {
			continue
		}
		current.TargetGUID = creature.GUID
		current.LastTargetHealthPct = creature.HealthPct
		current.Phase = "combat"
		current.LastScanUTC = time.Time{}
		current.OperationID = a.sendPrimitive("engage_target", map[string]any{"target_guid": creature.GUID})
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if !current.LastScanUTC.IsZero() && time.Since(current.LastScanUTC) < rescanInterval {
		return
	}
	current.LastScanUTC = time.Now().UTC()
	current.Phase = "scanning"
	current.OperationID = a.sendPrimitive("move_random", map[string]any{})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) advanceGatherTask(current *task) {
	if a.latest.Bot.MapID != current.MapID {
		a.finishTask("blocked", "bot left the gathering area")
		return
	}
	if current.Completed >= current.GoalCount {
		a.finishTask("completed", "resource-node count reached")
		return
	}
	wantedSkill := map[string]uint32{"herbalism": 182, "mining": 186, "fishing": 356}[current.Profession]
	if wantedSkill == 0 {
		a.finishTask("failed", "unsupported gathering profession")
		return
	}
	if current.Phase == "gather_verify" {
		for _, object := range a.latest.NearbyGameObjects {
			if object.GUID == current.TargetGUID && object.CanGather {
				current.Retries++
				if current.Retries >= 3 {
					a.finishTask("blocked", "resource node remained unavailable after repeated interaction")
					return
				}
				current.Phase = "gathering"
				current.LastScanUTC = time.Time{}
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
				return
			}
		}
		current.Completed++
		current.TargetGUID = ""
		current.Phase = "select"
		current.Retries = 0
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		if current.Completed >= current.GoalCount {
			a.finishTask("completed", "resource-node count reached")
		}
		return
	}
	if current.Phase == "gathering" {
		for _, object := range a.latest.NearbyGameObjects {
			if object.GUID != current.TargetGUID || !object.CanGather {
				continue
			}
			if current.LastScanUTC.IsZero() || time.Since(current.LastScanUTC) >= 5*time.Second {
				current.LastScanUTC = time.Now().UTC()
				current.OperationID = a.sendPrimitive("gather_target", map[string]any{"target_guid": current.TargetGUID})
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
			}
			return
		}
		current.Completed++
		current.TargetGUID = ""
		current.Phase = "select"
		current.LastScanUTC = time.Time{}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		if current.Completed >= current.GoalCount {
			a.finishTask("completed", "resource-node count reached")
			return
		}
	}
	for _, object := range a.latest.NearbyGameObjects {
		if !object.CanGather || object.GUID == "" || object.GatherSkill != wantedSkill ||
			object.Distance > current.Radius || !withinArea(object.Position, current.AreaCenter, current.Radius) {
			continue
		}
		current.TargetGUID = object.GUID
		current.Phase = "gathering"
		current.LastScanUTC = time.Time{}
		current.OperationID = a.sendPrimitive("gather_target", map[string]any{"target_guid": object.GUID})
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	if !current.LastScanUTC.IsZero() && time.Since(current.LastScanUTC) < rescanInterval {
		return
	}
	current.LastScanUTC = time.Now().UTC()
	current.Phase = "scanning"
	current.OperationID = a.sendPrimitive("move_random", map[string]any{})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) advanceQuestTask(current *task) {
	var quest *questInfo
	for index := range a.latest.Quests {
		if a.latest.Quests[index].QuestID == current.QuestID {
			quest = &a.latest.Quests[index]
			break
		}
	}
	if quest == nil {
		if current.Phase == "verify_turnin" || current.Phase == "turnin_interaction" {
			a.finishTask("completed", "quest turned in")
		} else {
			a.finishTask("blocked", "quest is not in the bot's active quest log")
		}
		return
	}

	if current.Phase == "combat" {
		found, alive, lootPossible, healthPct := targetStatus(a.latest, current.TargetGUID)
		if found && alive {
			if healthPct < current.LastTargetHealthPct {
				current.LastTargetHealthPct = healthPct
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
			}
			if time.Since(current.LastProgressUTC) > combatOperationTimeout {
				a.finishTask("blocked", "quest combat made no progress")
			}
			return
		}
		if a.latest.Bot.HasAvailableLoot || lootPossible {
			current.Phase = "quest_loot"
			current.LastScanUTC = time.Time{}
			current.OperationID = a.sendPrimitive("loot_target", map[string]any{"target_guid": current.TargetGUID})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
		current.TargetGUID = ""
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
	}
	if current.Phase == "quest_loot" {
		_, _, lootPossible, _ := targetStatus(a.latest, current.TargetGUID)
		if a.latest.Bot.HasAvailableLoot || lootPossible {
			if current.LastScanUTC.IsZero() || time.Since(current.LastScanUTC) >= 5*time.Second {
				current.LastScanUTC = time.Now().UTC()
				current.OperationID = a.sendPrimitive("loot_target", map[string]any{"target_guid": current.TargetGUID})
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
			}
			return
		}
		current.TargetGUID = ""
		current.Phase = "select"
		current.LastScanUTC = time.Time{}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
	}
	if current.Phase == "travel" {
		travel := a.latest.TravelTarget
		if travel != nil && travel.IsWorking {
			current.Phase = "select"
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
		} else if travel != nil && travel.IsTraveling {
			return
		} else {
			current.Retries++
			if current.Retries >= 3 {
				a.finishTask("blocked", "quest objective navigation failed repeatedly")
				return
			}
			current.OperationID = a.sendPrimitive("navigate_to_quest_objective", map[string]any{
				"quest_id": current.QuestID, "objective_index": current.ObjectiveIndex,
			})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
	}
	if current.Phase == "turnin_travel" {
		travel := a.latest.TravelTarget
		if travel != nil && travel.IsWorking {
			current.Phase = "turnin_giver"
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
		} else if travel != nil && travel.IsTraveling {
			return
		} else {
			current.Retries++
			if current.Retries >= 3 {
				a.finishTask("blocked", "quest turn-in navigation failed repeatedly")
				return
			}
			current.OperationID = a.sendPrimitive("navigate_to_quest_turnin", map[string]any{
				"quest_id": current.QuestID,
			})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
	}

	var objective *questObjective
	for index := range quest.Objectives {
		if quest.Objectives[index].Count < quest.Objectives[index].Required {
			objective = &quest.Objectives[index]
			break
		}
	}
	if objective == nil {
		if quest.StatusName != "complete" {
			a.finishTask("blocked", "no supported incomplete quest objective")
			return
		}
		if current.Phase == "verify_turnin" {
			a.finishTask("blocked", "quest giver did not complete the turn-in")
			return
		}
		for _, npc := range a.latest.NearbyNPCs {
			if !npc.QuestGiver || npc.GUID == "" {
				continue
			}
			current.Phase = "turnin_interaction"
			current.TargetGUID = npc.GUID
			current.OperationID = a.sendPrimitive("interact_quest_giver", map[string]any{
				"quest_id": current.QuestID, "target_guid": npc.GUID,
			})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			return
		}
		if current.Phase != "turnin_giver" {
			current.Phase = "turnin_travel"
			current.OperationID = a.sendPrimitive("navigate_to_quest_turnin", map[string]any{
				"quest_id": current.QuestID,
			})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
		} else if current.Retries >= 3 {
			a.finishTask("blocked", "no nearby quest giver after repeated turn-in attempts")
		} else {
			current.Retries++
			current.Phase = "turnin_travel"
			current.OperationID = a.sendPrimitive("navigate_to_quest_turnin", map[string]any{
				"quest_id": current.QuestID,
			})
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
		}
		return
	}
	if objective.Index != current.ObjectiveIndex {
		current.ObjectiveIndex = objective.Index
		current.ObjectiveCount = objective.Count
		current.Retries = 0
	} else if objective.Count > current.ObjectiveCount {
		current.ObjectiveCount = objective.Count
		current.Retries = 0
	}
	if objective.Entry == 0 && objective.ItemID != 0 {
		a.finishTask("blocked", "item objective source selection is not supported yet")
		return
	}

	entry := objective.Entry
	if entry < 0 {
		gameObjectEntry := uint32(-entry)
		for _, object := range a.latest.NearbyGameObjects {
			if object.Entry == gameObjectEntry {
				current.TargetGUID = object.GUID
				current.Phase = "interacting"
				current.Retries = 0
				current.OperationID = a.sendPrimitive("use_gameobject", map[string]any{"target_guid": object.GUID})
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
				return
			}
		}
	} else if entry != 0 {
		for _, creature := range a.latest.NearbyCreatures {
			if creature.Entry == uint32(entry) {
				current.TargetGUID = creature.GUID
				current.Phase = "combat"
				current.ObjectiveIndex = objective.Index
				current.ObjectiveCount = objective.Count
				current.Retries = 0
				current.LastTargetHealthPct = creature.HealthPct
				current.OperationID = a.sendPrimitive("engage_target", map[string]any{"target_guid": creature.GUID})
				current.LastProgressUTC = time.Now().UTC()
				a.persist()
				return
			}
		}
	}

	if current.Retries >= 3 {
		a.finishTask("blocked", "quest objective could not be reached or found")
		return
	}
	current.Retries++
	current.Phase = "travel"
	current.ObjectiveIndex = objective.Index
	current.OperationID = a.sendPrimitive("navigate_to_quest_objective", map[string]any{
		"quest_id": current.QuestID, "objective_index": objective.Index,
	})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleTaskOperation(incoming event) {
	current := a.state.Task
	if current == nil {
		return
	}
	var result struct {
		OperationID string `json:"operation_id"`
		Operation   string `json:"operation"`
		Status      string `json:"status"`
		Reason      string `json:"reason"`
		TargetGUID  string `json:"target_guid"`
	}
	if json.Unmarshal(incoming.Payload, &result) != nil || result.OperationID == "" ||
		result.OperationID != current.OperationID {
		return
	}
	if result.Status == "accepted" {
		current.OperationID = ""
		current.Retries = 0
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	}
	current.OperationID = ""
	if result.Status != "completed" {
		if result.Status == "rejected" && result.Operation == "gather_target" &&
			current.Kind == "gather_resources" && current.Phase == "gathering" {
			current.OperationID = ""
			current.Phase = "gather_verify"
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			a.requestSnapshot()
			return
		}
		if result.Status == "rejected" && result.Operation == "loot_target" &&
			(current.Phase == "loot" || current.Phase == "quest_loot") {
			if current.Kind == "kill_count" {
				current.Completed++
			}
			current.TargetGUID = ""
			current.Phase = "select"
			current.LastProgressUTC = time.Now().UTC()
			a.persist()
			a.requestSnapshot()
			return
		}
		current.Retries++
		if current.Retries >= 3 {
			a.finishTask("blocked", "primitive repeatedly failed: "+result.Reason)
			return
		}
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	}
	current.Retries = 0

	switch current.Phase {
	case "combat":
		if current.Kind == "quest" {
			current.Phase = "quest_loot"
		} else {
			current.Phase = "loot"
		}
		if result.TargetGUID != "" {
			current.TargetGUID = result.TargetGUID
		}
		current.OperationID = a.sendPrimitive("loot_target", map[string]any{"target_guid": current.TargetGUID})
	case "loot", "quest_loot", "gathering", "interacting":
		if current.Kind != "quest" {
			current.Completed++
		} else {
			if current.Phase == "interacting" {
				current.Retries++
				if current.Retries >= 3 {
					a.finishTask("blocked", "quest object interaction did not advance the objective")
					return
				}
			}
			current.Phase = "select"
		}
		current.TargetGUID = ""
		current.LastScanUTC = time.Time{}
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	case "travel":
		if current.Kind == "quest" {
			current.Retries++
			if current.Retries >= 3 {
				a.finishTask("blocked", "quest objective remained unavailable after repeated travel")
				return
			}
		}
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	case "turnin_travel":
		current.Phase = "turnin_giver"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	case "turnin_interaction":
		current.Phase = "verify_turnin"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	case "scanning":
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	default:
		current.Phase = "select"
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		a.requestSnapshot()
		return
	}
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) requestSnapshot() {
	if !a.bridgeActive || (a.pendingSnapshotID != "" && time.Since(a.pendingSnapshotAt) < 10*time.Second) {
		return
	}
	a.pendingSnapshotID = newID()
	a.pendingSnapshotAt = time.Now().UTC()
	a.sendCommand(a.pendingSnapshotID, "snapshot", map[string]any{})
}

func (a *actor) sendPrimitive(operation string, arguments any) string {
	operationID := newID()
	requestID := newID()
	a.sendCommandWithID(requestID, operationID, operation, arguments)
	return operationID
}

func (a *actor) sendCommand(requestID, operation string, arguments any) {
	a.sendCommandWithID(requestID, "", operation, arguments)
}

func (a *actor) sendCommandWithID(requestID, operationID, operation string, arguments any) {
	argumentBytes, err := json.Marshal(arguments)
	if err != nil {
		log.Printf("encode command arguments for %s: %v", a.botGUID, err)
		return
	}
	body, err := json.Marshal(command{
		Version: 1, BotGUID: a.botGUID, OwnerToken: a.ownerToken, OwnerEpoch: a.state.OwnerEpoch,
		DeadlineUnix: time.Now().Add(30 * time.Second).UnixMilli(), RequestID: requestID,
		OperationID: operationID, Operation: operation, Arguments: argumentBytes,
	})
	if err != nil {
		log.Printf("encode command for %s: %v", a.botGUID, err)
		return
	}
	subject := a.owner.cfg.subjectPrefix + ".commands." + a.ownerToken
	if err := a.owner.nats.Publish(subject, body); err != nil {
		log.Printf("publish command for bot %s: %v", a.botGUID, err)
	}
}

func (a *actor) finishTask(status, reason string) {
	if a.state.Task == nil {
		return
	}
	a.cancelTaskPrimitive()
	finished := *a.state.Task
	a.state.Task = nil
	a.recent = append(a.recent, recentEvent{Type: "task_result", Payload: mustJSON(map[string]any{
		"task_id": finished.ID, "kind": finished.Kind, "status": status, "reason": reason,
		"completed": finished.Completed, "goal": finished.GoalCount,
	}), New: true})
	if len(a.recent) > maxEvents {
		a.recent = a.recent[len(a.recent)-maxEvents:]
	}
	a.pendingDecisionReason = "task_" + status
	a.persist()
	log.Printf("agent task ended bot=%s kind=%s status=%s reason=%q progress=%d/%d",
		a.botGUID, finished.Kind, status, reason, finished.Completed, finished.GoalCount)
	a.requestSnapshot()
}

func (a *actor) persist() bool {
	a.state.OwnerToken = a.ownerToken
	a.state.RecentEvents = append([]recentEvent(nil), a.recent...)
	a.state.PendingDecisionReason = a.pendingDecisionReason
	a.state.DecisionPending = a.decisionPending
	data, err := json.Marshal(a.state)
	if err != nil {
		log.Printf("marshal agent state for %s: %v", a.botGUID, err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.owner.redis.Set(ctx, a.stateKey(), data, 0).Err(); err != nil {
		log.Printf("persist agent state for %s: %v", a.botGUID, err)
		return false
	}
	return true
}

func (a *actor) load() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	data, err := a.owner.redis.Get(ctx, a.stateKey()).Bytes()
	if err == redis.Nil {
		a.state.Profile = a.owner.population.BotProfile(a.botGUID)
		a.persist()
		return
	}
	if err != nil {
		log.Printf("load agent state for %s: %v", a.botGUID, err)
		a.state.Profile = a.owner.population.BotProfile(a.botGUID)
		return
	}
	if err := json.Unmarshal(data, &a.state); err != nil {
		log.Printf("decode agent state for %s: %v", a.botGUID, err)
		a.state.Profile = a.owner.population.BotProfile(a.botGUID)
	}
	if a.state.Profile == "" {
		a.state.Profile = a.owner.population.BotProfile(a.botGUID)
	}
	a.ownerToken = a.state.OwnerToken
	a.recent = append([]recentEvent(nil), a.state.RecentEvents...)
	a.pendingDecisionReason = a.state.PendingDecisionReason
	if a.state.DecisionPending {
		a.state.DecisionPending = false
		a.pendingDecisionReason = "recovered_decision"
	}
}

func operationStatus(incoming event) string {
	var payload struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(incoming.Payload, &payload)
	return payload.Status
}

func eventOperationID(incoming event) string {
	var payload struct {
		OperationID string `json:"operation_id"`
	}
	_ = json.Unmarshal(incoming.Payload, &payload)
	return payload.OperationID
}

func isNonDeliberativePrimitive(incoming event) bool {
	var payload struct {
		Operation string `json:"operation"`
	}
	if json.Unmarshal(incoming.Payload, &payload) != nil {
		return false
	}
	switch payload.Operation {
	case "send_chat", "invite_to_group", "accept_group_invite", "decline_group_invite":
		return true
	default:
		return false
	}
}

func withinArea(position, center []float64, radius float64) bool {
	if len(center) < 3 || len(position) < 3 || radius <= 0 {
		return true
	}
	dx := position[0] - center[0]
	dy := position[1] - center[1]
	dz := position[2] - center[2]
	return math.Sqrt(dx*dx+dy*dy+dz*dz) <= radius
}

func (c *controller) startHealthServer() {
	port := envOr("AGENT_HEALTH_PORT", "8098")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Printf("health server stopped: %v", err)
	}
}

func newID() string {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw[:])
}

func trimUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func memoryBytes(memories []string) int {
	bytes := 0
	for _, memory := range memories {
		bytes += len(memory)
	}
	return bytes
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return encoded
}
