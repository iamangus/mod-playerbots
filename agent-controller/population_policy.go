package main

// Deterministic pure planner for composable population-control policies.
//
// The planner consumes a validated rule set, a live census and outstanding
// commitments, and emits a bounded action plan plus commitment mutations. It
// performs no I/O, no model calls and no time-based side effects: the
// population manager owns persistence, rate limits, hysteresis and native
// receipts. Newcomers are always level 1; existing characters are never
// delevelled or involuntarily relocated.

import (
	"fmt"
	"math"
	"sort"
)

const (
	policyKindStarterArea    = "starter_area"
	policyKindPlayerVicinity = "player_vicinity"

	policyModeInitial  = "initial"
	policyModeMaintain = "maintain"

	policyActionAdmit  = "admit_existing"
	policyActionCreate = "create_level1"
	policyActionLogout = "logout"

	policyCommitmentPending  = "pending"
	policyCommitmentCreated  = "created"
	policyCommitmentReleased = "released"

	policyCommitAdmit    = "admit"
	policyCommitCreate   = "create"
	policyCommitAllocate = "allocate"

	planOK          = "ok"
	planStaleCensus = "stale_census"
	planInvalid     = "invalid_rules"
)

// policyRule is one declarative population-control scheme with a stable ID.
// Rules are independently enabled and may be combined freely.
type policyRule struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"` // starter_area | player_vicinity
	Mode   string `json:"mode"` // initial | maintain (starter_area only)
	Target uint32 `json:"target"`
	// Higher priority wins when hard caps cannot satisfy every rule.
	Priority int `json:"priority,omitempty"`

	// starter_area selector: configured area IDs. Target applies per area.
	Areas []string `json:"areas,omitempty"`

	// player_vicinity selector: radius around every fresh online human.
	RadiusYards float64 `json:"radius_yards,omitempty"`

	// Eligibility filters; empty/zero means unrestricted.
	MinLevel uint32   `json:"min_level,omitempty"`
	MaxLevel uint32   `json:"max_level,omitempty"`
	Factions []uint32 `json:"factions,omitempty"`

	// Explicit replenishment source override; empty resolves the closest
	// eligible starter area to each player.
	SourceArea string `json:"source_area,omitempty"`

	// Explicit cross-faction replenishment opt-in; default false keeps
	// guarded starter areas free of hostile-faction newcomers.
	AllowCrossFaction bool `json:"allow_cross_faction,omitempty"`

	Enabled bool `json:"enabled"`
}

// policyArea is one operator-verified starter subarea. Northshire Valley is a
// subarea of Elwynn Forest, so areas carry their own position and radius
// rather than reusing the racial starting zone. Coordinates come from
// operator-verified data, never invented here.
type policyArea struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	MapID       uint32  `json:"map_id"`
	ZoneID      uint32  `json:"zone_id"`
	AreaID      uint32  `json:"area_id"`
	Faction     uint32  `json:"faction"`
	X           float64 `json:"x"`
	Y           float64 `json:"y"`
	Z           float64 `json:"z"`
	RadiusYards float64 `json:"radius_yards"`
	// ContinentRank orders cross-map closest-source resolution; same-map
	// candidates always win, ties break by rank then ID.
	ContinentRank int `json:"continent_rank,omitempty"`
}

// policyBot is one managed-pool character from the live census.
type policyBot struct {
	GUID       uint32  `json:"guid"`
	Name       string  `json:"name"`
	Race       uint32  `json:"race"`
	Class      uint32  `json:"class"`
	Level      uint32  `json:"level"`
	Faction    uint32  `json:"faction"`
	Online     bool    `json:"online"`
	Busy       bool    `json:"busy"`
	MapID      uint32  `json:"map_id"`
	InstanceID uint32  `json:"instance_id"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Z          float64 `json:"z"`
	ZoneID     uint32  `json:"zone_id"`
	ObservedMs int64   `json:"observed_ms"`
}

// policyHuman is one online real-player observation. Bots never appear here.
type policyHuman struct {
	GUID       uint64  `json:"guid"`
	Name       string  `json:"name"`
	Faction    uint32  `json:"faction"`
	Online     bool    `json:"online"`
	MapID      uint32  `json:"map_id"`
	InstanceID uint32  `json:"instance_id"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	ZoneID     uint32  `json:"zone_id"`
	ObservedMs int64   `json:"observed_ms"`
}

// policyCommitment is durable outstanding replenishment or initial
// allocation state. Commitments are counted separately from observed
// nearby bots so in-progress cohorts are never reported as arrivals.
type policyCommitment struct {
	ID         string  `json:"id"`
	RuleID     string  `json:"rule_id"`
	Kind       string  `json:"kind"` // admit | create | allocate
	AreaID     string  `json:"area_id,omitempty"`
	PlayerGUID uint64  `json:"player_guid,omitempty"`
	BotGUID    uint32  `json:"bot_guid,omitempty"`
	State      string  `json:"state"`
	MapID      uint32  `json:"map_id"`
	InstanceID uint32  `json:"instance_id,omitempty"`
	CenterX    float64 `json:"center_x"`
	CenterY    float64 `json:"center_y"`
	Radius     float64 `json:"radius"`
	CreatedMs  int64   `json:"created_ms"`
}

// policyCaps are hard shared limits across every enabled rule.
type policyCaps struct {
	MaxOnline              uint32 `json:"max_online"`
	MaxPool                uint32 `json:"max_pool"`
	PoolSize               uint32 `json:"pool_size"`
	MaxActionsPerReconcile uint32 `json:"max_actions_per_reconcile"`
}

type policyAction struct {
	Kind       string  `json:"kind"`
	RuleID     string  `json:"rule_id"`
	BotGUID    uint32  `json:"bot_guid,omitempty"`
	PlayerGUID uint64  `json:"player_guid,omitempty"`
	AreaID     string  `json:"area_id,omitempty"`
	Reason     string  `json:"reason"`
	Distance   float64 `json:"distance,omitempty"`
	// CommitID links the action to the commitment persisted for it.
	CommitID string `json:"commit_id,omitempty"`
}

// policyCommitmentOp is a commitment mutation for the manager to persist.
type policyCommitmentOp struct {
	Op  string            `json:"op"` // add | release
	New *policyCommitment `json:"new,omitempty"`
	ID  string            `json:"id,omitempty"`
}

type policyRuleStatus struct {
	RuleID    string   `json:"rule_id"`
	Kind      string   `json:"kind"`
	Mode      string   `json:"mode,omitempty"`
	Target    uint32   `json:"target"`
	Observed  uint32   `json:"observed"`
	Committed uint32   `json:"committed"`
	Satisfied uint32   `json:"satisfied"`
	Shortfall uint32   `json:"shortfall"`
	Notes     []string `json:"notes,omitempty"`
}

type policyPlan struct {
	Status      string               `json:"status"`
	Actions     []policyAction       `json:"actions,omitempty"`
	Commitments []policyCommitmentOp `json:"commitment_ops,omitempty"`
	Statuses    []policyRuleStatus   `json:"rule_statuses,omitempty"`
	Notes       []string             `json:"notes,omitempty"`
}

func policyDistance(x1, y1, x2, y2 float64) float64 {
	return math.Hypot(x1-x2, y1-y2)
}

func policyFactionAllowed(factions []uint32, faction uint32) bool {
	if len(factions) == 0 {
		return true
	}
	for _, f := range factions {
		if f == faction {
			return true
		}
	}
	return false
}

func (r *policyRule) validate(areaByID map[string]policyArea) error {
	if r.ID == "" {
		return fmt.Errorf("rule id is required")
	}
	if r.Target == 0 {
		return fmt.Errorf("rule %s: target must be positive", r.ID)
	}
	switch r.Kind {
	case policyKindStarterArea:
		if r.Mode != policyModeInitial && r.Mode != policyModeMaintain {
			return fmt.Errorf("rule %s: mode must be initial or maintain", r.ID)
		}
		if len(r.Areas) == 0 {
			return fmt.Errorf("rule %s: starter_area rules require at least one area", r.ID)
		}
		for _, areaID := range r.Areas {
			if _, ok := areaByID[areaID]; !ok {
				return fmt.Errorf("rule %s: unknown area %q", r.ID, areaID)
			}
		}
	case policyKindPlayerVicinity:
		if r.RadiusYards <= 0 {
			return fmt.Errorf("rule %s: player_vicinity rules require a positive radius", r.ID)
		}
		if r.SourceArea != "" {
			if _, ok := areaByID[r.SourceArea]; !ok {
				return fmt.Errorf("rule %s: unknown source area %q", r.ID, r.SourceArea)
			}
		}
	default:
		return fmt.Errorf("rule %s: unknown kind %q", r.ID, r.Kind)
	}
	return nil
}

// botEligible applies the rule's level and faction filters.
func botEligible(r *policyRule, b *policyBot) bool {
	if b.Level < r.MinLevel {
		return false
	}
	if r.MaxLevel > 0 && b.Level > r.MaxLevel {
		return false
	}
	return policyFactionAllowed(r.Factions, b.Faction)
}

// botFresh reports whether an online bot's position evidence is current.
func botFresh(b *policyBot, nowMs, freshnessMs int64) bool {
	return b.Online && b.ObservedMs > 0 && nowMs-b.ObservedMs <= freshnessMs
}

// botInRegion tests spatial containment on the same map and instance.
func botInRegion(b *policyBot, mapID uint32, instanceID uint32, cx, cy, radius float64) bool {
	if b.MapID != mapID || b.InstanceID != instanceID {
		return false
	}
	return policyDistance(b.X, b.Y, cx, cy) <= radius
}

// closestStarterArea resolves the closest eligible starter area to a player.
// Same-map areas always precede cross-map ones; cross-map ordering uses the
// configured continent rank then area ID, never invented geography.
func closestStarterArea(h *policyHuman, r *policyRule, areaByID map[string]policyArea) (policyArea, bool) {
	if r.SourceArea != "" {
		if area, ok := areaByID[r.SourceArea]; ok &&
			(area.Faction == 0 || area.Faction == h.Faction || r.AllowCrossFaction) {
			return area, true
		}
	}
	var sameMap, crossMap []policyArea
	for _, area := range areaByID {
		if area.Faction != 0 && area.Faction != h.Faction && !r.AllowCrossFaction {
			continue
		}
		if area.MapID == h.MapID {
			sameMap = append(sameMap, area)
		} else {
			crossMap = append(crossMap, area)
		}
	}
	sort.Slice(sameMap, func(i, j int) bool {
		di := policyDistance(h.X, h.Y, sameMap[i].X, sameMap[i].Y)
		dj := policyDistance(h.X, h.Y, sameMap[j].X, sameMap[j].Y)
		if di != dj {
			return di < dj
		}
		return sameMap[i].ID < sameMap[j].ID
	})
	sort.Slice(crossMap, func(i, j int) bool {
		if crossMap[i].ContinentRank != crossMap[j].ContinentRank {
			return crossMap[i].ContinentRank < crossMap[j].ContinentRank
		}
		return crossMap[i].ID < crossMap[j].ID
	})
	if len(sameMap) > 0 {
		return sameMap[0], true
	}
	if len(crossMap) > 0 {
		return crossMap[0], true
	}
	return policyArea{}, false
}

// areaFactionCompatible reports whether newcomers of a faction may be placed
// in the area. Neutral areas accept any faction; hostile-faction placement
// requires the rule's explicit cross-faction opt-in.
func areaFactionCompatible(area policyArea, faction uint32, allowCrossFaction bool) bool {
	return area.Faction == 0 || area.Faction == faction || allowCrossFaction
}

type plannerState struct {
	areaByID   map[string]policyArea
	botByGUID  map[uint32]*policyBot
	live       []*policyCommitment
	botClaimed map[uint32]bool

	ops     []policyCommitmentOp
	actions []policyAction
	notes   []string

	onlineNow     uint32
	logoutPlanned uint32
	admitPlanned  uint32
	createPlanned uint32
	actionBudget  uint32

	now int64
	seq int
}

// botCommittedLive reports whether a bot is the subject of any commitment.
func (s *plannerState) botCommittedLive(guid uint32) bool {
	for _, c := range s.live {
		if c.BotGUID != 0 && c.BotGUID == guid {
			return true
		}
	}
	return false
}

// botCommittedAnyRule reports whether any rule currently claims the bot.
func (s *plannerState) botCommittedAnyRule(guid uint32) bool {
	return s.botCommittedLive(guid)
}

func (s *plannerState) addCommitment(c policyCommitment) policyCommitment {
	s.seq++
	c.State = policyCommitmentPending
	// Timestamp plus sequence keeps IDs unique across reconciles without a
	// shared counter; the manager persists them verbatim.
	c.ID = fmt.Sprintf("%s/%s/%d-%06d", c.RuleID, c.Kind, s.now, s.seq)
	s.ops = append(s.ops, policyCommitmentOp{Op: "add", New: &c})
	s.live = append(s.live, &c)
	if c.BotGUID != 0 {
		s.botClaimed[c.BotGUID] = true
	}
	return c
}

// reserveAction consumes one action-budget slot for the given action kind.
func (s *plannerState) reserveAction(kind string) bool {
	if s.actionBudget == 0 {
		return false
	}
	s.actionBudget--
	switch kind {
	case policyActionLogout:
		s.logoutPlanned++
	case policyActionAdmit:
		s.admitPlanned++
	case policyActionCreate:
		s.createPlanned++
	}
	return true
}

// planPopulation is the pure planner entry point.
func planPopulation(rules []policyRule, areas []policyArea, bots []policyBot, humans []policyHuman,
	commitments []policyCommitment, caps policyCaps, freshnessMs, nowMs int64) policyPlan {

	plan := policyPlan{Status: planOK}
	areaByID := make(map[string]policyArea, len(areas))
	for _, area := range areas {
		if area.ID == "" || area.RadiusYards <= 0 {
			plan.Status = planInvalid
			plan.Notes = append(plan.Notes, fmt.Sprintf("area %q missing id or radius", area.ID))
			return plan
		}
		areaByID[area.ID] = area
	}

	// Validate the full rule set first; a bad rule leaves the last known
	// good policy in place rather than silently changing population.
	seen := map[string]bool{}
	enabled := make([]*policyRule, 0, len(rules))
	for i := range rules {
		r := &rules[i]
		if !r.Enabled {
			continue
		}
		if seen[r.ID] {
			plan.Status = planInvalid
			plan.Notes = append(plan.Notes, fmt.Sprintf("duplicate rule id %q", r.ID))
			return plan
		}
		seen[r.ID] = true
		if err := r.validate(areaByID); err != nil {
			plan.Status = planInvalid
			plan.Notes = append(plan.Notes, err.Error())
			return plan
		}
		enabled = append(enabled, r)
	}

	// With no enabled rules the policy system is idle: it manages nothing
	// and never mass-pauses the pool.
	if len(enabled) == 0 {
		plan.Status = planOK
		return plan
	}
	plan.Statuses = make([]policyRuleStatus, 0, len(enabled))

	s := &plannerState{
		areaByID:   areaByID,
		botByGUID:  make(map[uint32]*policyBot, len(bots)),
		botClaimed: make(map[uint32]bool),
		now:        nowMs,
	}
	for i := range bots {
		s.botByGUID[bots[i].GUID] = &bots[i]
		if bots[i].Online {
			s.onlineNow++
		}
	}
	s.actionBudget = caps.MaxActionsPerReconcile

	// Census outage guard: with vicinity rules and no fresh evidence at
	// all, act on nothing rather than mass-pausing or creating blindly.
	freshHumans := 0
	freshBots := 0
	anyVicinity := false
	for _, r := range enabled {
		if r.Kind == policyKindPlayerVicinity {
			anyVicinity = true
		}
	}
	for i := range humans {
		h := &humans[i]
		if h.Online && h.ObservedMs > 0 && nowMs-h.ObservedMs <= freshnessMs {
			freshHumans++
		}
	}
	for i := range bots {
		if botFresh(&bots[i], nowMs, freshnessMs) {
			freshBots++
		}
	}
	if anyVicinity && freshHumans == 0 && freshBots == 0 {
		plan.Status = planStaleCensus
		plan.Notes = append(plan.Notes, "no fresh human or bot observations; treating population as unknown")
		return plan
	}

	// Rule modes for release semantics: initial allocations never release;
	// maintain/vicinity commitments release when their bot arrives.
	ruleModes := make(map[string]string, len(enabled))
	for _, r := range enabled {
		ruleModes[r.ID] = r.Mode
	}

	// Release pass: a replenishment commitment whose bot is now observed
	// inside its demand region has arrived; release it so the bot counts
	// exactly once, as an observed bot. Initial allocations are permanent
	// cohort assignments and never release on arrival.
	liveCommitments := make([]*policyCommitment, 0, len(commitments))
	for i := range commitments {
		c := &commitments[i]
		if c.State == policyCommitmentReleased {
			continue
		}
		if mode, known := ruleModes[c.RuleID]; known && mode != policyModeInitial && c.Kind != policyCommitAllocate && c.BotGUID != 0 {
			if b, ok := s.botByGUID[c.BotGUID]; ok && botFresh(b, nowMs, freshnessMs) &&
				botInRegion(b, c.MapID, c.InstanceID, c.CenterX, c.CenterY, c.Radius) {
				s.ops = append(s.ops, policyCommitmentOp{Op: "release", ID: c.ID})
				continue
			}
		}
		liveCommitments = append(liveCommitments, c)
	}
	s.live = liveCommitments
	for _, c := range s.live {
		if c.BotGUID != 0 {
			s.botClaimed[c.BotGUID] = true
		}
	}

	// Logout pass first: unclaimed idle bots free online headroom for the
	// demand pass in the same plan.
	s.planLogouts(enabled, bots, humans, freshnessMs, nowMs)

	// Demand pass in deterministic priority order.
	sort.Slice(enabled, func(i, j int) bool {
		if enabled[i].Priority != enabled[j].Priority {
			return enabled[i].Priority > enabled[j].Priority
		}
		return enabled[i].ID < enabled[j].ID
	})
	for _, r := range enabled {
		var status policyRuleStatus
		switch r.Kind {
		case policyKindStarterArea:
			status = s.planStarterRule(r, bots, caps)
		case policyKindPlayerVicinity:
			status = s.planVicinityRule(r, humans, bots, caps, freshnessMs, nowMs)
		}
		plan.Statuses = append(plan.Statuses, status)
	}

	plan.Actions = s.actions
	plan.Commitments = s.ops
	plan.Notes = s.notes
	return plan
}

// planLogouts marks online bots that no enabled scheme claims as pause
// candidates. Busy bots are never paused. The manager owns the logout grace
// interval; plan actions are candidates, not completed pauses.
func (s *plannerState) planLogouts(rules []*policyRule, bots []policyBot, humans []policyHuman,
	freshnessMs, nowMs int64) {

	claimed := make(map[uint32]bool)

	// Committed bots stay active: initial allocations and in-progress
	// replenishment keep their characters online.
	for _, c := range s.live {
		if c.BotGUID != 0 {
			claimed[c.BotGUID] = true
		}
	}

	for _, r := range rules {
		switch r.Kind {
		case policyKindStarterArea:
			for _, areaID := range r.Areas {
				area := s.areaByID[areaID]
				var inArea []*policyBot
				for i := range bots {
					b := &bots[i]
					if b.Online && botEligible(r, b) &&
						botInRegion(b, area.MapID, 0, area.X, area.Y, area.RadiusYards) {
						inArea = append(inArea, b)
					}
				}
				sort.Slice(inArea, func(i, j int) bool {
					di := policyDistance(inArea[i].X, inArea[i].Y, area.X, area.Y)
					dj := policyDistance(inArea[j].X, inArea[j].Y, area.X, area.Y)
					if di != dj {
						return di < dj
					}
					return inArea[i].GUID < inArea[j].GUID
				})
				limit := r.Target
				if uint32(len(inArea)) < limit {
					limit = uint32(len(inArea))
				}
				for _, b := range inArea[:limit] {
					claimed[b.GUID] = true
				}
			}
		case policyKindPlayerVicinity:
			for i := range humans {
				h := &humans[i]
				if !h.Online || h.ObservedMs == 0 || nowMs-h.ObservedMs > freshnessMs {
					continue
				}
				var nearby []*policyBot
				for j := range bots {
					b := &bots[j]
					if b.Online && botEligible(r, b) &&
						botInRegion(b, h.MapID, h.InstanceID, h.X, h.Y, r.RadiusYards) {
						nearby = append(nearby, b)
					}
				}
				sort.Slice(nearby, func(i, j int) bool {
					di := policyDistance(nearby[i].X, nearby[i].Y, h.X, h.Y)
					dj := policyDistance(nearby[j].X, nearby[j].Y, h.X, h.Y)
					if di != dj {
						return di < dj
					}
					return nearby[i].GUID < nearby[j].GUID
				})
				limit := r.Target
				if uint32(len(nearby)) < limit {
					limit = uint32(len(nearby))
				}
				for _, b := range nearby[:limit] {
					claimed[b.GUID] = true
				}
			}
		}
	}

	var candidates []*policyBot
	for i := range bots {
		b := &bots[i]
		if b.Online && !b.Busy && !claimed[b.GUID] {
			candidates = append(candidates, b)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].GUID < candidates[j].GUID })
	for _, b := range candidates {
		if s.actionBudget == 0 {
			s.notes = append(s.notes, "action budget exhausted during logout pass")
			return
		}
		s.reserveAction(policyActionLogout)
		s.actions = append(s.actions, policyAction{
			Kind: policyActionLogout, BotGUID: b.GUID,
			Reason: "no active policy claim; manager applies logout grace before pausing",
		})
	}
}

// countCommitments sums live commitments matching the predicate.
func (s *plannerState) countCommitments(ruleID string, pred func(*policyCommitment) bool) uint32 {
	count := uint32(0)
	for _, c := range s.live {
		if c.RuleID == ruleID && pred(c) {
			count++
		}
	}
	return count
}

// commitCovered reports whether a commitment's demand region covers a point
// on the same map and instance, so nearby players share replenishment
// instead of each independently re-introducing the same shortage.
func commitCovered(c *policyCommitment, mapID uint32, instanceID uint32, x, y float64) bool {
	if c.MapID != mapID || c.InstanceID != instanceID {
		return false
	}
	return policyDistance(x, y, c.CenterX, c.CenterY) <= c.Radius
}

// canAdmit enforces the hard online ceiling for one reused bot.
func (s *plannerState) canAdmit(caps policyCaps) bool {
	return s.onlineNow-s.logoutPlanned+s.admitPlanned+s.createPlanned+1 <= caps.MaxOnline
}

// canCreate enforces the hard online and pool ceilings for one newcomer.
func (s *plannerState) canCreate(caps policyCaps) bool {
	if s.onlineNow-s.logoutPlanned+s.admitPlanned+s.createPlanned+1 > caps.MaxOnline {
		return false
	}
	if caps.MaxPool > 0 && caps.PoolSize+s.createPlanned+1 > caps.MaxPool {
		return false
	}
	return true
}

// planStarterRule reconciles one initial or maintain starter-area rule. The
// rule target applies per area. Initial allocations are permanent cohort
// assignments that persist when bots travel; maintain areas reconcile live
// occupancy with commitments covering in-progress arrivals.
func (s *plannerState) planStarterRule(r *policyRule, bots []policyBot, caps policyCaps) policyRuleStatus {
	status := policyRuleStatus{RuleID: r.ID, Kind: r.Kind, Mode: r.Mode, Target: r.Target}
	minSatisfied := int64(-1)
	minObserved := uint32(0)

	for areaIdx, areaID := range r.Areas {
		area := s.areaByID[areaID]

		observed := uint32(0)
		var offline []*policyBot
		for i := range bots {
			b := &bots[i]
			if !botEligible(r, b) {
				continue
			}
			if !botInRegion(b, area.MapID, 0, area.X, area.Y, area.RadiusYards) {
				continue
			}
			if b.Online {
				if r.Mode == policyModeInitial && !s.botCommittedAnyRule(b.GUID) {
					// Permanently allocate the existing character into the
					// cohort; it counts through its commitment from now on
					// even after it leaves the area.
					s.addCommitment(policyCommitment{
						RuleID: r.ID, Kind: policyCommitAllocate, AreaID: areaID, BotGUID: b.GUID,
						MapID: area.MapID, CenterX: area.X, CenterY: area.Y, Radius: area.RadiusYards,
						CreatedMs: s.now,
					})
				}
				observed++
			} else {
				offline = append(offline, b)
			}
		}
		sort.Slice(offline, func(i, j int) bool {
			di := policyDistance(offline[i].X, offline[i].Y, area.X, area.Y)
			dj := policyDistance(offline[j].X, offline[j].Y, area.X, area.Y)
			if di != dj {
				return di < dj
			}
			return offline[i].GUID < offline[j].GUID
		})

		committed := s.countCommitments(r.ID, func(c *policyCommitment) bool { return c.AreaID == areaID })
		var satisfied int64
		if r.Mode == policyModeInitial {
			// Satisfaction is allocation-based and persists on departure;
			// observed stays informational.
			satisfied = int64(committed)
		} else {
			satisfied = int64(observed + committed)
		}
		deficit := int64(r.Target) - satisfied

		for unit := int64(0); unit < deficit; unit++ {
			// Reuse an eligible offline bot already located in the area
			// before creating anyone.
			var bot *policyBot
			for _, candidate := range offline {
				if s.botCommittedAnyRule(candidate.GUID) {
					continue
				}
				bot = candidate
				break
			}
			if bot != nil {
				if !s.canAdmit(caps) {
					status.Notes = append(status.Notes, "online cap blocked reuse/admission")
					break
				}
				if !s.reserveAction(policyActionAdmit) {
					status.Notes = append(status.Notes, "action budget exhausted")
					break
				}
				commit := s.addCommitment(policyCommitment{
					RuleID: r.ID, Kind: policyCommitAdmit, AreaID: areaID, BotGUID: bot.GUID,
					MapID: area.MapID, CenterX: area.X, CenterY: area.Y, Radius: area.RadiusYards,
					CreatedMs: s.now,
				})
				s.actions = append(s.actions, policyAction{
					Kind: policyActionAdmit, RuleID: r.ID, BotGUID: bot.GUID, AreaID: areaID,
					Distance: policyDistance(bot.X, bot.Y, area.X, area.Y),
					Reason:   "reuse offline bot already located in the starter area",
					CommitID: commit.ID,
				})
				satisfied++
				continue
			}
			// Creation is always a level-1 character in the area's
			// faction; the manager's chooser picks a native race/class.
			if !s.canCreate(caps) {
				status.Notes = append(status.Notes, "hard cap blocked creation")
				break
			}
			if !s.reserveAction(policyActionCreate) {
				status.Notes = append(status.Notes, "action budget exhausted")
				break
			}
			commit := s.addCommitment(policyCommitment{
				RuleID: r.ID, Kind: policyCommitCreate, AreaID: areaID,
				MapID: area.MapID, CenterX: area.X, CenterY: area.Y, Radius: area.RadiusYards,
				CreatedMs: s.now,
			})
			s.actions = append(s.actions, policyAction{
				Kind: policyActionCreate, RuleID: r.ID, AreaID: areaID,
				Reason:   "create level-1 newcomer in the starter area",
				CommitID: commit.ID,
			})
			satisfied++
		}

		if minSatisfied < 0 || satisfied < minSatisfied {
			minSatisfied = satisfied
		}
		if areaIdx == 0 || observed < minObserved {
			minObserved = observed
		}
	}
	status.Observed = minObserved
	status.Committed = s.countCommitments(r.ID, func(c *policyCommitment) bool { return true })
	status.Satisfied = uint32(minSatisfied)
	if status.Target > status.Satisfied {
		status.Shortfall = status.Target - status.Satisfied
	}
	return status
}

// planVicinityRule reconciles one player-vicinity rule. Every fresh online
// human defines demand within the rule radius; nearby players share bots and
// replenishment commitments geometrically instead of adding per-player
// cohorts. Deficits reuse offline bots already near the player, then create
// level-1 newcomers at the closest eligible starter area to that player.
func (s *plannerState) planVicinityRule(r *policyRule, humans []policyHuman, bots []policyBot,
	caps policyCaps, freshnessMs, nowMs int64) policyRuleStatus {

	status := policyRuleStatus{RuleID: r.ID, Kind: r.Kind, Target: r.Target}

	type playerDemand struct {
		h       *policyHuman
		deficit int64
	}
	demands := make([]playerDemand, 0, len(humans))
	for i := range humans {
		h := &humans[i]
		if !h.Online || h.ObservedMs == 0 || nowMs-h.ObservedMs > freshnessMs {
			continue
		}
		observed := uint32(0)
		for j := range bots {
			b := &bots[j]
			if b.Online && !s.botCommittedLive(b.GUID) && botEligible(r, b) &&
				botInRegion(b, h.MapID, h.InstanceID, h.X, h.Y, r.RadiusYards) {
				observed++
			}
		}
		covered := s.countCommitments(r.ID, func(c *policyCommitment) bool {
			return commitCovered(c, h.MapID, h.InstanceID, h.X, h.Y)
		})
		demands = append(demands, playerDemand{
			h:       h,
			deficit: int64(r.Target) - int64(observed+covered),
		})
	}
	if len(demands) == 0 {
		status.Notes = append(status.Notes, "no fresh online humans; vicinity demand unknown")
		return status
	}
	// Deterministic order: more deficient players first, then GUID.
	sort.Slice(demands, func(i, j int) bool {
		if demands[i].deficit != demands[j].deficit {
			return demands[i].deficit > demands[j].deficit
		}
		return demands[i].h.GUID < demands[j].h.GUID
	})

	minSatisfied := int64(-1)
	minObserved := uint32(0)
	haveObserved := false
	for _, d := range demands {
		h := d.h
		observed := uint32(0)
		var offline []*policyBot
		for i := range bots {
			b := &bots[i]
			if !botEligible(r, b) || !botInRegion(b, h.MapID, h.InstanceID, h.X, h.Y, r.RadiusYards) {
				continue
			}
			if b.Online {
				if !s.botCommittedLive(b.GUID) {
					observed++
				}
			} else {
				offline = append(offline, b)
			}
		}
		sort.Slice(offline, func(i, j int) bool {
			di := policyDistance(offline[i].X, offline[i].Y, h.X, h.Y)
			dj := policyDistance(offline[j].X, offline[j].Y, h.X, h.Y)
			if di != dj {
				return di < dj
			}
			return offline[i].GUID < offline[j].GUID
		})
		covered := s.countCommitments(r.ID, func(c *policyCommitment) bool {
			return commitCovered(c, h.MapID, h.InstanceID, h.X, h.Y)
		})
		// Fresh deficit: commitments added for earlier players in this plan
		// already cover overlapping demand.
		deficit := int64(r.Target) - int64(observed+covered)

		for unit := int64(0); unit < deficit; unit++ {
			var bot *policyBot
			for _, candidate := range offline {
				if s.botCommittedAnyRule(candidate.GUID) {
					continue
				}
				bot = candidate
				break
			}
			if bot != nil {
				if !s.canAdmit(caps) {
					status.Notes = append(status.Notes, "online cap blocked reuse/admission")
					break
				}
				if !s.reserveAction(policyActionAdmit) {
					status.Notes = append(status.Notes, "action budget exhausted")
					break
				}
				commit := s.addCommitment(policyCommitment{
					RuleID: r.ID, Kind: policyCommitAdmit, PlayerGUID: h.GUID, BotGUID: bot.GUID,
					MapID: h.MapID, InstanceID: h.InstanceID, CenterX: h.X, CenterY: h.Y,
					Radius: r.RadiusYards, CreatedMs: s.now,
				})
				s.actions = append(s.actions, policyAction{
					Kind: policyActionAdmit, RuleID: r.ID, BotGUID: bot.GUID, PlayerGUID: h.GUID,
					Distance: policyDistance(bot.X, bot.Y, h.X, h.Y),
					Reason:   "reuse offline bot already located near the player",
					CommitID: commit.ID,
				})
				continue
			}
			// Replenishment source: closest eligible starter area to this
			// player; newcomers are level 1 and travel normally.
			area, ok := closestStarterArea(h, r, s.areaByID)
			if !ok || !areaFactionCompatible(area, h.Faction, r.AllowCrossFaction) {
				status.Notes = append(status.Notes, "no eligible starter area for replenishment")
				break
			}
			if !s.canCreate(caps) {
				status.Notes = append(status.Notes, "hard cap blocked creation")
				break
			}
			if !s.reserveAction(policyActionCreate) {
				status.Notes = append(status.Notes, "action budget exhausted")
				break
			}
			commit := s.addCommitment(policyCommitment{
				RuleID: r.ID, Kind: policyCommitCreate, PlayerGUID: h.GUID, AreaID: area.ID,
				MapID: h.MapID, InstanceID: h.InstanceID, CenterX: h.X, CenterY: h.Y,
				Radius: r.RadiusYards, CreatedMs: s.now,
			})
			s.actions = append(s.actions, policyAction{
				Kind: policyActionCreate, RuleID: r.ID, PlayerGUID: h.GUID, AreaID: area.ID,
				Reason:   fmt.Sprintf("create level-1 newcomer at closest starter area %s", area.ID),
				CommitID: commit.ID,
			})
		}

		satisfied := int64(observed) + int64(s.countCommitments(r.ID, func(c *policyCommitment) bool {
			return commitCovered(c, h.MapID, h.InstanceID, h.X, h.Y)
		}))
		if minSatisfied < 0 || satisfied < minSatisfied {
			minSatisfied = satisfied
		}
		if !haveObserved || observed < minObserved {
			minObserved = observed
			haveObserved = true
		}
	}
	status.Observed = minObserved
	status.Committed = s.countCommitments(r.ID, func(c *policyCommitment) bool { return true })
	if minSatisfied < 0 {
		minSatisfied = 0
	}
	status.Satisfied = uint32(minSatisfied)
	if status.Target > status.Satisfied {
		status.Shortfall = status.Target - status.Satisfied
	}
	return status
}
