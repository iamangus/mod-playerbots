package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	maxGatheringEvidenceSources = 64
	maxGatheringEvidenceItems   = 32
	maxGatheringObservedItems   = 12
	gatheringEvidenceAge        = time.Hour
	maxGatheringSites           = 4
	maxGatheringTravelSites     = 8
)

type gatheringSite struct {
	GUID       string    `json:"guid"`
	MapID      uint32    `json:"map_id"`
	Entry      uint32    `json:"entry"`
	Position   []float64 `json:"position"`
	ObservedAt int64     `json:"observed_at"`
}

type gatheringEvidence struct {
	ItemIDs        []uint32         `json:"item_ids"`
	ObservedAt     int64            `json:"observed_at"`
	ItemObservedAt map[uint32]int64 `json:"item_observed_at,omitempty"`
	Sites          []gatheringSite  `json:"sites,omitempty"`
}

func (value gatheringEvidence) freshItems(now time.Time) []uint32 {
	var items []uint32
	for _, itemID := range value.ItemIDs {
		at := value.ItemObservedAt[itemID]
		if at > 0 && at <= now.UnixMilli() && !time.UnixMilli(at).Before(now.Add(-gatheringEvidenceAge)) {
			items = append(items, itemID)
		}
	}
	return items
}

func gatheringSourceKey(kind string, entry uint32) string {
	return fmt.Sprintf("%s:%d", kind, entry)
}

func (a *actor) observeGatheringSource(incoming event, now time.Time) {
	var sample struct {
		Kind     string    `json:"source_kind"`
		Entry    uint32    `json:"source_entry"`
		ItemIDs  []uint32  `json:"item_ids"`
		GUID     string    `json:"source_guid"`
		MapID    *uint32   `json:"map_id"`
		Position []float64 `json:"position"`
	}
	if json.Unmarshal(incoming.Payload, &sample) != nil || sample.Entry == 0 ||
		(sample.Kind != "gameobject" && sample.Kind != "creature") || len(sample.ItemIDs) == 0 || len(sample.ItemIDs) > maxGatheringObservedItems {
		return
	}
	if incoming.Timestamp <= 0 || incoming.Timestamp > now.Add(time.Minute).UnixMilli() ||
		time.UnixMilli(incoming.Timestamp).Before(now.Add(-gatheringEvidenceAge)) {
		return
	}
	for _, itemID := range sample.ItemIDs {
		if itemID == 0 {
			return
		}
	}
	observedAt := incoming.Timestamp
	if observedAt > now.UnixMilli() {
		observedAt = now.UnixMilli()
	}
	if a.state.GatheringSources == nil {
		a.state.GatheringSources = make(map[string]gatheringEvidence)
	}
	for key, value := range a.state.GatheringSources {
		if time.UnixMilli(value.ObservedAt).Before(now.Add(-gatheringEvidenceAge)) {
			delete(a.state.GatheringSources, key)
		}
	}
	key := gatheringSourceKey(sample.Kind, sample.Entry)
	previous, exists := a.state.GatheringSources[key]
	if exists && previous.ObservedAt > observedAt {
		return // A delayed/replayed sample must not refresh or replace newer evidence.
	}
	if !exists && len(a.state.GatheringSources) >= maxGatheringEvidenceSources {
		oldestKey := ""
		oldest := int64(0)
		for candidate, value := range a.state.GatheringSources {
			if oldestKey == "" || value.ObservedAt < oldest || (value.ObservedAt == oldest && candidate < oldestKey) {
				oldestKey, oldest = candidate, value.ObservedAt
			}
		}
		delete(a.state.GatheringSources, oldestKey)
	}
	items := make([]uint32, 0, maxGatheringEvidenceItems)
	seen := make(map[uint32]bool)
	timestamps := make(map[uint32]int64)
	for _, itemID := range sample.ItemIDs {
		timestamps[itemID] = observedAt
	}
	for _, group := range [][]uint32{sample.ItemIDs, previous.freshItems(now)} {
		for _, itemID := range group {
			if len(items) == maxGatheringEvidenceItems {
				break
			}
			if itemID != 0 && !seen[itemID] {
				items = append(items, itemID)
				seen[itemID] = true
				if timestamps[itemID] == 0 {
					timestamps[itemID] = previous.ItemObservedAt[itemID]
				}
			}
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i] < items[j] })
	sites := make([]gatheringSite, 0, maxGatheringSites)
	validSite := sample.Kind == "gameobject" && sample.GUID != "" && sample.GUID != "0" && sample.MapID != nil && validGatheringPosition(sample.Position)
	if validSite {
		sites = append(sites, gatheringSite{GUID: sample.GUID, MapID: *sample.MapID, Entry: sample.Entry, Position: append([]float64(nil), sample.Position...), ObservedAt: observedAt})
	}
	for _, site := range previous.Sites {
		if len(sites) >= maxGatheringSites {
			break
		}
		if site.ObservedAt > now.UnixMilli() || now.Sub(time.UnixMilli(site.ObservedAt)) > gatheringEvidenceAge || !validGatheringPosition(site.Position) || (validSite && site.GUID == sample.GUID) {
			continue
		}
		sites = append(sites, site)
	}
	a.state.GatheringSources[key] = gatheringEvidence{ItemIDs: items, ObservedAt: observedAt, ItemObservedAt: timestamps, Sites: sites}
	a.persist()
}

func validGatheringPosition(position []float64) bool {
	if len(position) != 3 {
		return false
	}
	for _, coordinate := range position {
		if math.IsNaN(coordinate) || math.IsInf(coordinate, 0) || math.Abs(coordinate) > 20000 {
			return false
		}
	}
	return true
}

func (a *actor) knownGatheringSite(itemID uint32, tried []string) *gatheringSite {
	if len(tried) >= maxGatheringTravelSites || len(a.latest.Bot.Position) < 3 {
		return nil
	}
	var best *gatheringSite
	bestDistance := float64(0)
	now := time.Now()
	for key, evidence := range a.state.GatheringSources {
		fresh := false
		for _, observed := range evidence.freshItems(now) {
			fresh = fresh || observed == itemID
		}
		if !fresh {
			continue
		}
		for _, site := range evidence.Sites {
			if key != gatheringSourceKey("gameobject", site.Entry) || site.MapID != a.latest.Bot.MapID || site.ObservedAt > now.UnixMilli() || now.Sub(time.UnixMilli(site.ObservedAt)) > gatheringEvidenceAge || !validGatheringPosition(site.Position) {
				continue
			}
			seen := false
			for _, guid := range tried {
				seen = seen || guid == site.GUID
			}
			if seen {
				continue
			}
			distance := float64(0)
			for index := 0; index < 3; index++ {
				delta := site.Position[index] - a.latest.Bot.Position[index]
				distance += delta * delta
			}
			if distance > 5000*5000 || distance <= 10*10 {
				continue
			}
			if best == nil || distance < bestDistance || (distance == bestDistance && site.GUID < best.GUID) {
				copy := site
				copy.Position = append([]float64(nil), site.Position...)
				best, bestDistance = &copy, distance
			}
		}
	}
	return best
}

func (a *actor) decorateGatheringEvidence(state *snapshot, now time.Time) {
	for index := range state.NearbyGameObjects {
		object := &state.NearbyGameObjects[index]
		object.ObservedDropItems = nil
		value, exists := a.state.GatheringSources[gatheringSourceKey("gameobject", object.Entry)]
		if exists && value.ObservedAt <= now.UnixMilli() &&
			!time.UnixMilli(value.ObservedAt).Before(now.Add(-gatheringEvidenceAge)) {
			object.ObservedDropItems = value.freshItems(now)
		}
	}
}
