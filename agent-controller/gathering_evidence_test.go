package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func evidenceSample(entry uint32, items []uint32, at time.Time) event {
	return event{Type: "gathering_source_observed", Timestamp: at.UnixMilli(),
		Payload: mustJSON(map[string]any{"source_kind": "gameobject", "source_entry": entry, "item_ids": items})}
}

func TestGatheringEvidenceMergesOwnObservedDropsAndPersists(t *testing.T) {
	a, _ := loopTestActor(t)
	now := time.Now()
	a.observeGatheringSource(evidenceSample(1731, []uint32{2770, 2835}, now), now)
	a.observeGatheringSource(evidenceSample(1731, []uint32{2770, 774}, now), now)
	value := a.state.GatheringSources["gameobject:1731"]
	if len(value.ItemIDs) != 3 || value.ItemIDs[0] != 774 || value.ObservedAt != now.UnixMilli() {
		t.Fatalf("wrong source evidence: %+v", value)
	}
	var restored persistedAgent
	if json.Unmarshal(mustJSON(a.state), &restored) != nil || len(restored.GatheringSources["gameobject:1731"].ItemIDs) != 3 {
		t.Fatal("observed source knowledge was not persisted")
	}
}

func TestGatheringEvidenceRejectsUnboundedInvalidAndStaleSamples(t *testing.T) {
	a, _ := loopTestActor(t)
	now := time.Now()
	for _, sample := range []event{
		evidenceSample(0, []uint32{2770}, now), evidenceSample(1731, []uint32{0}, now),
		evidenceSample(1731, nil, now), evidenceSample(1731, make([]uint32, 13), now),
		evidenceSample(1731, []uint32{2770}, now.Add(-2*time.Hour)), evidenceSample(1731, []uint32{2770}, now.Add(time.Hour)),
		{Timestamp: now.UnixMilli(), Payload: json.RawMessage(`{"source_kind":"player","source_entry":1,"item_ids":[2770]}`)},
	} {
		a.observeGatheringSource(sample, now)
	}
	if len(a.state.GatheringSources) != 0 {
		t.Fatal("unsafe source evidence entered the candidate map")
	}
}

func TestGatheringEvidenceIsBoundedAndDoesNotReplaceNewerWithReplay(t *testing.T) {
	a, _ := loopTestActor(t)
	now := time.Now()
	for entry := uint32(1); entry <= maxGatheringEvidenceSources+1; entry++ {
		a.observeGatheringSource(evidenceSample(entry, []uint32{2770}, now.Add(time.Duration(entry)*time.Millisecond)), now.Add(time.Second))
	}
	if len(a.state.GatheringSources) != maxGatheringEvidenceSources {
		t.Fatal("source knowledge grew without a bound")
	}
	if _, retained := a.state.GatheringSources["gameobject:1"]; retained {
		t.Fatal("oldest source was not evicted")
	}
	key := fmt.Sprintf("gameobject:%d", maxGatheringEvidenceSources+1)
	before := a.state.GatheringSources[key]
	a.observeGatheringSource(evidenceSample(maxGatheringEvidenceSources+1, []uint32{999}, now), now.Add(time.Second))
	if after := a.state.GatheringSources[key]; after.ObservedAt != before.ObservedAt || len(after.ItemIDs) != 1 {
		t.Fatal("delayed evidence displaced a newer observation")
	}
}

func TestGatheringEvidenceDecoratesFreshSnapshotsWithoutAliasing(t *testing.T) {
	a, _ := loopTestActor(t)
	now := time.Now()
	a.observeGatheringSource(evidenceSample(1731, []uint32{2770}, now), now)
	var state snapshot
	if json.Unmarshal([]byte(`{"nearby_game_objects":[{"entry":1731,"observed_drop_items":[999]}]}`), &state) != nil {
		t.Fatal("invalid test snapshot")
	}
	a.decorateGatheringEvidence(&state, now)
	if len(state.NearbyGameObjects[0].ObservedDropItems) != 1 || state.NearbyGameObjects[0].ObservedDropItems[0] != 2770 {
		t.Fatal("snapshot accepted unobserved drop claims")
	}
	state.NearbyGameObjects[0].ObservedDropItems[0] = 888
	if a.state.GatheringSources["gameobject:1731"].ItemIDs[0] != 2770 {
		t.Fatal("model observation aliases persisted source knowledge")
	}
	a.decorateGatheringEvidence(&state, now.Add(2*time.Hour))
	if len(state.NearbyGameObjects[0].ObservedDropItems) != 0 {
		t.Fatal("expired evidence remained visible as a source")
	}
}

func TestGatheringEvidenceDoesNotInvalidateOrSpendOnRoutineLoot(t *testing.T) {
	a, _ := loopTestActor(t)
	a.revision = 5
	sample := evidenceSample(1731, []uint32{2770}, time.Now())
	sample.OwnerToken = "owner"
	a.handleEvent(sample)
	if a.revision != 5 || a.decisionPending || a.pendingDecisionReason != "" || len(a.recent) != 1 {
		t.Fatal("source metadata invalidated deliberation or triggered a model request")
	}
}

func TestGatheringEvidenceNewDropsDoNotRefreshOldItems(t *testing.T) {
	a, _ := loopTestActor(t)
	now := time.Now()
	a.observeGatheringSource(evidenceSample(1731, []uint32{2770}, now), now)
	a.observeGatheringSource(evidenceSample(1731, []uint32{774}, now.Add(50*time.Minute)), now.Add(50*time.Minute))
	items := a.state.GatheringSources["gameobject:1731"].freshItems(now.Add(70 * time.Minute))
	if len(items) != 1 || items[0] != 774 {
		t.Fatal("unrelated new observation extended old item evidence")
	}
}
