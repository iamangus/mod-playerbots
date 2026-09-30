package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPopulationOwnerDiscoveryWithoutBots(t *testing.T) {
	c := &controller{owners: make(map[string]map[uint32]time.Time)}
	m := newPopulationManager(c, populationConfig{maxConcurrent: 1})
	if got := m.activeOwnerToken(); got != "" {
		t.Fatalf("unexpected initial owner: %s", got)
	}
	announce := func(owner string, seen time.Time) {
		t.Helper()
		body, err := json.Marshal(event{Version: 1, Type: "population_owner_online", BotGUID: "population",
			OwnerToken: owner, Timestamp: seen.UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.handlePopulationEvent(body); err != nil {
			t.Fatal(err)
		}
	}
	announce("stale", time.Now().Add(-3*time.Minute))
	if got := m.activeOwnerToken(); got != "" {
		t.Fatalf("replayed stale owner selected: %s", got)
	}
	announce("live", time.Now())
	if got := m.activeOwnerToken(); got != "live" {
		t.Fatalf("empty realm did not discover live owner: %s", got)
	}
	announce("older", time.Now().Add(-time.Minute))
	if got := m.activeOwnerToken(); got != "live" {
		t.Fatalf("older heartbeat replaced live owner: %s", got)
	}
	if len(c.owners) != 0 {
		t.Fatal("population discovery registered a nonexistent bot shard")
	}
	m.ownerSeen = time.Now().Add(-3 * time.Minute)
	if got := m.activeOwnerToken(); got != "" {
		t.Fatalf("expired owner selected: %s", got)
	}
}
