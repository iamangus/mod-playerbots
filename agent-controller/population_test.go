package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEmptyPopulationSnapshot(t *testing.T) {
	counts := parsePopulationSnapshot(json.RawMessage(`{"status":"completed","total":0,"counts":[],"zones":[]}`))
	if counts == nil {
		t.Fatal("empty realm snapshot must establish a population count for initial provisioning")
	}
	if counts.Total != 0 || len(counts.ByRace) != 0 || len(counts.ByClass) != 0 || len(counts.Zones) != 0 {
		t.Fatalf("unexpected empty realm counts: %+v", counts)
	}
	for _, payload := range []string{
		`{"status":"completed","guid":123,"name":"Example"}`,
		`{"status":"failed","counts":[],"zones":[]}`,
		`{"status":"completed","counts":null,"zones":null}`,
		`{`,
	} {
		if got := parsePopulationSnapshot(json.RawMessage(payload)); got != nil {
			t.Fatalf("non-snapshot or failed result accepted as a population snapshot: %s", payload)
		}
	}
}

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
	// Every non-stale owner announcement must be registered across every owned
	// shard so controller heartbeats reach the native login gate before any bot
	// is online; only stale announcements stay unregistered.
	for owner, shards := range c.owners {
		if owner == "stale" {
			t.Fatal("stale announcement registered an owner")
		}
		if len(shards) != eventShardCount {
			t.Fatalf("owner %s not registered for all owned shards (%d)", owner, len(shards))
		}
	}
	if _, registered := c.owners["live"]; !registered {
		t.Fatal("live owner missing from heartbeat registration")
	}
	// An expired population stamp falls back to recent heartbeat registration so
	// commands still reach a live worldserver when no bot is online.
	m.ownerSeen = time.Now().Add(-3 * time.Minute)
	if got := m.activeOwnerToken(); got != "older" {
		t.Fatalf("heartbeat registration fallback did not select the live owner: %q", got)
	}
	// With no fresh registration either, no owner is selectable.
	c.ownersMu.Lock()
	delete(c.owners, "older")
	delete(c.owners, "live")
	c.ownersMu.Unlock()
	if got := m.activeOwnerToken(); got != "" {
		t.Fatalf("stale owner selected without fresh registration: %q", got)
	}
}
