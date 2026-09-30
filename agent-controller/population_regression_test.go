package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testPopulationManager(t *testing.T, target uint32) *populationManager {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	m := &populationManager{
		owner: &controller{redis: client},
		cfg:   populationConfig{targetTotal: target, maxPerHour: 1000},
	}
	m.storeCounts(populationCounts{ByRace: map[uint32]uint64{}, ByClass: map[uint32]uint64{},
		Zones: map[uint32]uint64{}, SnapshotAt: time.Now().UnixMilli()})
	return m
}

func TestPopulationReservationsBalanceAndCap(t *testing.T) {
	m := testPopulationManager(t, 100)
	for i := 0; i < 100; i++ {
		if _, _, ok := m.reserve(0, "population_refill"); !ok {
			t.Fatalf("reservation %d rejected before reaching target", i)
		}
	}
	if _, _, ok := m.reserve(12, "zone_refill"); ok {
		t.Fatal("zone refill exceeded total target")
	}
	counts := m.effectiveCounts()
	if counts == nil || counts.Total != 100 {
		t.Fatalf("incorrect pending population: %+v", counts)
	}
	for race := range raceStartZones {
		if counts.ByRace[race] < 8 || counts.ByRace[race] > 12 {
			t.Fatalf("imbalanced race %d: %d", race, counts.ByRace[race])
		}
	}
	for class := range validClassRaces {
		if counts.ByClass[class] < 9 || counts.ByClass[class] > 13 {
			t.Fatalf("imbalanced class %d: %d", class, counts.ByClass[class])
		}
	}
}

func TestConcurrentReservationsDoNotOvershoot(t *testing.T) {
	m := testPopulationManager(t, 1)
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			if _, _, ok := m.reserve(0, "population_refill"); ok {
				accepted.Add(1)
			}
		}(i)
	}
	workers.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d reservations for one slot", accepted.Load())
	}
}

func TestConfirmedReservationsCountUntilAuthoritativeSnapshot(t *testing.T) {
	m := testPopulationManager(t, 2)
	requestID, reservation, ok := m.reserve(12, "population_refill")
	if !ok {
		t.Fatal("reservation failed")
	}
	reservation.ConfirmedAt = time.Now().UnixMilli() + 1
	data, _ := json.Marshal(reservation)
	if err := m.owner.redis.Set(context.Background(), m.prefixKey("resv:"+requestID), data,
		populationReservationTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if counts := m.effectiveCounts(); counts.Total != 1 {
		t.Fatalf("creation disappeared before snapshot: %+v", counts)
	}
	m.storeCounts(populationCounts{Total: 1, ByRace: map[uint32]uint64{reservation.Race: 1},
		ByClass: map[uint32]uint64{reservation.Class: 1}, Zones: map[uint32]uint64{12: 1},
		SnapshotAt: reservation.ConfirmedAt})
	if counts := m.effectiveCounts(); counts.Total != 1 || counts.ByRace[reservation.Race] != 1 {
		t.Fatalf("creation double-counted after snapshot: %+v", counts)
	}
}
