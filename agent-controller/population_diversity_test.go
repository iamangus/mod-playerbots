package main

import "testing"

func TestStarterCohortBalancesClassesWithinRace(t *testing.T) {
	m := testPopulationManager(t, 1)
	counts := &populationCounts{ByRace: map[uint32]uint64{1: 20},
		ByClass:     map[uint32]uint64{1: 100, 2: 20, 4: 100, 5: 100, 8: 100, 9: 100},
		ByRaceClass: map[uint32]map[uint32]uint64{1: {2: 20}}}
	allocations := map[uint32]int{}
	for i := 0; i < 60; i++ {
		race, class, ok := m.chooseRaceClass(12, counts)
		if !ok || race != 1 {
			t.Fatalf("invalid Elwynn allocation: race=%d class=%d ok=%v", race, class, ok)
		}
		counts.ByRace[race]++
		counts.ByClass[class]++
		counts.ByRaceClass[race][class]++
		allocations[class]++
	}
	if len(allocations) < len(validRaceClasses[1])-1 || allocations[2] != 0 {
		t.Fatalf("starter cohort replicated global paladin deficit rather than repairing local diversity: %v", allocations)
	}
}

func TestPopulationSnapshotPreservesJointCounts(t *testing.T) {
	counts := parsePopulationSnapshot([]byte(`{"status":"completed","total":5,"counts":[{"race":1,"class":2,"level_band":0,"count":2},{"race":1,"class":2,"level_band":1,"count":1},{"race":1,"class":8,"count":2}],"zones":[]}`))
	if counts == nil || counts.ByRaceClass[1][2] != 3 || counts.ByRaceClass[1][8] != 2 {
		t.Fatalf("joint population counts were lost: %+v", counts)
	}
}

func TestStarterCohortRespectsConditionalClassWeights(t *testing.T) {
	m := testPopulationManager(t, 1)
	m.cfg.classWeights = map[uint32]float64{1: 1, 8: 2}
	counts := &populationCounts{ByRace: map[uint32]uint64{}, ByClass: map[uint32]uint64{},
		ByRaceClass: map[uint32]map[uint32]uint64{1: {}}}
	for i := 0; i < 60; i++ {
		race, class, ok := m.chooseRaceClass(12, counts)
		if !ok || race != 1 || (class != 1 && class != 8) {
			t.Fatalf("weighted allocation selected an excluded class: race=%d class=%d", race, class)
		}
		counts.ByRace[race]++
		counts.ByClass[class]++
		counts.ByRaceClass[race][class]++
	}
	if counts.ByRaceClass[1][1] != 20 || counts.ByRaceClass[1][8] != 40 {
		t.Fatalf("conditional weights not maintained: %+v", counts.ByRaceClass)
	}
}
