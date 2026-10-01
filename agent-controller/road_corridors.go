package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

type roadCorridor struct {
	ID          string      `json:"id"`
	Destination string      `json:"destination"`
	Map         uint32      `json:"map_id"`
	Evidence    string      `json:"evidence"`
	Points      [][]float64 `json:"points"`
}

func loadRoadCorridors(path string) ([]roadCorridor, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() > 1024*1024 {
		return nil, fmt.Errorf("road corridor file unavailable or exceeds 1 MiB")
	}
	var corridors []roadCorridor
	decoder := json.NewDecoder(io.LimitReader(file, 1024*1024+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&corridors); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("road corridor file must contain one JSON document")
	}
	if len(corridors) > 128 {
		return nil, fmt.Errorf("at most 128 road corridors supported")
	}
	seen := make(map[string]bool)
	for _, corridor := range corridors {
		switch corridor.Map {
		case 0, 1, 530, 571: // Outdoor continents only; never dungeon/BG/arena movement.
		default:
			return nil, fmt.Errorf("road corridors require an outdoor continent map")
		}
		if corridor.ID == "" || seen[corridor.ID] || strings.TrimSpace(corridor.Evidence) == "" || corridor.Destination == "" || len(corridor.Points) < 2 || len(corridor.Points) > 32 {
			return nil, fmt.Errorf("invalid road corridor metadata/size")
		}
		seen[corridor.ID] = true
		for index, point := range corridor.Points {
			if !validGatheringPosition(point) || (index > 0 && positionDistance(corridor.Points[index-1], point) > 500) {
				return nil, fmt.Errorf("invalid or excessively long corridor segment")
			}
		}
	}
	return corridors, nil
}

func (a *actor) preferredRoadCorridor(destination string) *roadCorridor {
	if a.owner == nil || !a.owner.cfg.roadPreference || a.latest.Bot.InCombat || a.latest.Bot.InFlight || !validGatheringPosition(a.latest.Bot.Position) {
		return nil
	}
	var best *roadCorridor
	bestLength := math.Inf(1)
	for index := range a.owner.cfg.roadCorridors {
		corridor := &a.owner.cfg.roadCorridors[index]
		if corridor.Map != a.latest.Bot.MapID || !strings.EqualFold(corridor.Destination, destination) || len(corridor.Points) < 2 ||
			a.state.RoadCooldowns[corridor.ID].After(time.Now()) || positionDistance(a.latest.Bot.Position, corridor.Points[0]) > 100 {
			continue
		}
		length := positionDistance(a.latest.Bot.Position, corridor.Points[0])
		for point := 1; point < len(corridor.Points); point++ {
			length += positionDistance(corridor.Points[point-1], corridor.Points[point])
		}
		direct := positionDistance(a.latest.Bot.Position, corridor.Points[len(corridor.Points)-1])
		// A conservative geometric lower bound prevents large detours; it is not
		// presented as the actual native path length or evidence that terrain is road.
		if direct < 10 || length > direct*2 || length > 5000 {
			continue
		}
		if length < bestLength || (length == bestLength && best != nil && corridor.ID < best.ID) {
			best, bestLength = corridor, length
		}
	}
	return best
}

func (a *actor) advanceRoadCorridor(current *task) {
	if a.latest.Bot.InCombat || a.latest.Bot.InFlight {
		return
	}
	if a.owner == nil || !a.owner.cfg.roadPreference || a.latest.Bot.MapID != current.MapID || current.Retries != 0 || current.RoadPoint >= uint32(len(current.RoadPoints)) {
		a.fallbackRoadCorridor(current)
		return
	}
	point := current.RoadPoints[current.RoadPoint]
	if !validGatheringPosition(point) {
		a.fallbackRoadCorridor(current)
		return
	}
	current.Phase = "road_segment"
	current.OperationID = a.sendPrimitive("navigate_to_position", map[string]any{"map_id": current.MapID, "x": point[0], "y": point[1], "z": point[2]})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}

func (a *actor) handleRoadCorridor(status, reason string, persistent bool) {
	current := a.state.Task
	if status == "accepted" && persistent {
		return
	}
	current.OperationID = ""
	if status != "completed" || reason != "observed position reached" {
		a.fallbackRoadCorridor(current)
		return
	}
	current.RoadPoint++
	if current.RoadPoint >= uint32(len(current.RoadPoints)) {
		current.Phase = "travel"
		current.OperationID = a.sendPrimitive("navigate_to_destination", map[string]any{"destination": current.DestinationName})
		current.LastProgressUTC = time.Now().UTC()
		a.persist()
		return
	}
	a.advanceRoadCorridor(current)
}

func (a *actor) fallbackRoadCorridor(current *task) {
	if a.state.RoadCooldowns == nil {
		a.state.RoadCooldowns = make(map[string]time.Time)
	}
	for key, until := range a.state.RoadCooldowns {
		if !until.After(time.Now()) {
			delete(a.state.RoadCooldowns, key)
		}
	}
	if len(a.state.RoadCooldowns) < 128 {
		a.state.RoadCooldowns[current.RoadCorridorID] = time.Now().Add(10 * time.Minute)
	}
	current.RoadPoints, current.Retries, current.Phase = nil, 0, "travel"
	current.OperationID = a.sendPrimitive("navigate_to_destination", map[string]any{"destination": current.DestinationName})
	current.LastProgressUTC = time.Now().UTC()
	a.persist()
}
