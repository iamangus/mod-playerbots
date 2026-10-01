package main

import (
	"encoding/json"
	"sort"
	"time"
)

const socialPresenceAge = 3 * time.Minute

type humanAffinity struct {
	GUID   uint64 `json:"guid"`
	Name   string `json:"name"`
	Online bool   `json:"online"`
	Level  uint32 `json:"level"`
	Zone   uint32 `json:"zone_id"`
	Map    uint32 `json:"map_id"`
}

type socialAffinities struct {
	Realm      uint32          `json:"realm_id"`
	Friends    []humanAffinity `json:"friends"`
	ObservedAt int64           `json:"observed_at"`
}

type socialProgressionPolicy struct {
	Friend        *humanAffinity `json:"friend,omitempty"`
	Presence      string         `json:"presence"`
	Progression   string         `json:"progression"`
	KeepGroupTask bool           `json:"keep_group_task"`
	RegionalGoal  bool           `json:"prefer_friend_region"`
}

func (a *actor) observeSocialAffinities(incoming event) {
	if a.owner == nil || !a.owner.cfg.socialProgression {
		return
	}
	var observation socialAffinities
	if json.Unmarshal(incoming.Payload, &observation) != nil || observation.Realm != a.owner.cfg.socialRealm || len(observation.Friends) > 8 ||
		incoming.Timestamp <= 0 || incoming.Timestamp > time.Now().Add(time.Minute).UnixMilli() || time.Since(time.UnixMilli(incoming.Timestamp)) > socialPresenceAge {
		return
	}
	if previous := a.state.SocialAffinities; previous != nil && previous.ObservedAt >= incoming.Timestamp {
		return
	}
	seen := make(map[uint64]bool)
	for _, friend := range observation.Friends {
		if friend.GUID == 0 || seen[friend.GUID] || (friend.Online && (friend.Name == "" || friend.Level == 0 || friend.Level > 80)) {
			return
		}
		seen[friend.GUID] = true
	}
	allOffline := len(observation.Friends) > 0
	for _, friend := range observation.Friends {
		if friend.Online {
			allOffline = false
			break
		}
	}
	previous := a.state.SocialAffinities
	if !allOffline {
		a.state.SocialOfflineSince = 0
	} else if a.state.SocialOfflineSince == 0 || previous == nil || incoming.Timestamp-previous.ObservedAt > socialPresenceAge.Milliseconds() {
		a.state.SocialOfflineSince = incoming.Timestamp
	}
	observation.ObservedAt = incoming.Timestamp
	a.state.SocialAffinities = &observation
	before := a.latest.SocialProgression
	a.latest.SocialProgression = a.socialPolicy(time.Now())
	// Presence refreshes alone never cause redundant model calls.
	if string(mustJSON(before)) != string(mustJSON(a.latest.SocialProgression)) {
		a.revision++
		a.rememberEvent(incoming)
		a.pendingDecisionReason = "social_progression_changed"
		a.requestSnapshot()
	}
	a.persist()
}

func (a *actor) socialPolicy(now time.Time) *socialProgressionPolicy {
	if a.owner == nil || !a.owner.cfg.socialProgression {
		return nil
	}
	policy := &socialProgressionPolicy{Presence: "unknown", Progression: "independent", KeepGroupTask: a.latest.Bot.GroupSize >= 2}
	observation := a.state.SocialAffinities
	if observation == nil || observation.Realm != a.owner.cfg.socialRealm || observation.ObservedAt > now.UnixMilli() || now.Sub(time.UnixMilli(observation.ObservedAt)) > socialPresenceAge {
		return policy
	}
	if len(observation.Friends) == 0 {
		policy.Presence = "unlinked"
		a.state.SocialPreferredFriend = 0
		return policy
	}
	policy.Presence = "friends_offline"
	friends := append([]humanAffinity(nil), observation.Friends...)
	sort.Slice(friends, func(i, j int) bool { return friends[i].GUID < friends[j].GUID })
	for _, friend := range friends {
		if !friend.Online {
			continue
		}
		if policy.Friend == nil || friend.GUID == a.state.SocialPreferredFriend {
			copy := friend
			policy.Friend = &copy
		}
		if friend.GUID == a.state.SocialPreferredFriend {
			break
		}
	}
	if policy.Friend == nil {
		grace := a.owner.cfg.socialOfflineGrace
		if grace < time.Minute {
			grace = 10 * time.Minute
		}
		if a.state.SocialOfflineSince > 0 && now.Sub(time.UnixMilli(a.state.SocialOfflineSince)) >= grace {
			policy.Progression = "favor_low_xp"
		}
		return policy
	}
	a.state.SocialPreferredFriend = policy.Friend.GUID
	policy.Presence = "friend_online"
	policy.Progression = "same_band"
	if uint64(a.latest.Bot.Level) > uint64(policy.Friend.Level)+2 {
		policy.Progression = "favor_low_xp"
	}
	if uint64(a.latest.Bot.Level)+2 < uint64(policy.Friend.Level) {
		policy.Progression = "catch_up_normally"
	}
	policy.RegionalGoal = a.latest.Bot.ZoneID != policy.Friend.Zone || a.latest.Bot.MapID != policy.Friend.Map
	return policy
}

func (a *actor) socialToolAllowed(name string) bool {
	policy := a.socialPolicy(time.Now())
	if policy == nil || policy.KeepGroupTask || policy.Progression != "favor_low_xp" {
		return true
	}
	switch name {
	case "work_on_quest", "accept_quest", "kill_count":
		a.rejectTool(name, "friend-linked progression favors independent low-XP crafting, fishing, trading or social play while ahead or after confirmed friends-offline grace; active group work is never interrupted")
		return false
	}
	return true
}
