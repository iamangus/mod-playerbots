package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSocialPolicyUsesFreshConsentAndPreservesGroupWork(t *testing.T) {
	a, _ := craftingActor(t)
	a.owner.cfg.socialProgression, a.owner.cfg.socialRealm = true, 1
	a.latest.Bot.Level = 15
	a.observeSocialAffinities(event{Timestamp: time.Now().UnixMilli(), Payload: mustJSON(socialAffinities{Realm: 1, Friends: []humanAffinity{{GUID: 10, Name: "Human", Online: true, Level: 10, Zone: 12}}})})
	policy := a.socialPolicy(time.Now())
	if policy.Progression != "favor_low_xp" || policy.Friend.GUID != 10 {
		t.Fatal("friend level pacing missing")
	}
	if a.socialToolAllowed("kill_count") {
		t.Fatal("ahead solo bot started XP farm")
	}
	a.latest.Bot.GroupSize = 2
	if !a.socialToolAllowed("kill_count") || !a.socialPolicy(time.Now()).KeepGroupTask {
		t.Fatal("friend pacing overrode party obligations")
	}
	if policy := a.socialPolicy(time.Now().Add(4 * time.Minute)); policy.Presence != "unknown" || policy.Progression != "independent" {
		t.Fatal("stale presence treated as an authoritative logout/level ceiling")
	}
}

func TestSocialPolicyStableFriendArbitrationAndUnfriend(t *testing.T) {
	a, _ := craftingActor(t)
	a.owner.cfg.socialProgression, a.owner.cfg.socialRealm = true, 1
	now := time.Now()
	a.state.SocialPreferredFriend = 20
	a.state.SocialAffinities = &socialAffinities{Realm: 1, ObservedAt: now.UnixMilli(), Friends: []humanAffinity{{GUID: 10, Name: "A", Online: true, Level: 10}, {GUID: 20, Name: "B", Online: true, Level: 20}}}
	if a.socialPolicy(now).Friend.GUID != 20 {
		t.Fatal("new friend caused regional oscillation")
	}
	a.state.SocialAffinities.Friends[1].Online = false
	if a.socialPolicy(now).Friend.GUID != 10 {
		t.Fatal("offline selection prevented another online friendship")
	}
	a.state.SocialAffinities.Friends = nil
	if a.socialPolicy(now).Presence != "unlinked" || a.state.SocialPreferredFriend != 0 {
		t.Fatal("unfriend failed to remove steering")
	}
}

func TestSocialPolicyRejectsWrongRealmAndDoesNotBindChat(t *testing.T) {
	a, _ := craftingActor(t)
	a.owner.cfg.socialProgression, a.owner.cfg.socialRealm = true, 1
	a.rememberEvent(event{Type: "chat_received", Timestamp: time.Now().UnixMilli(), Payload: mustJSON(map[string]any{"sender_name": "Human"})})
	if a.socialPolicy(time.Now()).Friend != nil {
		t.Fatal("casual chat created affinity")
	}
	a.observeSocialAffinities(event{Timestamp: time.Now().UnixMilli(), Payload: mustJSON(socialAffinities{Realm: 2, Friends: []humanAffinity{{GUID: 10, Name: "Human", Online: true, Level: 10}}})})
	if a.state.SocialAffinities != nil {
		t.Fatal("cross-realm consent accepted")
	}
	a.owner.cfg.socialProgression = false
	if a.socialPolicy(time.Now()) != nil || !a.socialToolAllowed("kill_count") {
		t.Fatal("disabled feature changed default behavior")
	}
}

func TestSocialOfflineGraceRequiresContinuousFreshEvidence(t *testing.T) {
	a, _ := craftingActor(t)
	a.owner.cfg.socialProgression, a.owner.cfg.socialRealm = true, 1
	a.owner.cfg.socialOfflineGrace = 10 * time.Minute
	now := time.Now()
	a.state.SocialAffinities = &socialAffinities{Realm: 1, ObservedAt: now.UnixMilli(), Friends: []humanAffinity{{GUID: 10}}}
	a.state.SocialOfflineSince = now.Add(-9 * time.Minute).UnixMilli()
	if !a.socialToolAllowed("kill_count") {
		t.Fatal("grace period blocked independent work")
	}
	a.state.SocialOfflineSince = now.Add(-11 * time.Minute).UnixMilli()
	if a.socialPolicy(now).Progression != "favor_low_xp" || a.socialToolAllowed("kill_count") {
		t.Fatal("confirmed offline progression was unlimited")
	}
	a.latest.Bot.GroupSize = 2
	if !a.socialToolAllowed("kill_count") {
		t.Fatal("offline grace abandoned party")
	}
	a.latest.Bot.GroupSize = 0
	a.state.SocialAffinities.ObservedAt = now.Add(-4 * time.Minute).UnixMilli()
	if !a.socialToolAllowed("kill_count") {
		t.Fatal("stale evidence inferred logout")
	}
	a.observeSocialAffinities(event{Timestamp: now.UnixMilli(), Payload: mustJSON(socialAffinities{Realm: 1, Friends: []humanAffinity{{GUID: 10}}})})
	if a.state.SocialOfflineSince != now.UnixMilli() || !a.socialToolAllowed("kill_count") {
		t.Fatal("unknown presence gap falsely counted as confirmed offline time")
	}
	var restored persistedAgent
	if err := json.Unmarshal(mustJSON(a.state), &restored); err != nil || restored.SocialOfflineSince != a.state.SocialOfflineSince {
		t.Fatal("offline grace lost on restart")
	}
}

func TestSocialPresenceHeartbeatDoesNotInvalidateDecision(t *testing.T) {
	a, _ := craftingActor(t)
	a.owner.cfg.socialProgression, a.owner.cfg.socialRealm = true, 1
	now := time.Now()
	payload := mustJSON(socialAffinities{Realm: 1, Friends: []humanAffinity{{GUID: 10, Name: "Human", Online: true, Level: 10}}})
	a.handleEvent(event{Type: "social_affinity", Timestamp: now.Add(-time.Second).UnixMilli(), Payload: payload})
	revision := a.revision
	a.handleEvent(event{Type: "social_affinity", Timestamp: now.UnixMilli(), Payload: payload})
	if a.revision != revision {
		t.Fatal("unchanged friendship heartbeat invalidated an in-flight decision")
	}
}
