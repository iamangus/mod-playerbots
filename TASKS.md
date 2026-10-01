# Agent task queue

Requests moved from [TODO.md](TODO.md) are tracked here. Preserve the original
request and record any clarification, progress, blockers, and verification beneath it.
Use stable IDs such as TASK-001 and statuses: queued, in progress, blocked, done.

## Active and queued

<!-- Each entry: ID/title, status, original request, then agent notes. -->

### TASK-001 — Interaction coverage audit and ranked implementation plan (in progress)

**Original request:** "take some time and think through all the types of interactions that a bot needs to support in order to take itself to max level while engaging with all content. That includes questing, dungeons, professions, auction house, fishing, gathering, battlegrounds all grouped and solo and everything else I'm not thinking of. Think of it all. Then create a list Of tasks and order it from easiest to hardest to implement. What makes it easy is being able to fit it into our loop hierarchy system. Then start implementing"

**Clarification:** "I didn't mean we can't create new loops. We can and we will. I meant that some tasks may be more difficult to build in a system based on a loop hierarchy. That is how we classify it as more difficult to implement."

**Difficulty criterion:** how hard it is to express the interaction as reliable nested loops —
clear per-iteration progress signals and terminal conditions are easy; shared/dynamic/adversarial
state and unpredictable interruptions are hard. New loops are expected and normal.

**Ranked backlog (easiest → hardest):**

1. Quest acceptance loop — bots can work quests already in the log but cannot take new ones.
   Clear signals (quest appears in log); travel-to-giver + gossip accept are linear loops.
2. Loot-all batch loop — controller-side loop over existing loot_target primitive; clear signal
   (no more lootable corpses).
3. Vendor restock/repair/sell-junk — rpg buy/sell/repair actions exist; money/inventory signals.
4. Class trainer loop — rpg train exists; spell list + money signals.
5. Fishing loop — cast → wait for bite → reel → loot; needs a new core loop but is linear with
   clear signals (bobber splash, loot window).
6. Mail loop — check mail / send items at mailbox; existing check mail action + travel.
7. Flight-path discovery/taxi usage — rpg discover/taxi exist; integrates with travel loops.
8. Professions crafting with material dependencies — recursive goal loop (craft X → need Y →
   acquire Y via gather/vendor/trade); branching but per-bot local.
9. Auction house buying — browse/bid/buyout loops; economy state is external and volatile.
10. Dungeon leveling — LFG queue, enter, follow group objectives, reset on wipe; coordination
    plus instance state.
11. Auction house selling — pricing decisions on external state; worse than buying.
12. Guild participation — petition/sign/charter flows involve other players' cooperation.
13. Elite/group quests — suggested-players content with coordinated pulls.
14. Escort quests — protect NPC with unpredictable path; interruption handling is the hard part.
15. Battlegrounds — dynamic adversarial objectives; the whole loop keeps changing under you.
16. Raid/endgame coordination — many-bot choreography, shared roles, wipe recovery.

**Progress:**
- Audited engine docs, controller task/phase machinery, and core AgentRuntime primitives.
- Item 1 (quest acceptance) implemented, tested, committed as c9106418:
  core `accept_quest` + `navigate_to_quest_giver` operations, `available_quests`
  snapshot section (bounded 12 quests, 3 per giver), quest-giver destination lookup,
  controller travel/accept task phases, tool + prompt line, 7 new tests
  (`go test -race` passes), codestyle + clang-format pass.
- Item 2 prerequisite fixed in the same commit: controller retains `in_group`.
- Next: item 2 (loot-all batch loop), then vendor/trainer/fishing loops.
- Controller intentionally left scaled to 0 replicas; core changes go live only
  after a user-authorized build/deploy.

### TASK-002 — Fix nearby-player `in_group` retention (done)

**Original request (context):** "They dont seem to see who they are in a group with" and the
group-confusion reports; the LLM cannot see which nearby characters are already grouped because
the controller dropped the `in_group` field during snapshot decode.

**Outcome:** `playerInfo` now decodes `in_group`; regression test
`TestSnapshotRetainsNearbyPlayerInGroup` added. Shipped in commit c9106418.

## Completed

<!-- Move finished entries here with a concise outcome and verification. -->