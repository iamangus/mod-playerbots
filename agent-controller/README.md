# Playerbot agent-controller

This process owns agent profiles, memories, high-level task loops, and model calls
outside the AzerothCore process. Run it as a Kubernetes StatefulSet, independently
scalable from the ToCloud9 worldservers. It consumes its assigned virtual NATS event
shards and publishes commands to the current owning worldserver using the event's
owner token.

`work_on_quest` supports ordinary kill/credit objectives and quest-item loot from
native-observed creature or gameobject sources. Item objectives retain their own
slot and item identity; quest counts, not navigation or loot-command admission,
prove progress. Repeated arrivals without objective progress have a bounded search
budget. Native item-source projections are capped at 32 entries and disclose
truncation; unsupported or missing sources fail clearly rather than being invented.
Native snapshots expose `objective_navigation_blocked_reason` for incomplete
quests outside the supported level/content limits. Choose other eligible work or
level up before retrying those objectives; completed quests can still be turned
in. Path rejections log native path flags and exact start/goal only when the
per-operation path type changes, without treating movement admission as arrival.

`recover_death` is an explicit controller-selected task: release spirit, walk back
to a same-map corpse, wait for the ordinary reclaim delay, and reclaim through the
normal player handler. Completion requires native-observed resurrection. It grants
no free repairs, instant revival, or teleport to the corpse. Cross-map, arena and
battleground recovery remain unsupported. Ordinary navigation to a spirit healer
does not substitute for recovery, and legacy autonomous death actions stay disabled.

## Chat decision pipeline (TypeSafe Jev)

Incoming chat is decided by TypeSafe's System One model (Jev), not by the task
model. One evaluation request carries the whole battery against a compact state
(bot summary, current task, recent chat, enumerated candidates):

- `should_communicate` (noul): gate; below `AGENT_CHAT_DECIDE_THRESHOLD` the bot
  stays silent.
- `audience` (choice): recent chat senders, group members and nearby players,
  enumerated from the native snapshot with stable `person_N` keys. The model
  cannot address anyone outside the snapshot; unknown or low-confidence
  (`AGENT_CHAT_AUDIENCE_CONFIDENCE_MIN`) audience answers send nothing.
- `channel` (choice): only channels this code can validate (say, yell, general,
  whisper with a person, party with a group). Invalid combinations fall back to
  `/say`.
- `intent` (choice): reply / greet / assist_offer / recruit / flavor, used only
  to steer the writer.

When the gate opens, the configured text model writes only the message body
from a tool-free prompt; it cannot start actions. A writer failure sends the
canned line `Sorry, I can't talk right now.` and counts against the same
budget. Every dispatch — writer, canned, or the task model's own `send_chat` —
is bounded by a persisted per-bot budget (`AGENT_CHAT_MIN_INTERVAL`,
`AGENT_CHAT_WINDOW`, `AGENT_CHAT_MAX_PER_WINDOW`). Decision metadata is logged
without chat contents. With `AGENT_TYPESAFE_API_KEY` unset the pipeline is
disabled and chat decisions fall back to the ordinary decision model. The task
decision resumes through the normal snapshot path after each pipeline result.

Configuration: `AGENT_TYPESAFE_ENDPOINT` (default
`https://api.typesafe.ai/v1/systemone`), `AGENT_TYPESAFE_API_KEY`,
`AGENT_TYPESAFE_MODEL` (default `jev-latest`), `AGENT_CHAT_TIMEOUT`,
`AGENT_CHAT_WORKERS`, `AGENT_CHAT_WRITER_MAX_TOKENS`, `AGENT_CHAT_JITTER`,
plus the thresholds and budgets above.

The controller requires:

- `TC9_NATS_URL`, `AGENT_STATE_REDIS_URL`, and `AGENT_SHARD_COUNT`;
- a StatefulSet hostname ending in its numeric ordinal, used as `AGENT_SHARD_ID`;
- `LLM_ENDPOINT`, `LLM_MODEL`, and optionally `LLM_API_KEY`;
- a matching `AgentBridge.SubjectPrefix` in `playerbots.conf`.

Build from this directory with `docker build -t <registry>/playerbot-agent-controller:<tag> .`.
The ToCloud9 Helm chart has an opt-in `agent_controller` StatefulSet configuration;
it is disabled by default. The core bridge and controller must use the same subject
prefix and shard count (the bridge currently uses 256 virtual shards).

The agent state is stored in a Redis key namespaced by subject prefix and bot GUID.
The game-state database is never accessed by this process.
LLM calls are event-driven and idle decisions are jittered; high-level tasks continue
as controller state machines between LLM decisions. The controller has a bounded
global model-worker pool, while Redis leases fence duplicate actor ownership during
StatefulSet resharding.
ToCloud9 enables a file-backed JetStream stream for bridge event subjects with
one-hour retention, so controller restarts can resume recent unacknowledged events.

Population allocation combines authoritative snapshots with pending and recently
completed creation reservations. Completed reservations remain counted until a
snapshot includes their results. Ordinary population and zone refills share the
configured total target; player-triggered cohorts can add characters beyond it.
Existing characters are preserved when a target is lowered.

With `AiPlayerbot.AgentBridge.Population.Enabled = 1`, bots exist only while a
live agent controller is connected. A missing or stale controller heartbeat stops
new scheduler logins, and online externally provisioned bots log out instead of
falling back to legacy autonomous AI (a 90-second heartbeat grace covers normal
controller rollouts). Real players' own characters and their master-linked bots
are unaffected.

## Decision and priority diagnostics

Controller diagnostics are key/value lines; filter on `bot=<guid>`, then use
`trace_id`, `task_id`, and `operation_id` to join the lifecycle:

- `event_received`: chat-event delivery delay, without chat contents.
- `decision_queued` → `decision_result`: trigger, current task/phase, fresh-event
  count, snapshot age, revision, chosen tool, and stale/error flags. `queue_ms`
  measures time waiting for a worker; `model_ms` includes model-slot waiting and
  the provider request. A stale result is discarded, not applied.
- `decision_deferred`: in-flight request, minimum decision gap, full model queue,
  missing snapshot, offline/inactive bridge, or unconfigured model. Repeated
  identical blockers are logged at most once per 30 seconds per bot.
- `tool_selected`, `tool_rejected`, `task_replaced`, `task_cancel_requested`:
  explain when model choices supersede an existing task or fail validation.
- `primitive_dispatched`: request/operation/task correlation. A dispatch alone
  does not prove that the native operation ran or completed.
- `task_wait` / `task_wait_cleared`: native ownership, trade, inactive bridge,
  offline state, or missing observation. `native_owned` is normal while a lower
  loop runs; an old progress age and `task_watchdog` identify a possible stall.

Native logs use `playerbots.agent`: rejected operations are warnings; accepted /
completed results and priority-block transitions are debug diagnostics. Enable
that category's debug level when investigating combat/trade/casting/movement
ownership, and correlate by `operation_id`. Block transitions are logged only
when the reason changes, not every tick.

The new traces omit raw tool arguments, personas, memory, private chat, and model
credentials. They are diagnostic evidence, not proof of gameplay success. Keep
live testing paused until explicitly authorized; reading logs does not require
starting the controller or making model requests.

## Friend-linked progression and curated travel

`AGENT_SOCIAL_PROGRESSION=true` and `AGENT_SOCIAL_REALM` (default 1) opt into
friend-linked goal policy. The charserver also needs
`PLAYERBOTS_SOCIAL_PROGRESSION=true`. Its once-per-minute bounded observation
uses human-owned friend-list entries and the live gateway directory. Friendships
remain durable in the ordinary character social table; stale presence (three
minutes) and directory failures are unknown, never inferred logouts. Unfriend
removes steering. Multiple friends use stable online selection, not proximity.
An absent directory entry after charserver restart is also unknown. Confirmed
offline presence requires an observed human gateway logout; a login clears it
before directory reconciliation. No saved database online flag is substituted.

Decision state includes `social_progression`: level pacing within two levels,
normal catch-up when behind, and preference for the friend's general region.
New solo quest/kill goals are gated while ahead; active group obligations and
existing tasks are not interrupted. Regional goals remain model-selected ordinary
activities, not automatic following or teleporting. After continuous authoritative
friends-offline evidence for `AGENT_SOCIAL_OFFLINE_GRACE` (default `10m`), new solo
XP goals are likewise gated. Unknown/stale intervals reset the confirmed-offline
window on the next observation, and group work continues normally.

`AiPlayerbot.AgentBridge.SocialProgression.Enabled = 1` opts the native scheduler
into friendship weighting. Charserver independently requests an offline-capable
manifest from each live population owner, reconstructs relationships/presence,
and returns correlated preferences without model calls. Scans are capped at 256
accounts/bots; manifests report when capped. Native preferences expire three
minutes after observation and reject replayed/wrong-manifest evidence. Preferred
accounts/characters are ordered first within existing admission limits and faction
ratios; admitted online bots may renew an expired session for five minutes while
a friend is authoritatively online. Missing/offline evidence returns to ordinary
scheduling, not a forced logout or eviction. Native scheduler execution and
restart behavior still require build/deployment and in-game verification.

`AGENT_ROAD_PREFERENCE=true` optionally loads `AGENT_ROAD_CORRIDORS_FILE`, a JSON
array of operator-verified corridors. Each has `id`, `destination`, `map_id`,
nonempty `evidence`, and `points` (arrays of three coordinates). There are at most
128 corridors and 32 points each, with segments at most 500 yards. No invented
coordinates or assumption that all walkable terrain is road are shipped.

Named destination tasks can use a same-map corridor starting within 100 yards,
at most 5,000 yards long and twice the direct geometric endpoint distance. Each
ordinary native coordinate-navigation child must report observed arrival before
the next starts. The endpoint still requires native named-destination resolution
and arrival. Failures/timeouts fall back to ordinary navigation and persist a
ten-minute corridor cooldown. Without a matching verified corridor (or with the
option off), ordinary navigation is unchanged. Curated route data and native
in-game verification remain outstanding; this is not a global road classifier.

## Cluster auction/mail authority

ToCloud9 gateways route auction and mail requests through dedicated services, not
the legacy worldserver auction/mail handlers. Native bot commands must use that
same authority. The safety guard rejects legacy `inspect_auctions`, `buy_auction`,
`bid_auction`, `collect_mail`, and `send_mail` in cluster mode (and fails closed
when a sidecar build cannot establish its mode).

The native auction/mail implementations described below are therefore not yet
usable in this cluster: their service handoff and authoritative receipts remain
unfinished. Controller planning tests and a successful C++ build do not prove
cluster economic correctness. Do not enable those flows or treat local auction
cache revalidation as proof of the live service book.

## Crafting hierarchy

`craft_item` creates a persisted dependency plan for additional output. Known
subcrafts precede the parent recipe. External materials already in inventory are
reserved once across shared dependencies; batch surplus is reused. Optional vendor
procurement requires observed offers and an explicit total copper budget, divided
into reserved child allowances. Unknown material sources are not guessed.

Gathering procurement uses eligible nearby nodes whose loot this bot actually saw
while running `gather_target`. Evidence is persisted, limited to 64 sources and 32
items per source, and expires per item after one hour. It is a candidate hint, not
a guaranteed drop table. Each node is attempted once per parent, with at most 40
attempts; partial material gains may advance to another observed node. Missing
requested gains, exhausted candidates, or uncertain children block the parent.
Observed gameobject loot also retains at most four actual locations per source.
If local candidates run out, crafting may navigate ordinarily to up to eight
untried, fresh same-map sites within 5,000 yards, then inspect for a live eligible
node. An old GUID is never assumed to have respawned. No teleport or cross-map
shortcut is used.

Each purchase or craft child must verify its native postcondition. Gathering's
native interaction result alone is insufficient: requested inventory must increase.
The parent then
requests a correlated inventory snapshot before dispatching the next child; an
unrelated observation or accepted command does not count as completion. In-flight
resource use is never automatically replayed after uncertain failure or owner loss.
No model call is needed to pace these children.

Only observed, learned recipes with one deterministic item output are supported.
Plans have bounded depth, cast count, and observation retries.
`inspect_crafting_recipes` browses 40-recipe pages using `next_offset`, or requests
an output `item_id` with bounded known subcraft dependencies. Truncated dependency
views cannot start a crafting plan. Inspections do not replace an active task and
persist the query rather than a stale spellbook cache.
Crafting can also consume one already negotiated, partner-accepted material bundle
with an explicit copper budget. It never gives away items or accepts enchant-service offers. Native acceptance checks
partner identity, offer revision, material quantity, currency, and the actual final
transfer; the parent then verifies correlated inventory. Other work still pauses
during an active trade.
Bundles may contain up to six material types/slots and 40 total items; every
material's transfer and subsequent correlated inventory must be verified.

`craft_item` with `procurement: "trade"` and an explicit request message/channel
starts a bounded ten-minute discovery child. Ordinary incoming chat triggers the
same live-state/events/current-task decision input. The LLM negotiates through
chat and uses `negotiate_crafting_trade` to bind a recently contacted, observed
seller and an agreed total price within the original budget. The parent meets
that seller without continuously following, opens their pending trade, offers
the agreed money once, and waits for a matching partner-accepted material bundle.
Each control waits for its native result; queue acceptance is not completion.
Control and transfer no-replay markers persist before dispatch. Expiry, changed
partner, wrong materials, uncertain controls, or lost transfer evidence block.
Manual trade controls cannot bypass this parent's allowance; stop it first.
Remote sellers can arrange an ordinary meeting through chat, but cannot be
navigated to until observed on the bot's map. No private seller inventory is read.

`inspect_auctions` observes a bounded public auction-book page at an observed
auctioneer (at most 512 entries examined, 40 matching listings returned).
`search_auctions` realizes up to eight item searches as native page children,
with at most 16 pages per item and 160 cached offers per item. Each offer expires
independently after two minutes; another page never refreshes old prices.
`cached_auction_items.partial` marks incomplete retained evidence. Empty pages are not proof of no
offers elsewhere. `buy_auction` reserves one total copper budget across observed
whole stacks, rechecks listing identity/price/quantity through the ordinary native
buyout handler, collects each exact won-mail attachment at an observed mailbox,
and verifies correlated inventory. The same children can fill crafting deficits.
Buyout submission is not inventory receipt or proof of asynchronous database
durability. Missing/changed offers, unavailable/delayed mail, owner loss, or uncertain
results block without repurchasing. Stack selection minimizes spending among the
bounded cached offers, not across the whole book. Different materials share one
total procurement budget. Crafting can nest a bounded multi-material search when
an observed auctioneer and mailbox are available but supply evidence is missing.

`bid_auction` places one exact bid below buyout under an explicit budget. Its
write-ahead escrow journal persists before dispatch and retains up to eight bids.
Bid placement is not item receipt. Bidder notifications trigger decisions with
the same live-state/events/current-task input as other events. Exact won-mail
attachments or outbid/cancellation refunds can be collected by
`collect_auction_proceeds`, or opportunistically at observed mailboxes while idle
(at most once per minute per bid). Native identity/quantity/refund checks and a
correlated inventory gate close the journal. Empty mail keeps the bid pending;
uncertain collection blocks without automatic rebidding.

Cross-map gathering remains outstanding; native negotiation/trade execution is
implemented locally but not yet verified in-game.
Native C++ gameplay still requires authorized
build/deployment and live verification.
