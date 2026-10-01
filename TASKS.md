# Agent task queue

Requests moved from [TODO.md](TODO.md) are tracked here. Preserve the original
request and record any clarification, progress, blockers, and verification beneath it.
Use stable IDs such as TASK-001 and statuses: queued, in progress, blocked, done.

## Active and queued

<!-- Each entry: ID/title, status, original request, then agent notes. -->

### TASK-011 — Ordered acquisition, social/travel, and live validation (in progress)

**Original request:** "Let’s do 1, then 3, then 4. We will save 2 for later"

**Follow-up:** "go" — continue the ordered work, without changing the live-test phase gate.

**Further follow-ups:** "go and dont stop till its done"; "Please continue" —
continue the same ordered scope, preserving existing state and deferred content.

**Order/scope:** (1) finish acquisition: trading discovery/negotiation and multi-material
trades, multi-page/multi-material auction search, bidding/outbid/refunds, and nonlocal
gathering; (3) player-linked social progression and opt-in road-preferred routing;
(4) review/commit, cached local build, publish/deploy, and native/live verification,
including restart/group/leadership/chat/trade behavior and the 80-bot diverse cohort.
Harder content loops (dungeons, auction selling, guilds, elite/group quests, escorts,
battlegrounds, raids) are deferred by the user, not completed or implicitly selected.

**Authorization:** This request authorizes the build/deploy/live-validation phase
listed in step 4, superseding the earlier live-test pause for that phase. Keep the
controller paused and avoid live token spending during steps 1 and 3. Do not reset
or delete existing player/bot state as part of validation.

**Current work:** Step 4 compilation/preparation after local social/scheduler and
corridor machinery, with cross-map gathering and verified road data still open. Locally
implemented bounded multi-page/multi-material
auction search, shared-budget stack planning, multi-material negotiated trade
bundles, and same-map travel to fresh actual-loot gathering sites. Added bid escrow
journals, native bid verification, event-triggered outbid/win observations, exact
won-mail/refund collection, and correlated inventory gates. Bid no-replay markers
are persisted before native dispatch; missing mail remains pending, not a loss.

**Latest verification:** Eight new bid/proceeds test functions cover escrow,
missing receipts, budget/buyout rejection, duplicate/old/wrong-owner notices,
notification-before-receipt ordering, restart serialization, won/refund collection,
delayed mail, and missing inventory. Full `go test -race -count=5 -timeout=60s ./...`
and `go vet ./...` pass. C++ codestyle, touched-file clang-format dry runs,
`git diff --check`, and the reused movement action creator lookup pass.
Native changes remain unbuilt/undeployed. No live controller/model calls occurred.

**Trade discovery/negotiation:** Added explicit `craft_item` trade procurement with
a ten-minute bounded public request and `negotiate_crafting_trade`. Recent ordinary
chat and observed same-map seller identity are required; the LLM chooses the agreed
price/message within the original allowance. The existing crafting parent realizes
meeting, seller-initiated trade opening, one exact money offer, partner-accepted
bundle validation, native atomic receipt and correlated inventory before subcrafts.
No continuous following, hidden seller inventory, unsolicited automatic rebids,
or manual trade-control bypass. Control/transfer markers persist before dispatch;
queue acceptance does not advance a control. Six regression functions cover the
successful nested chain, budget/contact/visibility rejection, deadline, manual
controls, distant meeting, one-shot uncertain pricing, restart markers, and wrong
partner/materials. Full race suite (five runs), vet, and diff whitespace pass.
No native changes, build, deployment, or live model calls in this follow-up.

**Social/travel implementation:** Added opt-in charserver affinity observations
from explicit human friend-list entries and the live gateway directory, with
bounded refresh and failure-as-unknown handling. Controller persists observations,
rejects stale/wrong-realm evidence, selects a stable online friend, exposes
level-band/regional policy to the normal state/events/task decision input, and
gates new solo quest/kill goals while ahead without interrupting group obligations.
Added opt-in operator-verified same-map corridor loading and persisted waypoint
children for named destination tasks; terminal arrivals advance locally, endpoint
arrival still uses native named-destination resolution, and failed/timed-out routes
fall back with a persisted cooldown. No curated coordinates were invented.
Five social and five corridor regression functions, full controller race tests
(five runs), controller vet, and charserver service/config/main race tests pass.

**Scheduler/offline policy:** Added opt-in native account/character preference
ordering within existing admission caps/faction ratios, and five-minute renewal
of already admitted expired sessions while a friend is freshly online. Charserver
reconstructs explicit friendships for offline bots from correlated population
manifests, including after service restart, without LLM decisions. Manifest scans
are capped at 256 accounts/bots and disclose partial coverage. Preferences have a
three-minute observation TTL, manifest correlation, and monotonic replay rejection;
missing evidence returns to ordinary scheduling, never forced logout/eviction.
Confirmed friends-offline evidence gates new solo XP goals after a configurable
ten-minute grace; unknown intervals reset the window and group work remains exempt.
Two charserver scheduling regressions cover offline-session-independent discovery,
replayed manifests, stale/unrequested evidence, and wrong realm. Service race tests,
vet, `python3 apps/codestyle/codestyle-cpp.py` (no `python` alias is installed),
touched-file clang-format checks and both repositories' diff whitespace pass.
Native scheduler changes remain unbuilt/unexercised. Curated road data, cross-map
gathering, deployment settings, and live/native validation remain outstanding.
No deployment/live model calls occurred during implementation.

**Build/deployment preparation:** Added default-off chart wiring for social
realm/offline grace and verified-corridor JSON, read-only mounts and rollout
checksums. Default and fully opted-in Helm lint pass; rendered controller env,
mounts, JSON and checksum were inspected programmatically. Kubernetes confirms
the controller remains at 0 replicas and there is no HPA. Saved values still request
one replica, so a blanket chart upgrade would resume it: preserve the pause until
native verification is ready. Started authorized cached rootful Podman builds for
the custom core, controller, and charserver under `local-acquisition-social` tags.
Updated the local core build recipe to copy all module sources/headers/config,
not just earlier movement/runtime files. Logs are under
`/tmp/opencode/acquisition-social-{core,controller,charserver}-build.log`.
Controller and charserver image builds succeeded (local image IDs
`90769338fd9f` and `2e523aee8e7a`, not registry digests). The first custom-core
build failed because fishing used a const `WorldPosition` with the core's non-const
`IsValid()`. Corrected the local variable without changing core APIs; formatting,
codestyle and diff whitespace pass. The cached retry succeeded, logged at
`/tmp/opencode/acquisition-social-core-build-retry.log`; custom-core worldserver
compilation/link/install passed (local final image ID `184cb04bca81`). Final
controller race tests (five runs), full charserver race tests, both Go vets,
C++ codestyle and diff whitespace pass. Committed module/controller work as
`e84917c1`; pre-existing `azerothcore-wotlk/` was excluded. Published the first
three images by digest: core `sha256:4ca9981896082266430fb1d23a276f1528eb5f2387aaabfc8c8f45c2d889dc11`,
controller `sha256:2eab478e6d86ba7e93b32f9a05847b01539e1115e1ee67c64fa4ef55183e651a`,
charserver `sha256:ef07b8136e48f66a5a16de2dfe8cc74b1e921882d80fec43d3c66b92c634ec2b`.
Native gameplay and deployment remain unverified/pending; no state resets or
live model requests occurred.

**Pre-deployment review finding:** Charserver's in-memory directory is empty
after restart; absence alone cannot prove a human logout. Added explicit
`presence_known`, bounded human gateway logout tracking, and login clearing;
unobserved presence stays unknown and cannot start offline XP gating. Added a
restart/logout/login regression and controller unknown-presence checks. Full
controller race suite (five runs), full charserver race tests and both vets pass.
Controller/charserver images were rebuilt/published with this correction; do not
use their first digests above. Core image was unchanged at that point.
Existing core/charserver deployments use rolling surge despite in-memory bot
ownership. Added opt-in `Recreate` chart strategy to avoid overlapping schedulers
and social listeners when playerbot integration is enabled; Helm lint passes.
Unrelated ToCloud9/Kubernetes working changes remain local and must be preserved.

**Deployment/restart validation:** Committed presence correction as `fdfc2792`.
Deployed corrected charserver
`sha256:fd07a6d66adaff8141140a23bc4ab2cd1acda1f06bbfcbcb91c8ad27f582b3b8`,
staged controller
`sha256:77db032970e02507c5422551ab42d3fb26f8aac263c7c3734c9cc90d26096a3f`
at zero replicas, and deployed the core digest above. Enabled opt-in social
settings, preserving the remaining configuration. Charserver and core `Recreate`
rollouts completed; both pods were ready with zero restarts at inspection.
Kubernetes MCP cannot patch Secrets; used the authorized user kubeconfig for
minimal patches. No character/group/Redis cleanup or LLM requests occurred.
Observed two native population heartbeats and one request-correlated realm-1
friendship manifest: 256 bots, explicitly partial. This proves that protocol path,
not all affinities, native services, or 80-bot progression.

**Restart finding:** External mode skips the legacy factory that initializes
`randomBotAccounts`, while `IsRandomBot` still requires that list. Existing type-1
assignments populate admission lists but not account classification. Added a
startup-only external-mode rehydration from existing type-1 assignments before
map threads/logins, without creating/randomizing characters or changing saved
state. Native recompilation/redeployment and live causality verification are
pending. No registered bot social sessions were seen in the inspected log tail;
this is a symptom, not proof that this is the only restart problem.

**Saved settings:** Updated saved image digests and matching social/population
limits, and added an explicit chart `paused` switch preserving shard count.
Rendered paused workload stays at zero replicas with shard count one. The saved
Terraform release is still pinned to chart 0.2.4, which ignores the new switch;
documented that applying that old chart would resume the controller. Updated
chart publication/pinning remains outstanding; no blanket Helm/Tofu apply occurred.

**Economic authority blocker:** Verified gateway auction requests call
`auctionHouseServiceClient`, and mail requests call `mailServiceClient`, whereas
new native bot operations call legacy core handlers/caches. That bypasses the
cluster service authority and cannot safely be validated by local cache receipts.
Added a fail-closed runtime guard rejecting legacy auction inspection/buy/bid and
mail collection/sending in cluster mode (unknown sidecar mode also fails closed).
The authoritative service handoff/receipt integration must be completed before
those flows run in this cluster. Keep the controller paused; do not spend live
money or claim acquisition finished. Standalone algorithm tests remain useful
but do not prove ToCloud9 economic correctness.
Startup account restoration was committed as `a58b8cb7` and its native build
passed, but was not deployed: the next image must also contain this authority
guard and first-start-only restoration (to avoid changing account vectors on
config reload). The combined safe image compiled and published at
`sha256:14c9f3ebff979aaaa5e96691b914bf9b732b8f967518c6ce04720a6e89ac2739`;
its non-overlapping core rollout succeeded. Saved core digest matches. Observed
five native bot events (three distinct bot identities, fresh owner epochs,
online/heartbeat) after restart while the controller stayed paused. A read-only
database check found 36 type-1 accounts and 360 existing bot characters, levels
1–6; the saved `online` count is not treated as authoritative presence or proof
of the requested 80-bot cohort.

**Passive social snapshot finding:** Runtime discarded the command inbox without
controller ownership, including charserver's read-only reconstruction snapshots.
Added bounded snapshot-only handling while inactive (eight queued commands per
tick, one snapshot per bot per five seconds), retaining normal owner/epoch/deadline
validation and leaving every task/mutation command disabled. Formatting/codestyle
pass; cached compilation and publication passed at
`sha256:6731ab7a1df0c6cb9dc02a986c315ef66ebd3a6449534c50ff6317d1c595d2ae`.
Core rollout completed; saved image digest matches. Observed twenty native
online/heartbeat events from ten distinct bots, but no reconstruction snapshots
in that first bounded sample. Later inspection found 27 registered social-session
identities in charserver logs. A 50-event sample contains session-ready signals
from 22 distinct bots and an owner/request-correlated read-only native snapshot
for Ahosera (level 1). No controller heartbeat/model call was sent; controller
remains at zero replicas. This establishes passive reconstruction, not full
restart/group consistency or the 80-bot cohort. Core/charserver remain ready.
Committed passive snapshots as `812044cf`; module commits through it are pushed
to the iamangus fork. An attempted push to upstream was permission-denied; no
upstream changes occurred.

**Pause-aware chart publication:** Committed scoped ToCloud9 social/chart work as
`5f2dc6c`, preserving unrelated local Dockerfile/build notes. Published chart-v0.2.5
through release run `36894939696`; the public Helm index lists version 0.2.5.
Updated saved Terraform chart pin to 0.2.5. Rendering the published package with
saved values confirms zero controller replicas. Local charserver race tests/vet,
chart lint and the paused social/road render regression pass. No blanket Helm/Tofu
apply was performed, and no LLM requests or state cleanup occurred.

**Service transaction trace:** Existing gateway auction bidding mutates the
service before a separate native gold-debit RPC. Mail money is removed before a
gold-credit RPC; attachments are added natively before service removal. These
crash windows mean a simple socketless wrapper cannot provide safe exactly-once
effects. The bot handoff needs durable service receipts and idempotent native
mutation/reconciliation; no human gateway behavior was modified. Details are in
the local cluster-economy bridge analysis. Economic flows remain blocked.

**Partial social evidence correction:** Bounded friend projection previously
selected the first eight GUIDs, which could omit an online ninth friend and
mistake partial offline evidence for everyone offline. Added online-first stable
ordering, explicit projection `partial`, and conservative unknown policy when
no online friend is observed in a partial list. Partial evidence clears offline
grace rather than accumulating it. Added charserver projection and controller
offline-proof regressions; both full race suites and vets pass. Corrected a
helper's error return to retain nil evidence on directory failure. Go image builds
started under `local-partial-social-presence`, with logs under
`/tmp/opencode/partial-social-{controller,charserver}-build.log`.
Committed/pushed module correction as `d26e4198` and ToCloud9 correction as
`6610d4c`. Both Go image builds and publication passed. Staged controller
`sha256:2491832a9c3d79cda302564ec7fa5eba5cee7cf89dfa4280437f5cc62176ea8b`
at zero replicas and deployed charserver
`sha256:381a307c05fa9c277ab26ec763f30fe53104167095c668f44627c6a0efef59ac`;
charserver rollout completed. Saved image pins match. Native image is unchanged.
Existing economic blockers, cross-map gathering, and verified road
traces remain unfinished.

**User directive — no bots without the controller:** The user observed autonomous
bot activity while the model controller was paused and asked that no bots exist
without it ("I basically want there to be no bots if the controller isnt there to
manage them"), and does not want the legacy-AI fallback kept around. Implemented
(required-controller behavior, not the separate legacy-tree deletion): a 90-second
controller-heartbeat grace now governs externally provisioned populations. Without
a recent heartbeat, `RandomPlayerbotMgr` holds all scheduler logins, and each
online externally provisioned bot (`agentBridgePopulationEnabled` + configured +
random-bot) logs itself out via the established map-thread logout path instead of
restoring legacy strategies. Removed `RestoreAutonomousStrategies` and its
tracking state entirely; controller loss now clears the operation, stops
movement, publishes `agent_control_inactive`/`bot_offline`, and logs out on the
bot's next tick. Added `AgentRuntime::IsControllerPresent()` from an O(1) global
heartbeat stamp. Real players' characters and master-linked bots are unaffected;
legacy AI still serves non-population deployments (self bots, local setups).
Documented in `playerbots.conf.dist` and the controller README. Formatting,
codestyle, and diff whitespace pass; first cached native build hit one remaining
`suppressedLegacyStrategies` use in the group-changed handler; removed that
tracking call site (keep the existing `-follow` removal) and rebuilt. The
required-controller image was published as
`sha256:18e4395d53b5f0e99894d661dbfe18fd4026bf2a9c71b0d4c626f2e180a1fc5f`,
deployed, and its `Recreate` rollout completed. Post-rollout observation: zero
bots online in the characters database and no relogin events while the controller
stayed paused (the only captured events were in-flight `social_affinity` refreshes
for pre-restart sessions). Saved core digest matches. Commit/push pending.
Physical deletion of the legacy AI tree is queued as TASK-012, not mixed into
this safety change.

**User directive — mandatory clean reset on every container deployment:** The
user asked for the reset script's location/details to be documented where agents
read and required it after every container deployment. Documented in
`.agents/docs/deployment.md` (new; routed from AGENTS.md mandatory reading and
agent rules) and mirrored in `/home/angoo/repos/k8s/AGENTS.md` deployment
standards. Ran `clean-playerbot-state.sh` for the just-finished required-controller
deployment (log: `/tmp/opencode/clean-reset-controller-required.log`); verification
below. Going forward, every container deployment ends with this script.

**User directive — resume the controller:** The user asked to bring the controller
up. Scaled the staged controller (`2491832a…`, still latest) to one replica; pod
ready; saved values flipped to `paused: false`. The clean-reset baseline was just
established (zero bots, zero groups, purged stream). Live observation then found a
bootstrap deadlock in the required-controller design: the controller only
registers a worldserver owner for heartbeats when it sees bot events, and the
worldserver only subscribes the bot command subject from per-bot updates — with
zero bots online after the reset, no heartbeat can ever flow and no bot can ever
log in. Confirmed live: no controller_heartbeat on the owner command subject and
zero logins for minutes.

**Bootstrap fix:** (1) Controller: `population_owner_online` now registers the
announcing owner across every owned shard (`noteOwnedShards`), so heartbeats flow
before any bot is online; stale announcements stay unregistered and the expired-
stamp fallback now selects a freshly registered live owner. (2) Native: the
population bridge subscribes the bot command subject on the world thread
(`AgentRuntime::EnsureBotCommandSubscription`, idempotent, shared with the
per-bot refresh) so heartbeats are received with zero bots online. Updated the
owner-discovery regression to assert all-shard registration, stale rejection, and
the registration fallback. Controller race tests (three runs) and vet pass;
formatting/codestyle/diff whitespace pass. Controller (`sha256:c29e8ecce29e90e82133da1f9a81ff05ed330da8e1414b1c71dc8740e8a81204`)
and core (`sha256:98a50028807d44d5da1529a1e1342fa1a4186a4251eea1420560d68059f1a5ee`)
images were published, deployed, and the mandatory clean reset ran (log:
`/tmp/opencode/clean-reset-bootstrap-gate.log`; all four rollouts completed).
Saved digests match. Committed as `ee383db0` and pushed.

**Live bootstrap verified:** After the reset, the required-controller chain now
works end-to-end: controller heartbeats/commands flow on the owner command
subject, bots log in without any manual kickstart, and the LLM drives real
activity — observed `send_chat` introductions with team-up offers in say,
`approach_target` combat approaches, `navigate_to_quest_giver` (quest 400), and
request-correlated snapshots. Within minutes the online population reached 88
bots (levels 1–6) toward the cohort target; 39 tool-selected decisions in the
last six minutes; zero panics, model-unconfigured, or decision-failure lines.
This proves bootstrap, the login gate under an active controller, and live model
operation on the freshly reset state. It does not yet prove acquisition/social/
corridor outcomes or the full 80-bot stable cohort.

**User directive — wipe and rebuild the bot cohort:** The user corrected that
Ollos (60169) and Lotiner (60249) are bots, not their characters — earlier session
notes wrongly listed them alongside Peepee; corrected here. DB evidence confirmed
TASK-005's skew: classes present are only Paladin 192, Rogue 127, Priest 24,
Shaman 17 — Human/Dwarf/Draenei/Blood-Elf all mapped to Paladin and
Night-Elf/Orc/Gnome/Troll to Rogue by the pre-fix allocation. The joint
race/class fix only affects new characters, and with 360 existing characters the
population is over target, so no new creations can ever diversify the visible
cohort. The user authorized deleting the entire bot cohort (level 1–6 test
characters) and letting the controller rebuild it diversely.

**Controller-loss logout crash:** Scaling the controller to zero for the wipe
exposed a segfault (exit 139 at 17:59:20) in the required-controller logout path:
it called the instant-logout branch (synchronous session/player deletion) from
the map thread, where the caller still holds live pointers into destroyed
objects. Every pre-existing logout path runs on the world thread or early-returns
for already-logging-out sessions. Fixed by adding `LogoutPlayerBotOperation`
(world-thread queue) and a once-only `AgentRuntime::IsStopped()` marker; the map
thread now only stops the agent and queues the logout. Formatting/codestyle pass;
core image building under `local-safe-logout`. Also noted (not fixed here):
the pre-existing whisper-logout chat command path calls the same instant logout
from a map thread and carries the same latent crash risk.

**Movement stall diagnosis (in progress):** The user reported diversity fixed but
bots stationary and chatting in circles. Confirmed from controller logs: every
`accept_quest` task ends blocked with `primitive repeatedly failed: quest
destination unavailable` — the native `navigate_to_quest_giver` rejects, the
controller retries/blocks/re-plans, so bots never move and the LLM falls back to
chat. The rejection comes from `FindQuestDestination` finding nothing: the quest
travel table that backs it is loaded by `LoadQuestTravelTable()`, which our
branch moved to an `OnStartup` hook (4ae3370a) that fires only after the world is
ready and bots can log in — a startup race; upstream PR #1462 had removed the
original config-init call site entirely. Diagnosis was slowed by the deployed
logger config suppressing all module INFO logs (`Logger.root=2`, no
`Logger.playerbots`), making log-absence non-evidence.

**Root cause found — dead strategy gate broke ALL quest navigation:** Every
`accept_quest` task failed with `quest destination unavailable` because
`QuestRelationTravelDestination::isActive` (TravelMgr.cpp:1147) required a
`"rpg quest"` strategy that does not exist anywhere in this codebase — no
creator registration, no `addStrategy` call (introduced by upstream commit
0008d84f, likely ported from a fork where the strategy existed). Since no bot
can ever have it, every quest-giver/taker destination was permanently inactive:
agent quest navigation AND legacy travel-questing both structurally broken.
`QuestObjectiveTravelDestination::isActive` has no such check, which is why kill
tasks worked while quest-giver navigation failed. The travel table load race
(fixed earlier in this session) was a second, separate defect that had hidden
this one. Verified `getDialogStatus` (the remaining availability gate) is not
distance-based, so solo agent bots pass level/availability checks. Removed the
dead check (kept `context`, used by AI_VALUE macros). Legacy quest-travel
behavior resumes its pre-0008d84f semantics; remaining `isActive` guards
(level, map, can-take, group values) are unchanged. Formatting/codestyle pass;
core image building under `local-quest-destinations`.

**Solo quest-navigation fix:** With the dead strategy gate removed, quest
acceptance works for some bots but most still block — the remaining gate is the
legacy group coupling inside `QuestRelationTravelDestination::isActive`
("can fight equal", "following party" values): solo agent bots fail it until
they have fought something, explaining the bot-dependent mixed outcomes. Only
the giver path filters on `isActive` (`getQuestTravelDestinations` passes
`ignoreInactive=true`, and the turn-in/objective branches never filter).
Replaced the giver-path filter in the agent's `FindQuestDestination` with the
same map-match and quest-level rules applied directly; level and availability
are validated again by the accept primitive (CanTakeQuest/quest log) before any
quest is taken. Legacy paths are unchanged. Formatting/codestyle pass; core
image building under `local-solo-quest-navigation`. Earlier observed in this
window: dead bots handled with Spirit-Healer resurrection navigation (working
behavior); travel table load confirmed at 6778 ms during world init.

**Next:** Deploy, mandatory reset, verify all solo bots accept quests and
progress through objectives; then resume TASK-011 validation.

### TASK-012 — Remove legacy autonomous AI tree (queued)

**Original request:** "I dont want to keep all that old code around. I basically
want there to be no bots if the controller isnt there to manage them."

**Scope notes:** The behavioral guarantee (no bots without a live controller; no
legacy-AI fallback for provisioned bots) is implemented under TASK-011. This
follow-up is the physical cleanup: delete the class/dungeon/raid/world strategy
tree and unused legacy triggers/values/actions that externally provisioned bots
never run, while keeping the engine, shared primitives (`Agent*Action`), factory,
and whatever the agent runtime and local/self-bot deployments still need. Requires
a full native build plus grep verification that no registered creator/strategy
consumers remain orphaned per `.agents/docs/ai-engine.md`.

### TASK-009 — Profession crafting and material dependency loops (in progress)

**Request:** Continue TASK-001, ranked item 8.

**Follow-up:** "do it" — continue trading/AH procurement locally; this was not
treated as authorization to resume the paused controller or live token spending.

**Progress:** Added a local dependency planner in `agent-controller/crafting_plan.go`.
It orders subcrafts before parent casts, consumes shared material stock only once,
retains batch surplus, reports external material deficits, and deterministically
selects recipes. Goals, total casts, dependency depth, cycles, zero-net-output
cycles with seed inventory, and quantity overflow are bounded/rejected. It returns
no partial plan after a validation failure and never mutates input stock.

**Observations:** Core snapshots now publish up to 40 sorted, known trade recipes
with single deterministic item output and reagent quantities. Random/level-scaled
outputs are omitted rather than presented as guaranteed. The controller retains
these observations for both model input and local planning. A separate bounded
`crafting_inventory` section includes every published recipe output/reagent,
including zero counts, using the existing full inventory-count map. Requested
crafting observations explicitly focus the parent's output too. This prevents the
generic 40-item inventory display limit from hiding materials or final output.

**Execution:** Added `craft_item` and native `craft_once`. The persisted parent
executes the bounded dependency plan in subcraft-first order. The native child
revalidates learned recipe/output and real reagent quantities, casts ordinarily,
waits for completion, and verifies inventory gain. Between children the parent
requires a request-correlated fresh snapshot, with bounded observation retries;
unrelated snapshots and duplicate results cannot release the gate. Uncertain
casts, owner loss, and watchdog failures block instead of replaying resource use.
Copied decision tasks do not alias dependency steps.

**Procurement:** External deficits offered by observed nearby vendors can now be
filled by persisted `buy_vendor_item` children before casting. Requires an explicit
total `max_spend_copper`; each child receives a reserved allowance, and the sum
never exceeds the total. Bundles are rounded correctly, batches are bounded,
vendor choice is deterministic, and inventory is observed between purchases/casts.
Unknown/unaffordable sources reject the plan without displacing current work.
Partial or uncertain purchases block, never automatically replay.

**Verification:** Six planner/observation and nine execution/procurement test functions pass in
`go test -race -count=5 -timeout=60s ./...`; vet, C++ codestyle, clang-format dry run,
and `git diff --check` pass. An initial protocol-stub test omitted schema/owner
metadata and was rejected by the real event guards; corrected it before the passing
suite. No C++ build, deployment, live gameplay, or LLM calls were performed.

**Recipe inspection:** Added 40-recipe pages with total/next-offset metadata and a
focused output query that traverses bounded known subcraft dependencies. Truncation
is explicit and rejects crafting plans. `inspect_crafting_recipes` persists the
query, refreshes learned recipes natively, and does not replace the active task or
release its private observation gate. Six controller test functions cover inspection.

**Gathering procurement:** Crafting can now nest `gather_target` children for eligible
nearby nodes whose loot this bot actually observed. The packet hook preserves native
loot handling and only reads an atomic operation target; outbound events are mutex
protected. Source knowledge is bounded (64 sources, 32 items each, 12 per event),
persisted, and expires per item after one hour without refreshing unrelated drops.
Requested material inventory must increase in a correlated snapshot before another
node or cast runs. Partial gains use untried nodes; no gain, exhausted candidates,
40-attempt limit, uncertainty, and owner loss block rather than replay. Mixed vendor
and gathering children retain the same reserved total spending budget. Correlated
inventory is retained separately through combat waits and restarts, so a later
unrelated snapshot cannot change the verified postcondition. Ten gathering-crafting
and six source-evidence test functions pass, in addition to existing tests.

**Latest checks:** `go test -race -count=5 -timeout=60s ./...` and `go vet ./...`
pass after adding pagination/gathering and combat-wait regressions. C++ codestyle,
clang-format dry runs, and `git diff --check` pass; the reused `open loot` action
resolves in `ActionContext.h`. Native changes remain unbuilt and undeployed; live
gameplay and token spending remain paused.

**Trade/AH procurement:** Added world-thread native handoffs and persisted crafting
children for an already negotiated, partner-accepted single-material trade and for
fresh observed auction buyouts. Trade acceptance rechecks partner/revision/quantity,
explicit copper allowance, no outgoing items or enchant services, and the actual
inventory/money transfer. Ordinary work still pauses during trades. Auction children
revalidate the exact listing item identity, quantity, and price; use the native bid
handler at buyout price; require a correlated won-mail receipt; collect only the
exact attachment; and require correlated inventory before casting. Mixed sources
share the same reserved total copper budget. Owner loss, changed offers, missing
receipts, unavailable mail, or uncertainty block without replaying economic effects.

**Latest procurement checks:** Nine auction and seven material-trade test functions
pass in `go test -race -count=5 -timeout=60s ./...`, alongside all existing tests.
Vet, C++ codestyle, clang-format dry runs, and `git diff --check` pass. No build,
deployment, native gameplay, or live model requests were performed.

**Acquisition extension:** Multi-material negotiated trades now support up to six
types/slots and 40 total items, validating each transfer and correlated inventory.
Auction search nests at most 16 pages per item for eight items, caches at most 160
offers per item with independent expiry, and marks partial evidence. Crafting may
nest that search when an auctioneer/mailbox is observed. A bounded stack planner
minimizes spend across retained offers; different materials share one total budget.
Gathering retains four actual source sites per entry and can nest ordinary same-map
navigation to at most eight fresh untried sites within 5,000 yards. Arrival requires
a fresh live eligible node, not an assumed respawn of an old GUID. Six search,
four gathering-site, and an additional multi-material trade regression pass.

**Trade discovery/negotiation extension:** Explicit public discovery and LLM-agreed
seller/price selection now nest meeting/opening/pricing/acceptance/inventory children
inside crafting; see TASK-011 for limits and actual controller verification.

**Remaining:** Cross-map gathering and native acquisition/negotiation verification.
Unknown sources must still be acquired through ordinary tools first. Only already
observed evidence is used, and native execution still needs authorized verification.
Snapshot recipe discovery sorts known recipes per observation, only for enabled
agent bots; no new per-tick trigger scan or map-thread database query was added.

### TASK-010 — Auction-house buying loops (in progress)

**Request:** Continue TASK-001, ranked item 9; follow-up "do it".

**Local outcome:** `inspect_auctions` traverses a bounded public-book page at an
observed auctioneer (512 examined entries, at most 40 matching buyout offers), with
explicit cursor/more metadata, correlated responses, a two-minute age limit, and
owner/map guards. `buy_auction` persists bounded buyout/mail/inventory children under
one total explicit budget. Whole stacks may exceed the requested quantity; only
the observed page is considered, and planning is conservative rather than globally
price-optimal. The native purchase uses ordinary auction handlers on the world
thread. Submission is not inventory receipt or proof of asynchronous DB durability.

**Extension:** Broader bounded search/page aggregation and bid/outbid/refund loops
are implemented locally. An eight-bid persisted escrow journal prevents automatic
rebidding; native submission and mail collection use ordinary world-thread handlers.
Proceeds match exact auction subject/item/count and native inventory/refund changes.
Won items additionally require correlated inventory. Opportunistic idle mailbox
checks are limited to once per minute per bid; empty mail retains pending escrow.

**Remaining:** Native gameplay verification. Tests/checks are recorded under TASK-011;
live testing/token spending remain paused until its step 4. Neither entry is marked complete.

### TASK-005 — Human bot class diversity (in progress)

**Original request:** "also every human bot was Paladin. please figure out the diversity thing"

**Investigation:** Starter-zone allocation selects classes using realm-wide class
counts, not the distribution within the selected race. A globally underrepresented
class can therefore monopolize a race-specific cohort. Preserve race/class joint
counts already supplied by the population snapshot and use them for zone-restricted
class allocation. Verify with a deliberately skewed realm regression case; this is
not yet proof of the live population's complete causal history.

**Local outcome:** Joint race/class counts are retained through snapshots and
pending reservations; starter cohorts use conditional class weights rather than
global deficits. Three regression tests cover skewed human cohorts, weighted class
selection, and aggregation across level bands. Already-created paladins are not
changed. Live causal confirmation and new-cohort observation remain paused.

### TASK-006 — Vendor and trainer loops (in progress)

**Request:** Continue TASK-001, ranked items 3 and 4.

**Scope:** Bounded native NPC service loops, explicit spending limits, live
eligibility checks, and verified postconditions. Reuse movement/inventory/native
trainer APIs rather than replacing the AI engine or adding a privileged shortcut.

**Local outcome:** Added vendor purchase/repair/sell-junk and class-training tools,
service flags, bounded vendor offers, and compatible-trainer observations. Native
loops verify postconditions and spending; uncertain owner loss/timeouts never replay
economic effects automatically. Repair covers equipped items; junk selling preserves
quest items and utility tools. Controller tests pass; C++ has not been built or run.

### TASK-007 — Player-linked social progression cohorts (queued)

**Original request:** "we will also want look into player based social bot steering. when a player makes a character and they team up with or interact bots, i want those bots to follow said player progression wise. or maybe it should use the friend feature. but basically when the player is online the bots are online. they remain around the same level, and try to be in the same general area. but.. like.. not following the player."

**Agent notes:** Investigate persistent, consensual social affinity (possibly friends)
and use it for online scheduling, level-band pacing, and regional goals, not continuous
following or synthetic XP. Account for multiple human friends, logout/restart state,
and what bots do while their associated players are offline. Do not force-bind every
casual chat partner or replace ordinary party behavior.

**Investigation result:** `.agents/plans/social-progression/social-progression.ANALYSIS.md`
proposes durable opt-in friendship affinities, authoritative presence reconciliation,
scheduler weighting, level-band goals, and independent regional activities. Includes
multi-human arbitration and logout/restart safeguards. Scheduling/affinity code is
not implemented; this entry remains open for implementation and validation.

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
   (observed corpse's ordinary loot is exhausted). Implemented as a bounded batch, not an endless scan.
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
- Item 2 (loot batch) implemented locally; see TASK-003 under Completed.
- Items 3–4 implemented locally; see TASK-006 (C++ build/gameplay verification pending).
- Item 5: `fish_count` runs bounded native cast/bite/reel/loot children. Requires a
  learned skill and equipped pole; no synthetic gear or skills. Completion checks
  inventory gain of items from the actual native fishing loot source, not any item
  gain or a bobber click. Five controller tests pass; native gameplay is unverified.
- Item 6: safe `collect_mail` implemented locally with world-thread native handlers,
  delivered/non-COD filtering, per-transfer evidence, and bounded batches. `send_mail`
  now submits one ordinary non-COD message, optional money and one observed whole
  stack, with explicit gift-plus-postage budget and native submission evidence (not
  recipient delivery). In-flight duplicate sends are rejected; uncertain sends are
  not automatically replayed. Two additional mail-send test functions cover unsafe
  input, budget, observed attachment, and duplicate suppression. Legacy check-mail
  deletion/return behavior is not used. Native gameplay remains unverified.
- Item 7: flight discovery and known direct taxi routes implemented locally, with
  normal fare limits and arrival verification. No arbitrary unknown-node unlocking.
- Service controller tests cover dispatch, unsafe input, observations, owner loss,
  uncertain economic failure, and immutable copied arguments. `go test -race
  -count=5 -timeout=60s ./...`, `go vet ./...`, C++ codestyle, clang-format dry run,
  creator-table checks, and `git diff --check` pass. No live tests or LLM calls.
- Item 8: local recipe inspection, dependency/cast, vendor, observed gathering,
  negotiated trade, and observed auction/mail procurement children are recorded
  under TASK-009. Broader acquisition/search and native verification remain open.
- Item 9: bounded auction inspection/buyout/mail receipt implemented locally; see
  TASK-010. Bidding and broader search remain unfinished.
- Profession crafting note: Legacy `SetCraftAction`
  requires a master and only configures reagent-tracking data; it is not a complete
  autonomous dependency/cast loop. Do not expose that action as if it were one.
- Controller intentionally left scaled to 0 replicas; core changes go live only
  after a user-authorized build/deploy.

## Completed

### TASK-008 — Decision and priority diagnostics (done — local, not deployed)

**Original request:** "please make sure we have good logging so we know when bots are facing decisioning or priority issues."

**Outcome:** Correlation IDs join queued decisions to results; traces include event
delivery delay, queue/model time, revisions/stale results, selected/rejected tools,
task replacement, native dispatch, waiting blockers, and watchdog failures. Repeated
waits/deferred decisions are rate-limited. Native `playerbots.agent` debug traces
record priority-block transitions; rejections are warnings. New traces omit raw
arguments, prompts, private chat, memory, and credentials.

**Verification:** Six diagnostic test functions cover privacy, rate limits, waiting
transitions, clock handling, full state/events/task queue input, and stale results.
Full controller race suite repeated five times and vet pass; C++ codestyle and
clang-format pass. Diagnostic names/correlation and interpretation are documented
in `agent-controller/README.md`. Live log usefulness/volume is not yet verified. One initial test
run timed out because its stub model configuration was missing; fixed the test
setup and added bounded waits before the successful repeated suite.

### TASK-004 — More human-like path selection (done — investigation only)

**Original request:** "think really hard about how we can make pathing more human like. a bot will take a pretty silly route through mountains if it's at all walkable, while a normal player would mostly take paths. be creative with the solutions here."

**Outcome:** `.agents/plans/human-like-pathing/human-like-pathing.ANALYSIS.md`
compares curated road corridors, cached terrain costs, an aggregated route atlas,
and road-aware extraction. Recommends an opt-in outdoor corridor overlay first,
with native segment navigation, bounded detours, and safe off-road fallback.
Verified graph costs and current steep/water mesh filtering; those filters do not
identify ordinary roads. No pathing behavior changed or navmesh assets rebuilt.
Live route capture and evaluation remain prerequisites for an implementation claim.

### TASK-003 — Loot-all batch loop (done — implemented locally, not deployed)

**Request:** Continue TASK-001's ranked backlog, item 2.

**Scope:** Process a bounded batch of observed, lootable nearby corpses using the
existing `loot_target` primitive. Freeze the batch at task creation, retain its
remaining GUIDs across persistence, and advance only on matching terminal results.
Use a corpse-only mode so looting does not silently become skinning/gathering.
No wandering, no repeated model calls between corpses, no live deployment.

**Outcome:** Added `loot_nearby`, a nearest-first batch of at most 12 observed
corpses. The core publishes `nearby_corpses` from the existing cached nearest-corpses
value. The controller freezes and persists the GUID queue, advances on matching
terminal results, ignores duplicates, bounds failures, and pauses between children
for combat/trade. The existing `loot_target` primitive has an opt-in `corpse_only`
mode; exhaustion, not disappearance or an accepted command, confirms processing.
Counts describe processed corpses, not guaranteed transferred items.

Also fixed `navigate_to_quest_giver` missing from the core travel-update branch
introduced by the previous quest-acceptance work.

**Verification:** 12 new local test functions cover sequencing, validation,
deduplication, bounded batches/retries, cancellation, snapshot round trips,
queue cloning/persistence, combat/trade pauses, death/map changes, and uncertain
completion. `go test -race -count=5 ./...`, `go vet ./...`, C++ codestyle,
clang-format dry run, creator-table checks, and `git diff --check` passed.
No C++ build, deployment, live gameplay verification, or LLM requests performed.

### TASK-002 — Fix nearby-player `in_group` retention (done)

**Original request (context):** "They dont seem to see who they are in a group with" and the
group-confusion reports; the LLM cannot see which nearby characters are already grouped because
the controller dropped the `in_group` field during snapshot decode.

**Outcome:** `playerInfo` now decodes `in_group`; regression test
`TestSnapshotRetainsNearbyPlayerInGroup` added. Shipped in commit c9106418.
