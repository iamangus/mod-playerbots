# Playerbot agent-controller

This process owns agent profiles, memories, high-level task loops, and model calls
outside the AzerothCore process. Run it as a Kubernetes StatefulSet, independently
scalable from the ToCloud9 worldservers. It consumes its assigned virtual NATS event
shards and publishes commands to the current owning worldserver using the event's
owner token.

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
