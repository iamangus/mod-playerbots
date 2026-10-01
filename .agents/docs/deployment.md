# ToCloud9 container deployment

How the built images reach the live cluster, and the mandatory reset that closes
every deployment.

## Images

- Core (worldserver with this module): built from the module tree against
  `mod-playerbots/azerothcore-wotlk` using rootful Podman with the cached
  `playerbots-core-build` image and persistent ccache. The Dockerfile used for
  incremental builds is `/tmp/opencode/loop-hierarchy.Dockerfile` (copy module
  `src/` + `conf/`, build, install, ship `worldserver`).
- Go services (agent-controller, charserver) build from their own Dockerfiles in
  `agent-controller/` and `ToCloud9/apps/charserver/`.
- Push with `--digestfile` and deploy **by digest**, never a floating tag.

## Mandatory post-deploy reset

Owner directive (2026-10-01): **always** run the clean reset after deploying a
change to any ToCloud9 container (worldserver, charserver, controller, group
service, or any other). Do not consider a deployment finished until it ran.

Script: `/home/angoo/repos/k8s/games/modules/tocloud9/apps/clean-playerbot-state.sh`
(run as `NS=gs-tocloud9 ./clean-playerbot-state.sh`). It performs, in order:

1. Truncates authoritative group state in `acore_characters`
   (`groups`, `group_member`, `group_invites`). Real players' group memberships
   are wiped; they must log out to character select and back in.
2. Deletes agent runtime state in Redis under `playerbot-agent:playerbots.v1:*`
   (tasks, leases, cohorts, reservations, rate keys). It **keeps** `bot:` creation
   records so the population manager does not lose track of created characters.
3. Purges the JetStream stream `PBA_PLAYERBOTS_V1` (one-hour bridge event
   history is intentionally not preserved across deployments).
4. `kubectl rollout restart` + `rollout status` for groupserver, charserver, the
   agent-controller StatefulSet, and gameserver-ac. Restarting the controller is
   a no-op while it is scaled to zero, so a paused controller stays paused.

It deliberately keeps the auth/characters databases (players, bot characters,
guilds) and the Redis bot creation records.

## Deployment order

1. Build/push the changed images (digest-pinned).
2. Patch or upgrade the affected workloads with the new digests (worldserver and
   charserver use `Recreate` strategy when playerbot integration is enabled).
3. Run the reset script above; wait for all four rollouts it reports.
4. Verify: group tables empty, Redis keys pruned (bot: records present),
   JetStream purged, expected pod images, and no unexpected bot logins while the
   controller is paused.

The saved deployment values live in
`/home/angoo/repos/k8s/games/modules/tocloud9/values.yml` with the chart pin in
`helm.tf` in the same directory. That directory is not a git checkout; treat the
files there as the live deployment record.