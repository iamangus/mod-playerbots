/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTPOPULATION_H
#define PLAYERBOTS_AGENTPOPULATION_H

#include <cstdint>

#include "Common.h"

class Player;

// World-thread bridge for external bot population management. Handles the
// create_bot_character and population_snapshot commands from the external
// controller, publishes population-level events (created characters, real
// player first entry), and registers created accounts with the random-bot
// login loop. Requires the agent bridge and libsidecar integration.
class AgentPopulation
{
public:
    static AgentPopulation* instance();

    AgentPopulation(AgentPopulation const&) = delete;
    AgentPopulation& operator=(AgentPopulation const&) = delete;

    bool IsEnabled() const;
    void Update(uint32 diff);
    void OnPlayerLogin(Player* player);
    bool PrefersOnline(uint32 botGuid) const;
    bool PrefersOnlineAccount(uint32 accountId) const;

private:
    AgentPopulation() = default;
    struct Impl;
    Impl* m_impl = nullptr;
};

#endif
