/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTRUNTIME_H
#define PLAYERBOTS_AGENTRUNTIME_H

#include "ObjectGuid.h"
#include <cstdint>
#include <memory>
#include <string>

class PlayerbotAI;
class WorldPacket;

class AgentRuntime
{
public:
    explicit AgentRuntime(ObjectGuid botGuid);
    ~AgentRuntime();
    static bool IsSupported();

    AgentRuntime(AgentRuntime const&) = delete;
    AgentRuntime& operator=(AgentRuntime const&) = delete;

    bool IsConfigured(PlayerbotAI* botAI) const;
    bool IsEnabled(PlayerbotAI* botAI) const;
    bool IsSendingChat() const;
    void Update(PlayerbotAI* botAI, uint32_t elapsed);
    void Stop(PlayerbotAI* botAI);
    void OnChatMessage(PlayerbotAI* botAI, uint8_t type, uint32_t language, ObjectGuid sender,
                       std::string const& senderName,
                       std::string const& channel, std::string const& message);
    void OnGroupInvite(PlayerbotAI* botAI, WorldPacket const& packet);
    void OnQuestProgress(PlayerbotAI* botAI, uint32_t opcode);

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

#endif
