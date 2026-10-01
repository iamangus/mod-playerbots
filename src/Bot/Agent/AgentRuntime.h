/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTRUNTIME_H
#define PLAYERBOTS_AGENTRUNTIME_H

#include <cstdint>
#include <memory>
#include <string>

#include "ObjectGuid.h"

class PlayerbotAI;
class WorldPacket;

class AgentRuntime
{
public:
    explicit AgentRuntime(ObjectGuid botGuid);
    ~AgentRuntime();
    static bool IsSupported();

    // True while any agent controller heartbeat for this worldserver is recent.
    // Externally provisioned populations are only managed while this holds.
    static bool IsControllerPresent();

    AgentRuntime(AgentRuntime const&) = delete;
    AgentRuntime& operator=(AgentRuntime const&) = delete;

    bool IsConfigured(PlayerbotAI* botAI) const;
    bool IsEnabled(PlayerbotAI* botAI) const;
    bool IsSendingChat() const;
    void OnLootResponse(WorldPacket const& packet);
    void OnAuctionBidderNotification(WorldPacket const& packet);
    void Update(PlayerbotAI* botAI, uint32_t elapsed);
    void Stop(PlayerbotAI* botAI);
    void OnChatMessage(PlayerbotAI* botAI, uint8_t type, uint32_t language, ObjectGuid sender,
                       std::string const& senderName, std::string const& channel, std::string const& message);
    void OnGroupInvite(PlayerbotAI* botAI, WorldPacket const& packet);
    void OnTradeStatus(PlayerbotAI* botAI, WorldPacket const& packet);
    uint64 GetTradeRevision() const;
    void OnTradeOperationResult(std::string const& requestId, std::string const& operationId,
                                std::string const& operation, bool success);
    void OnQuestProgress(PlayerbotAI* botAI, uint32_t opcode);
    void OnStrategiesReset(PlayerbotAI* botAI);

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;
};

#endif
