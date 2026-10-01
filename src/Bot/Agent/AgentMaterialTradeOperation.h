/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTMATERIALTRADEOPERATION_H
#define PLAYERBOTS_AGENTMATERIALTRADEOPERATION_H

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "AgentRuntime.h"
#include "Item.h"
#include "ObjectAccessor.h"
#include "Player.h"
#include "PlayerbotAI.h"
#include "PlayerbotOperation.h"
#include "Playerbots.h"
#include "TradeData.h"
#include "WorldPacket.h"
#include "WorldSession.h"

struct AgentMaterialTradeWork
{
    std::atomic<bool> Cancelled{false};
    std::mutex Mutex;
    bool Completed = false;
    bool Success = false;
};

struct AgentTradeMaterial
{
    uint32 ItemId = 0;
    uint32 Count = 0;
};

// This consumes only an already negotiated, partner-accepted material offer.
// It does not initiate trades, change negotiated offers, or impersonate the
// partner's acceptance. Only the bot's own ordinary acceptance is submitted.
class AgentMaterialTradeOperation : public PlayerbotOperation
{
public:
    AgentMaterialTradeOperation(ObjectGuid botGuid, ObjectGuid partnerGuid, uint64 revision,
                                std::vector<AgentTradeMaterial> materials, uint32 price,
                                std::shared_ptr<AgentMaterialTradeWork> work)
        : _botGuid(botGuid),
          _partnerGuid(partnerGuid),
          _revision(revision),
          _materials(std::move(materials)),
          _price(price),
          _work(std::move(work))
    {
    }

    bool Execute() override
    {
        bool const success = !_work->Cancelled.load() && Receive();
        std::lock_guard<std::mutex> guard(_work->Mutex);
        _work->Success = success;
        _work->Completed = true;
        return success;
    }

    ObjectGuid GetBotGuid() const override { return _botGuid; }
    uint32 GetPriority() const override { return 50; }
    std::string GetName() const override { return "AgentMaterialTradeOperation"; }

private:
    bool Receive()
    {
        Player* bot = ObjectAccessor::FindPlayer(_botGuid);
        PlayerbotAI* ai = bot ? GET_PLAYERBOT_AI(bot) : nullptr;
        AgentRuntime* runtime = ai ? ai->GetAgentRuntime() : nullptr;
        TradeData* ours = bot ? bot->GetTradeData() : nullptr;
        TradeData* theirs = ours ? ours->GetTraderData() : nullptr;
        Player* partner = bot ? bot->GetTrader() : nullptr;
        if (!runtime || !runtime->IsEnabled(ai) || runtime->GetTradeRevision() != _revision || !bot->GetSession() ||
            !bot->IsAlive() || bot->IsInCombat() || !partner || partner->GetGUID() != _partnerGuid || !theirs ||
            !theirs->IsAccepted() || ours->GetMoney() != _price || _price > bot->GetMoney() || theirs->GetMoney() ||
            ours->GetSpell() || theirs->GetSpell())
            return false;
        std::map<uint32, uint64> quantities;
        std::map<uint32, uint32> beforeItems;
        if (_materials.empty() || _materials.size() > TRADE_SLOT_TRADED_COUNT)
            return false;
        uint64 total = 0;
        for (AgentTradeMaterial const& material : _materials)
        {
            if (!material.ItemId || !material.Count || beforeItems.contains(material.ItemId))
                return false;
            total += material.Count;
            beforeItems[material.ItemId] = bot->GetItemCount(material.ItemId, false);
        }
        constexpr uint32 maxMaterialCount = 40;
        if (total > maxMaterialCount)
            return false;
        for (uint8 slot = 0; slot < TRADE_SLOT_COUNT; ++slot)
        {
            if (ours->GetItem(TradeSlots(slot)))
                return false;  // Material purchases never authorize giving away an item or enchant service.
            if (Item* item = theirs->GetItem(TradeSlots(slot)))
            {
                if (slot >= TRADE_SLOT_TRADED_COUNT || !beforeItems.contains(item->GetEntry()))
                    return false;
                quantities[item->GetEntry()] += item->GetCount();
            }
        }
        for (AgentTradeMaterial const& material : _materials)
            if (quantities[material.ItemId] != material.Count)
                return false;
        if (_work->Cancelled.load())
            return false;
        uint32 const beforeMoney = bot->GetMoney();
        WorldPacket packet;
        packet << uint32(0);
        bot->GetSession()->HandleAcceptTradeOpcode(packet);
        if (bot->GetTradeData() || uint64(beforeMoney) != uint64(bot->GetMoney()) + _price)
            return false;
        for (AgentTradeMaterial const& material : _materials)
            if (uint64(bot->GetItemCount(material.ItemId, false)) !=
                uint64(beforeItems[material.ItemId]) + material.Count)
                return false;
        return true;
    }

    ObjectGuid _botGuid;
    ObjectGuid _partnerGuid;
    uint64 _revision;
    std::vector<AgentTradeMaterial> _materials;
    uint32 _price;
    std::shared_ptr<AgentMaterialTradeWork> _work;
};

#endif
