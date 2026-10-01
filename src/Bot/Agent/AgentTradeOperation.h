/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTTRADEOPERATION_H
#define PLAYERBOTS_AGENTTRADEOPERATION_H

#include "AgentRuntime.h"
#include "Item.h"
#include "ObjectAccessor.h"
#include "Player.h"
#include "PlayerbotAI.h"
#include "PlayerbotOperation.h"
#include "Playerbots.h"
#include "WorldSession.h"

// Trade handlers run on the world thread, just as they do for client packets.
// Revalidate the partner and offer revision there, not only when enqueued.
class AgentTradeOperation : public PlayerbotOperation
{
public:
    AgentTradeOperation(ObjectGuid botGuid, ObjectGuid partnerGuid, uint64 revision, std::string operation,
                        uint32 money, uint8 tradeSlot, ObjectGuid itemGuid, std::string requestId,
                        std::string operationId)
        : _botGuid(botGuid),
          _partnerGuid(partnerGuid),
          _revision(revision),
          _operation(std::move(operation)),
          _money(money),
          _tradeSlot(tradeSlot),
          _itemGuid(itemGuid),
          _requestId(std::move(requestId)),
          _operationId(std::move(operationId))
    {
    }

    bool Execute() override
    {
        Player* bot = ObjectAccessor::FindPlayer(_botGuid);
        if (!bot || !bot->GetSession())
            return false;
        PlayerbotAI* botAI = GET_PLAYERBOT_AI(bot);
        AgentRuntime* runtime = botAI ? botAI->GetAgentRuntime() : nullptr;
        if (!runtime || !runtime->IsEnabled(botAI))
            return false;

        bool const success = Apply(bot, botAI, runtime);
        runtime->OnTradeOperationResult(_requestId, _operationId, _operation, success);
        return success;
    }

    ObjectGuid GetBotGuid() const override { return _botGuid; }
    uint32 GetPriority() const override { return 50; }
    std::string GetName() const override { return "AgentTradeOperation"; }

private:
    bool Apply(Player* bot, PlayerbotAI* botAI, AgentRuntime* runtime)
    {
        if (!bot->GetTrader() || bot->GetTrader()->GetGUID() != _partnerGuid ||
            runtime->GetTradeRevision() != _revision)
            return false;

        WorldPacket packet;
        if (_operation == "begin_trade")
            bot->GetSession()->HandleBeginTradeOpcode(packet);
        else if (_operation == "accept_trade")
        {
            packet << uint32(0);
            bot->GetSession()->HandleAcceptTradeOpcode(packet);
            return !bot->GetTradeData() || bot->GetTradeData()->IsAccepted();
        }
        else if (_operation == "cancel_trade")
            bot->GetSession()->HandleCancelTradeOpcode(packet);
        else if (_operation == "offer_trade_money")
        {
            if (_money > bot->GetMoney())
                return false;
            packet << _money;
            bot->GetSession()->HandleSetTradeGoldOpcode(packet);
            return bot->GetTradeData() && bot->GetTradeData()->GetMoney() == _money;
        }
        else if (_operation == "offer_trade_item")
        {
            if (_tradeSlot >= TRADE_SLOT_TRADED_COUNT)
                return false;
            Item* selected = nullptr;
            for (Item* item : botAI->GetInventoryItems())
                if (item && item->GetGUID() == _itemGuid && !item->IsEquipped() && item->CanBeTraded())
                {
                    selected = item;
                    break;
                }
            if (!selected)
                return false;
            packet << _tradeSlot << selected->GetBagSlot() << selected->GetSlot();
            bot->GetSession()->HandleSetTradeItemOpcode(packet);
            return bot->GetTradeData() && bot->GetTradeData()->GetItem(TradeSlots(_tradeSlot)) == selected;
        }
        else
            return false;
        return true;
    }

    ObjectGuid _botGuid;
    ObjectGuid _partnerGuid;
    uint64 _revision;
    std::string _operation;
    uint32 _money;
    uint8 _tradeSlot;
    ObjectGuid _itemGuid;
    std::string _requestId;
    std::string _operationId;
};

#endif
