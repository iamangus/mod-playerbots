/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTAUCTIONOPERATION_H
#define PLAYERBOTS_AGENTAUCTIONOPERATION_H

#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include "AgentRuntime.h"
#include "AuctionHouseMgr.h"
#include "Creature.h"
#include "GameTime.h"
#include "Item.h"
#include "ObjectAccessor.h"
#include "Player.h"
#include "PlayerbotAI.h"
#include "PlayerbotOperation.h"
#include "Playerbots.h"
#include "WorldPacket.h"
#include "WorldSession.h"

struct AgentAuctionOffer
{
    uint32 AuctionId = 0;
    uint32 ItemId = 0;
    uint32 Count = 0;
    uint32 Buyout = 0;
    uint64 ItemGuid = 0;
    uint32 MinimumBid = 0;
    int64 ExpiresAt = 0;
};

struct AgentAuctionWork
{
    std::atomic<bool> Cancelled{false};
    std::mutex Mutex;
    bool Completed = false;
    bool Success = false;
    bool More = false;
    uint32 NextCursor = 0;
    std::vector<AgentAuctionOffer> Offers;
    uint64 PurchasedItemGuid = 0;
};

// Auction maps and native buyout handlers belong to the world thread. Only a
// bounded, pointer-free public-book page crosses back to the map thread.
class AgentAuctionOperation : public PlayerbotOperation
{
public:
    AgentAuctionOperation(ObjectGuid botGuid, ObjectGuid npcGuid, std::shared_ptr<AgentAuctionWork> work, uint32 itemId,
                          uint32 cursor, uint32 auctionId, uint32 count, uint32 buyout, uint32 budget,
                          ObjectGuid expectedItemGuid, bool bid = false)
        : _botGuid(botGuid),
          _npcGuid(npcGuid),
          _work(std::move(work)),
          _itemId(itemId),
          _cursor(cursor),
          _auctionId(auctionId),
          _count(count),
          _buyout(buyout),
          _budget(budget),
          _expectedItemGuid(expectedItemGuid),
          _bid(bid)
    {
    }

    bool Execute() override
    {
        std::vector<AgentAuctionOffer> offers;
        uint32 nextCursor = _cursor;
        bool more = false;
        uint64 purchased = 0;
        Player* bot = ObjectAccessor::FindPlayer(_botGuid);
        PlayerbotAI* ai = bot ? GET_PLAYERBOT_AI(bot) : nullptr;
        AgentRuntime* runtime = ai ? ai->GetAgentRuntime() : nullptr;
        Creature* npc = bot ? bot->GetNPCIfCanInteractWith(_npcGuid, UNIT_NPC_FLAG_AUCTIONEER) : nullptr;
        bool success = false;
        if (!_work->Cancelled.load() && runtime && runtime->IsEnabled(ai) && npc && bot->GetSession() &&
            bot->IsAlive() && !bot->IsInCombat() && !bot->GetTradeData())
        {
            AuctionHouseObject* house = sAuctionMgr->GetAuctionsMap(npc->GetFaction());
            if (house)
                success = _auctionId ? Buy(bot, house, purchased) : Inspect(house, offers, nextCursor, more);
        }
        std::lock_guard<std::mutex> guard(_work->Mutex);
        _work->Success = success;
        _work->Offers = std::move(offers);
        _work->NextCursor = nextCursor;
        _work->More = more;
        _work->PurchasedItemGuid = purchased;
        _work->Completed = true;
        return success;
    }

    ObjectGuid GetBotGuid() const override { return _botGuid; }
    uint32 GetPriority() const override { return 50; }
    std::string GetName() const override { return "AgentAuctionOperation"; }

private:
    bool Inspect(AuctionHouseObject* house, std::vector<AgentAuctionOffer>& offers, uint32& nextCursor, bool& more)
    {
        constexpr uint32 maxExamined = 512;
        constexpr uint32 maxOffers = 40;
        auto const& auctions = house->GetAuctions();
        auto entry = auctions.upper_bound(_cursor);
        uint32 examined = 0;
        for (; entry != auctions.end() && examined < maxExamined && offers.size() < maxOffers; ++entry)
        {
            if (_work->Cancelled.load())
                return false;
            ++examined;
            nextCursor = entry->first;
            AuctionEntry const* auction = entry->second;
            if (!auction || auction->item_template != _itemId || !auction->itemCount || auction->owner == _botGuid ||
                auction->bidder == _botGuid || auction->expire_time <= GameTime::GetGameTime().count() ||
                !sAuctionMgr->GetAItem(auction->item_guid))
                continue;
            uint64 const minimumBid =
                auction->bid ? uint64(auction->bid) + auction->GetAuctionOutBid() : auction->startbid;
            if (!minimumBid || minimumBid > std::numeric_limits<uint32>::max())
                continue;
            offers.push_back({auction->Id, auction->item_template, auction->itemCount, auction->buyout,
                              auction->item_guid.GetRawValue(), uint32(minimumBid), int64(auction->expire_time)});
        }
        more = entry != auctions.end();
        return true;
    }

    bool Buy(Player* bot, AuctionHouseObject* house, uint64& purchased)
    {
        AuctionEntry const* auction = house->GetAuction(_auctionId);
        if (!auction || auction->item_template != _itemId || auction->itemCount != _count ||
            auction->item_guid != _expectedItemGuid || (!_bid && (!auction->buyout || auction->buyout != _buyout)) ||
            (_bid && (!_buyout || (auction->buyout && _buyout >= auction->buyout) || _buyout <= auction->bid ||
                      _buyout < auction->startbid || _buyout < uint64(auction->bid) + auction->GetAuctionOutBid())) ||
            _buyout > _budget || _buyout > bot->GetMoney() || auction->owner == _botGuid ||
            auction->bidder == _botGuid || auction->expire_time <= GameTime::GetGameTime().count() ||
            _work->Cancelled.load())
            return false;
        ObjectGuid const itemGuid = auction->item_guid;
        uint32 const before = bot->GetMoney();
        WorldPacket packet(CMSG_AUCTION_PLACE_BID);
        packet << _npcGuid << _auctionId << _buyout;
        bot->GetSession()->HandleAuctionPlaceBid(packet);
        if (_bid)
        {
            AuctionEntry const* placed = house->GetAuction(_auctionId);
            bool const verified = placed && placed->bidder == _botGuid && placed->bid == _buyout &&
                                  uint64(before) == uint64(bot->GetMoney()) + _buyout;
            if (verified)
                purchased = itemGuid.GetRawValue();  // Escrow receipt, not owned inventory.
            return verified;
        }
        // Ordinary auction-won mail is not immediate inventory, nor proof that
        // the asynchronous database transaction has committed.
        Item* mailed = bot->GetMItem(itemGuid.GetCounter());
        bool const verified = !house->GetAuction(_auctionId) && uint64(before) == uint64(bot->GetMoney()) + _buyout &&
                              mailed && mailed->GetEntry() == _itemId && mailed->GetCount() == _count;
        if (verified)
            purchased = itemGuid.GetRawValue();
        return verified;
    }

    ObjectGuid _botGuid;
    ObjectGuid _npcGuid;
    std::shared_ptr<AgentAuctionWork> _work;
    uint32 _itemId;
    uint32 _cursor;
    uint32 _auctionId;
    uint32 _count;
    uint32 _buyout;
    uint32 _budget;
    ObjectGuid _expectedItemGuid;
    bool _bid;
};

#endif
