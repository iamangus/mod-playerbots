/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTMAILBOXOPERATION_H
#define PLAYERBOTS_AGENTMAILBOXOPERATION_H

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "AuctionHouseMgr.h"
#include "GameTime.h"
#include "Item.h"
#include "Mail.h"
#include "ObjectAccessor.h"
#include "Player.h"
#include "PlayerbotOperation.h"
#include "WorldPacket.h"
#include "WorldSession.h"

struct AgentMailboxWork
{
    std::atomic<bool> Cancelled{false};
    std::mutex Mutex;
    bool Completed = false;
    bool Success = false;
    bool Empty = false;
    uint32 AuctionOutcome = 0;  // 1 = won attachment, 2 = outbid/cancel refund.
};

struct AgentAuctionMailFilter
{
    uint32 AuctionId = 0;
    uint32 ItemId = 0;
    uint32 Count = 0;
    uint32 Bid = 0;
};

// The map thread receives only this pointer-free result handoff. Native mail
// handlers and their database transactions run on the world thread.
class AgentMailboxOperation : public PlayerbotOperation
{
public:
    AgentMailboxOperation(ObjectGuid botGuid, ObjectGuid mailboxGuid, std::shared_ptr<AgentMailboxWork> work,
                          ObjectGuid itemGuid = ObjectGuid::Empty, AgentAuctionMailFilter filter = {})
        : _botGuid(botGuid),
          _mailboxGuid(mailboxGuid),
          _work(std::move(work)),
          _itemGuid(itemGuid),
          _auctionFilter(filter)
    {
    }

    AgentMailboxOperation(ObjectGuid botGuid, ObjectGuid mailboxGuid, std::shared_ptr<AgentMailboxWork> work,
                          std::string recipient, std::string subject, std::string body, ObjectGuid itemGuid,
                          uint32 money, uint32 budget)
        : _botGuid(botGuid),
          _mailboxGuid(mailboxGuid),
          _work(std::move(work)),
          _send(true),
          _recipient(std::move(recipient)),
          _subject(std::move(subject)),
          _body(std::move(body)),
          _itemGuid(itemGuid),
          _money(money),
          _budget(budget)
    {
    }

    bool Execute() override
    {
        bool empty = false;
        bool const success = !_work->Cancelled.load() && (_send ? SendOne() : CollectOne(empty));
        std::lock_guard<std::mutex> guard(_work->Mutex);
        _work->Success = success;
        _work->Empty = empty;
        _work->AuctionOutcome = _auctionOutcome;
        _work->Completed = true;
        return success;
    }

    ObjectGuid GetBotGuid() const override { return _botGuid; }
    uint32 GetPriority() const override { return 50; }
    std::string GetName() const override { return "AgentMailboxOperation"; }

private:
    bool SendOne()
    {
        constexpr uint32 postageCopper = 30;
        Player* bot = ObjectAccessor::FindPlayer(_botGuid);
        WorldSession* session = bot ? bot->GetSession() : nullptr;
        uint64 const spend = uint64(_money) + postageCopper;
        if (!session || !bot->IsAlive() || bot->IsInCombat() || bot->GetTradeData() ||
            !session->CanOpenMailBox(_mailboxGuid) || spend > _budget || spend > bot->GetMoney())
            return false;
        if (!_itemGuid.IsEmpty())
        {
            Item* item = bot->GetItemByGuid(_itemGuid);
            if (!item || item->IsEquipped() || item->IsInTrade() || !item->CanBeTraded(true))
                return false;
        }
        if (_work->Cancelled.load())
            return false;
        uint32 const before = bot->GetMoney();
        WorldPacket packet(CMSG_SEND_MAIL);
        packet << _mailboxGuid << _recipient << _subject << _body << uint32(0) << uint32(0);
        packet << uint8(_itemGuid.IsEmpty() ? 0 : 1);
        if (!_itemGuid.IsEmpty())
            packet << uint8(0) << _itemGuid;
        packet << _money << uint32(0) << uint64(0) << uint8(0);  // no COD
        session->HandleSendMail(packet);
        // Confirm native submission, not delivery to the recipient. Ordinary
        // mail delay and asynchronous database commit still apply.
        return uint64(before) == uint64(bot->GetMoney()) + spend &&
               (_itemGuid.IsEmpty() || !bot->GetItemByGuid(_itemGuid));
    }

    bool CollectOne(bool& empty)
    {
        Player* bot = ObjectAccessor::FindPlayer(_botGuid);
        WorldSession* session = bot ? bot->GetSession() : nullptr;
        if (!session || !bot->IsAlive() || bot->IsInCombat() || bot->GetTradeData() ||
            !session->CanOpenMailBox(_mailboxGuid))
            return false;
        WorldPacket list(CMSG_GET_MAIL_LIST);
        list << _mailboxGuid;
        session->HandleGetMailList(list);
        time_t const now = GameTime::GetGameTime().count();
        for (Mail* mail : bot->GetMails())
        {
            if (!mail || mail->state == MAIL_STATE_DELETED || mail->deliver_time > now || mail->expire_time < now ||
                mail->COD || _work->Cancelled.load())
                continue;
            bool refund = false;
            bool won = false;
            if (_auctionFilter.AuctionId)
            {
                if (mail->messageType != MAIL_AUCTION)
                    continue;
                auto subject = [&](MailAuctionAnswers answer)
                {
                    return std::to_string(_auctionFilter.ItemId) + ":0:" + std::to_string(answer) + ":" +
                           std::to_string(_auctionFilter.AuctionId) + ":" + std::to_string(_auctionFilter.Count);
                };
                won = mail->subject == subject(AUCTION_WON);
                refund = mail->subject == subject(AUCTION_OUTBIDDED) ||
                         mail->subject == subject(AUCTION_CANCELLED_TO_BIDDER);
                if ((!won && !refund) || (refund && mail->money != _auctionFilter.Bid))
                    continue;
            }
            if (mail->money && (_itemGuid.IsEmpty() || refund))
            {
                uint32 const before = bot->GetMoney();
                uint32 const amount = mail->money;
                WorldPacket packet(CMSG_MAIL_TAKE_MONEY);
                packet << _mailboxGuid << mail->messageID;
                session->HandleMailTakeMoney(packet);
                bool const verified = mail->money == 0 && uint64(bot->GetMoney()) == uint64(before) + amount;
                if (verified && refund)
                    _auctionOutcome = 2;
                return verified;
            }
            if (!mail->items.empty())
            {
                uint32 itemGuid = 0;
                for (auto const& attachment : mail->items)
                    if (_itemGuid.IsEmpty() || attachment.item_guid == _itemGuid.GetCounter())
                    {
                        itemGuid = attachment.item_guid;
                        break;
                    }
                if (!itemGuid)
                    continue;
                Item* mailedItem = bot->GetMItem(itemGuid);
                if (!mailedItem || (won && (mailedItem->GetEntry() != _auctionFilter.ItemId ||
                                            mailedItem->GetCount() != _auctionFilter.Count)))
                    return false;
                uint32 const itemId = mailedItem->GetEntry();
                uint32 const quantity = mailedItem->GetCount();
                uint32 const beforeItems = bot->GetItemCount(itemId, false);
                WorldPacket packet(CMSG_MAIL_TAKE_ITEM);
                packet << _mailboxGuid << mail->messageID << itemGuid;
                session->HandleMailTakeItem(packet);
                bool const verified = uint64(bot->GetItemCount(itemId, false)) == uint64(beforeItems) + quantity;
                if (verified && won)
                    _auctionOutcome = 1;
                return verified;
            }
        }
        if (_work->Cancelled.load())
            return false;
        empty = true;
        return true;
    }

    ObjectGuid _botGuid;
    ObjectGuid _mailboxGuid;
    std::shared_ptr<AgentMailboxWork> _work;
    bool _send = false;
    std::string _recipient;
    std::string _subject;
    std::string _body;
    ObjectGuid _itemGuid;
    uint32 _money = 0;
    uint32 _budget = 0;
    AgentAuctionMailFilter _auctionFilter;
    uint32 _auctionOutcome = 0;
};

#endif
