/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#include <boost/bind/placeholders.hpp>

namespace boost::property_tree::json_parser::detail
{
using boost::placeholders::_1;
}

#include "AgentAuctionOperation.h"
#include "AgentBridgeShared.h"
#include "AgentMailboxOperation.h"
#include "AgentMaterialTradeOperation.h"
#include "AgentRuntime.h"
#include "AgentTradeOperation.h"
#include "AiObjectContext.h"
#include "ChooseTravelTargetAction.h"
#include "Corpse.h"
#include "Creature.h"
#include "DBCStores.h"
#include "Duration.h"
#include "Event.h"
#include "EventMap.h"
#include "FishingAction.h"
#include "GameObject.h"
#include "GameTime.h"
#include "Group.h"
#include "Item.h"
#include "ItemPackets.h"
#include "LootMgr.h"
#include "LootObjectStack.h"
#include "MotionMaster.h"
#include "MovementActions.h"
#include "ObjectAccessor.h"
#include "ObjectMgr.h"
#include "PathGenerator.h"
#include "Player.h"
#include "PlayerbotAI.h"
#include "PlayerbotAIConfig.h"
#include "PlayerbotWorldThreadProcessor.h"
#include "Playerbots.h"
#include "QuestDef.h"
#include "SharedDefines.h"
#include "SpellInfo.h"
#include "SpellMgr.h"
#include "Timer.h"
#include "Trainer.h"
#include "TravelMgr.h"
#include "World.h"
#include "WorldPacket.h"
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
#include "libsidecar.h"
#endif
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

namespace
{
constexpr size_t AGENT_MAX_COMMAND_BYTES = 8192;
constexpr size_t AGENT_MAX_EVENT_BYTES = 65536;
constexpr size_t AGENT_MAX_PENDING_COMMANDS = 64;
constexpr size_t AGENT_MAX_PENDING_EVENTS = 64;
constexpr uint32 AGENT_HEARTBEAT_MS = 60000;
constexpr uint32 AGENT_CONTROLLER_TIMEOUT_MS = 90000;

// Most recent controller heartbeat arrival (any shard), world-uptime ms.
std::atomic<uint32> lastControllerHeartbeatMs{0};
constexpr uint32 AGENT_MAX_SNAPSHOT_CREATURES = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_CORPSES = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_GAMEOBJECTS = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_PLAYERS = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_ITEMS = 40;
constexpr uint32 AGENT_MAX_SNAPSHOT_AVAILABLE_QUESTS = 12;
constexpr uint32 AGENT_MAX_AVAILABLE_QUESTS_PER_GIVER = 3;
constexpr uint32 AGENT_MAX_QUEST_ITEM_SOURCES = 32;
constexpr uint32 AGENT_RECOVERY_TIMEOUT_MS = 15 * MINUTE * IN_MILLISECONDS;
constexpr uint32 AGENT_MAX_VENDOR_OFFERS_PER_NPC = 12;
constexpr uint32 AGENT_MAX_CRAFTING_RECIPES = 40;
constexpr uint32 AGENT_MAX_OBSERVED_DROP_ITEMS = 12;
constexpr uint32 AGENT_MAX_CRAFTING_QUERY_DEPTH = 8;
constexpr uint32 AGENT_MAX_CRAFTING_RECIPE_OFFSET = 4096;
constexpr uint32 AGENT_CRAFTING_CAST_TIMEOUT_MS = 90000;
constexpr uint32 AGENT_FISHING_CAST_TIMEOUT_MS = 45000;
constexpr uint32 AGENT_FISHING_BOBBER_SPAWN_MS = 3000;
constexpr uint32 AGENT_EVENT_SHARD_COUNT = 256;
constexpr uint32 AGENT_OPERATION_TICK_MS = 250;
constexpr uint32 AGENT_OPERATION_EVENT = 1;
constexpr uint32 AGENT_OPERATION_PROGRESS_MS = 5000;
constexpr uint32 AGENT_OPERATION_STALL_MS = 120000;

struct AgentBridgeCommand
{
    std::string botGuid;
    std::string requestId;
    std::string operationId;
    std::string ownerToken;
    std::string ownerEpoch;
    int64 deadlineUnixMs = 0;
    std::string operation;
    std::string arguments;
};

struct AgentBridgeInbox
{
    std::mutex mutex;
    std::deque<AgentBridgeCommand> commands;
    std::atomic<bool> hasCommands{false};
};

class AgentBridgeTransport
{
public:
    static std::string OwnerToken() { return agent_bridge::OwnerToken(); }

    static uint32 EventShard(std::string const& botToken) { return agent_bridge::EventShard(botToken); }

    static bool ControllerRecent(std::string const& ownerToken, uint32 eventShard)
    {
        (void)ownerToken;
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
        if (eventShard >= AGENT_EVENT_SHARD_COUNT)
            return false;
        uint32 const lastHeartbeat = HeartbeatTimes()[eventShard].load(std::memory_order_acquire);
        return lastHeartbeat && getMSTimeDiff(lastHeartbeat, getMSTime()) < AGENT_CONTROLLER_TIMEOUT_MS;
#else
        (void)eventShard;
        return false;
#endif
    }

    static void RecordControllerHeartbeat(std::string const& ownerToken, uint32 eventShard)
    {
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
        if (ownerToken == OwnerToken() && eventShard < AGENT_EVENT_SHARD_COUNT)
        {
            uint32 const now = getMSTime();
            HeartbeatTimes()[eventShard].store(now, std::memory_order_release);
            lastControllerHeartbeatMs.store(now, std::memory_order_release);
        }
#else
        (void)ownerToken;
        (void)eventShard;
#endif
    }

    static void Register(std::string const& botGuid, std::shared_ptr<AgentBridgeInbox> const& inbox)
    {
        std::lock_guard<std::mutex> lock(InboxesMutex());
        Inboxes()[botGuid] = inbox;
    }

    static void Unregister(std::string const& botGuid, std::shared_ptr<AgentBridgeInbox> const& inbox)
    {
        std::lock_guard<std::mutex> lock(InboxesMutex());
        auto found = Inboxes().find(botGuid);
        if (found != Inboxes().end() && found->second.lock() == inbox)
            Inboxes().erase(found);
    }

    static bool Pop(std::shared_ptr<AgentBridgeInbox> const& inbox, AgentBridgeCommand& command)
    {
        if (!inbox->hasCommands.load(std::memory_order_acquire))
            return false;
        std::lock_guard<std::mutex> lock(inbox->mutex);
        if (inbox->commands.empty())
        {
            inbox->hasCommands.store(false, std::memory_order_release);
            return false;
        }
        command = std::move(inbox->commands.front());
        inbox->commands.pop_front();
        if (inbox->commands.empty())
            inbox->hasCommands.store(false, std::memory_order_release);
        return true;
    }

    static void Clear(std::shared_ptr<AgentBridgeInbox> const& inbox)
    {
        std::lock_guard<std::mutex> lock(inbox->mutex);
        inbox->commands.clear();
        inbox->hasCommands.store(false, std::memory_order_release);
    }

    static bool EnsureSubscribed(std::string const& subjectPrefix, std::string const& ownerToken)
    {
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
        static std::mutex mutex;
        static bool subscribed = false;
        static std::string subscribedSubject;
        static uint32 lastAttempt = 0;
        std::lock_guard<std::mutex> lock(mutex);
        if (subscribed)
            return subscribedSubject == CommandSubject(subjectPrefix, ownerToken);
        uint32 const now = getMSTime();
        if (lastAttempt && getMSTimeDiff(lastAttempt, now) < 5000)
            return false;
        lastAttempt = now;

        if (TC9CheckAbiCompatible(TC9_VERSION_MAJOR, TC9_VERSION_MINOR) != 0)
        {
            LOG_ERROR("playerbots.agent", "ToCloud9 sidecar ABI is incompatible with the playerbot bridge");
            return false;
        }

        std::string const subject = CommandSubject(subjectPrefix, ownerToken);
        if (!agent_bridge::Subscribe(subject, &ReceiveCommand))
            return false;

        subscribed = true;
        subscribedSubject = subject;
        LOG_INFO("playerbots.agent", "Subscribed to ToCloud9 agent commands on {}", subject);
        return true;
#else
        (void)subjectPrefix;
        (void)ownerToken;
        return false;
#endif
    }

    static bool Publish(std::string const& subjectPrefix, std::string const& ownerToken, ObjectGuid botGuid,
                        std::string const& event)
    {
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
        std::string const subject = EventSubject(subjectPrefix, ownerToken, botGuid);
        return agent_bridge::Publish(subject, event);
#else
        (void)subjectPrefix;
        (void)ownerToken;
        (void)botGuid;
        (void)event;
        return false;
#endif
    }

    static std::string BotToken(ObjectGuid botGuid)
    {
        std::string token = botGuid.ToString();
        for (char& character : token)
        {
            unsigned char const value = static_cast<unsigned char>(character);
            if (!std::isalnum(value) && character != '_' && character != '-')
                character = '_';
        }
        return token;
    }

private:
    static std::mutex& InboxesMutex()
    {
        static std::mutex mutex;
        return mutex;
    }

    static std::map<std::string, std::weak_ptr<AgentBridgeInbox>>& Inboxes()
    {
        static std::map<std::string, std::weak_ptr<AgentBridgeInbox>> inboxes;
        return inboxes;
    }

#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    static std::array<std::atomic<uint32>, AGENT_EVENT_SHARD_COUNT>& HeartbeatTimes()
    {
        static std::array<std::atomic<uint32>, AGENT_EVENT_SHARD_COUNT> heartbeats{};
        return heartbeats;
    }
#endif

    static std::string CommandSubject(std::string const& prefix, std::string const& ownerToken)
    {
        return prefix + ".commands." + ownerToken;
    }

    static std::string EventSubject(std::string const& prefix, std::string const& ownerToken, ObjectGuid botGuid)
    {
        std::string const botToken = BotToken(botGuid);
        return prefix + ".events." + std::to_string(EventShard(botToken)) + "." + ownerToken + "." + botToken;
    }

#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    static void ReceiveCommand(char const* /*subject*/, char const* payload, int payloadLength)
    {
        if (!payload || payloadLength <= 0 || static_cast<size_t>(payloadLength) > AGENT_MAX_COMMAND_BYTES)
            return;

        try
        {
            boost::property_tree::ptree root;
            std::istringstream input(std::string(payload, static_cast<size_t>(payloadLength)));
            boost::property_tree::read_json(input, root);
            AgentBridgeCommand command;
            command.botGuid = root.get<std::string>("bot_guid", "");
            command.requestId = root.get<std::string>("request_id", "");
            command.operationId = root.get<std::string>("operation_id", "");
            command.ownerToken = root.get<std::string>("owner_token", "");
            command.ownerEpoch = root.get<std::string>("owner_epoch", "");
            command.deadlineUnixMs = root.get<int64>("deadline_unix_ms", 0);
            command.operation = root.get<std::string>("operation", "");
            if (command.operation == "controller_heartbeat")
            {
                RecordControllerHeartbeat(command.ownerToken, root.get<uint32>("shard_id", 0));
                return;
            }
            auto arguments = root.get_child_optional("arguments");
            if (!arguments || command.botGuid.empty() || command.operation.empty())
                return;

            std::ostringstream serialized;
            boost::property_tree::write_json(serialized, *arguments, false);
            command.arguments = serialized.str();

            std::shared_ptr<AgentBridgeInbox> inbox;
            {
                std::lock_guard<std::mutex> lock(InboxesMutex());
                auto found = Inboxes().find(command.botGuid);
                if (found != Inboxes().end())
                    inbox = found->second.lock();
            }
            if (!inbox)
                return;

            std::lock_guard<std::mutex> lock(inbox->mutex);
            if (inbox->commands.size() < AGENT_MAX_PENDING_COMMANDS)
            {
                inbox->commands.push_back(std::move(command));
                inbox->hasCommands.store(true, std::memory_order_release);
            }
            else
                LOG_WARN("playerbots.agent", "Bridge command queue full for bot {}", command.botGuid);
        }
        catch (std::exception const& exception)
        {
            LOG_WARN("playerbots.agent", "Rejected invalid bridge command: {}", exception.what());
        }
    }
#endif
};

std::string EscapeJson(std::string const& value) { return agent_bridge::EscapeJson(value); }

std::string TruncateUtf8(std::string value, size_t maxBytes)
{
    return agent_bridge::TruncateUtf8(std::move(value), maxBytes);
}

std::string GetString(boost::property_tree::ptree const& tree, std::string const& key, std::string const& fallback = "")
{
    return tree.get<std::string>(key, fallback);
}

std::string ChatTypeName(uint8 type)
{
    switch (type)
    {
        case CHAT_MSG_SAY:
            return "say";
        case CHAT_MSG_YELL:
            return "yell";
        case CHAT_MSG_WHISPER:
            return "whisper";
        case CHAT_MSG_PARTY:
            return "party";
        case CHAT_MSG_RAID:
            return "raid";
        case CHAT_MSG_GUILD:
            return "guild";
        case CHAT_MSG_OFFICER:
            return "officer";
        case CHAT_MSG_CHANNEL:
            return "channel";
        default:
            return "other";
    }
}

uint32 GetUInt(boost::property_tree::ptree const& tree, std::string const& key, uint32 fallback = 0)
{
    return tree.get<uint32>(key, fallback);
}

uint64 GetUInt64(boost::property_tree::ptree const& tree, std::string const& key)
{
    std::string const value = tree.get<std::string>(key, "0");
    return value.empty() ? 0 : std::stoull(value);
}

std::string QuestObjectiveNavigationBlockReason(Player* bot, Quest const* quest)
{
    if (!quest)
        return "quest template unavailable";
    if (quest->GetType() == QUEST_TYPE_ELITE || quest->GetType() == QUEST_TYPE_DUNGEON)
        return "quest objective navigation requires deferred elite or dungeon content";
    if (quest->GetQuestLevel() > bot->GetLevel() + 1)
        return "quest objective navigation requires level " + std::to_string(quest->GetQuestLevel() - 1);
    return "";
}

WorldPosition* FindQuestNavigationPoint(Player* bot, TravelDestination* destination)
{
    if (!destination)
        return nullptr;
    WorldPosition position(bot);
    std::vector<WorldPosition*> candidates;
    for (WorldPosition* point : destination->getPoints(true))
        if (point && point->GetMapId() == bot->GetMapId())
            candidates.push_back(point);
    if (candidates.empty())
        return nullptr;

    // Admission-only work: inspect at most eight nearby spawn points for this
    // exact source, never a scan/path query from a per-tick trigger.
    constexpr size_t MAX_QUEST_PATH_CANDIDATES = 8;
    size_t const count = std::min(candidates.size(), MAX_QUEST_PATH_CANDIDATES);
    std::partial_sort(candidates.begin(), candidates.begin() + count, candidates.end(),
                      [&](WorldPosition const* left, WorldPosition const* right)
                      { return left->GetExactDist(&position) < right->GetExactDist(&position); });
    for (size_t index = 0; index < count; ++index)
    {
        WorldPosition* point = candidates[index];
        if (point->GetExactDist(bot) <= destination->getRadiusMin())
            return point;
        PathGenerator path(bot);
        if (!path.CalculatePath(point->GetPositionX(), point->GetPositionY(), point->GetPositionZ()) ||
            !(path.GetPathType() & PATHFIND_NORMAL) ||
            (path.GetPathType() &
             (PATHFIND_SHORTCUT | PATHFIND_NOPATH | PATHFIND_SHORT | PATHFIND_NOT_USING_PATH | PATHFIND_FARFROMPOLY)) ||
            path.GetPath().size() < 2)
            continue;
        if (index)
            LOG_INFO("playerbots.agent", "Quest route alternate bot={} source_entry={} candidate={} goal=[{},{},{}]",
                     bot->GetName(), destination->getEntry(), index, point->GetPositionX(), point->GetPositionY(),
                     point->GetPositionZ());
        return point;
    }
    // No proven complete route: retain ordinary bounded partial-path navigation
    // and its diagnostics. Never relocate a bot or claim the spawn was reached.
    return candidates.front();
}

TravelDestination* FindQuestDestination(Player* bot, uint32 questId, uint32 objectiveIndex, uint32 itemId, bool turnIn,
                                        bool start)
{
    QuestStatus const status = bot->GetQuestStatus(questId);
    if (start)
    {
        if (status != QUEST_STATUS_NONE)
            return nullptr;
    }
    else
    {
        if (status != QUEST_STATUS_INCOMPLETE && status != QUEST_STATUS_COMPLETE)
            return nullptr;
        bool const completed = status == QUEST_STATUS_COMPLETE;
        if (turnIn != completed)
            return nullptr;
    }

    if (start)
    {
        // Available quests have no entry in getQuestTravelDestinations' taker
        // list; their start points live in the quest-giver relations.
        auto const iterator = TravelMgr::instance().quests.find(questId);
        if (iterator == TravelMgr::instance().quests.end() || !iterator->second)
        {
            if (TravelMgr::instance().quests.empty())
                LOG_ERROR("playerbots.agent",
                          "navigate_to_quest_giver rejected for bot {}: quest travel table is empty; the "
                          "quest travel table failed to load or is still building",
                          bot->GetName().c_str());
            return nullptr;
        }
        Quest const* quest = sObjectMgr->GetQuestTemplate(questId);
        if (!quest)
            return nullptr;
        WorldPosition botPosition(bot);
        TravelDestination* bestDestination = nullptr;
        float bestDistance = std::numeric_limits<float>::max();
        for (TravelDestination* destination : iterator->second->questGivers)
        {
            if (!destination)
                continue;
            // Quest-relation destinations gate on legacy party/strategy values
            // ("can fight equal", "following party") that agent-controlled solo
            // bots do not maintain. Check the same map and quest-level rules
            // directly instead; level and availability are validated again by
            // the accept primitive before any quest is taken.
            if ((int32)quest->GetQuestLevel() >= (int32)bot->GetLevel() + (int32)5)
                continue;
            float distance = std::numeric_limits<float>::max();
            for (WorldPosition* point : destination->getPoints(true))
                if (point && point->GetMapId() == bot->GetMapId())
                    distance = std::min(distance, point->distance(&botPosition));
            if (distance < bestDistance)
            {
                bestDistance = distance;
                bestDestination = destination;
            }
        }
        return bestDestination;
    }

    bool const ignoreObjectives = turnIn;
    Quest const* quest = sObjectMgr->GetQuestTemplate(questId);
    if (!quest)
        return nullptr;
    if (!turnIn && !QuestObjectiveNavigationBlockReason(bot, quest).empty())
        return nullptr;
    std::vector<int32> itemSources;
    if (!turnIn)
    {
        if (itemId)
        {
            if (objectiveIndex >= QUEST_ITEM_OBJECTIVES_COUNT || quest->RequiredItemId[objectiveIndex] != itemId)
                return nullptr;
            auto const source = TravelMgr::instance().questItemSources.find(itemId);
            if (source == TravelMgr::instance().questItemSources.end())
                return nullptr;
            itemSources = source->second;
        }
        else if (objectiveIndex >= QUEST_OBJECTIVES_COUNT || !quest->RequiredNpcOrGo[objectiveIndex])
            return nullptr;
    }
    std::vector<TravelDestination*> destinations =
        TravelMgr::instance().getQuestTravelDestinations(bot, questId, true, true, 0.0f, ignoreObjectives);
    WorldPosition position(bot);
    TravelDestination* bestDestination = nullptr;
    float bestDistance = std::numeric_limits<float>::max();
    for (TravelDestination* destination : destinations)
    {
        if (!destination)
            continue;
        if (turnIn)
        {
            QuestRelationTravelDestination* relation = dynamic_cast<QuestRelationTravelDestination*>(destination);
            if (!relation || relation->getRelation() == 0)
                continue;
        }
        else
        {
            QuestObjectiveTravelDestination* objective = dynamic_cast<QuestObjectiveTravelDestination*>(destination);
            if (!objective)
                continue;
            int32 const entry = objective->getEntry();
            if (itemId ? std::find(itemSources.begin(), itemSources.end(), entry) == itemSources.end()
                       : entry != quest->RequiredNpcOrGo[objectiveIndex])
                continue;
            // Agent tasks validate their own progress and do not maintain legacy
            // combat-readiness/group values. Keep concrete difficulty limits,
            // but allow travel to depleted spawn areas while mobs respawn.
            if (entry > 0)
            {
                CreatureTemplate const* creature = sObjectMgr->GetCreatureTemplate(entry);
                if (!creature || creature->rank != CREATURE_ELITE_NORMAL || creature->maxlevel > bot->GetLevel() + 4)
                    continue;
            }
        }
        std::vector<WorldPosition*> const points = destination->getPoints(true);
        float distance = std::numeric_limits<float>::max();
        for (WorldPosition* point : points)
        {
            if (!point || point->GetMapId() != bot->GetMapId())
                continue;
            distance = std::min(distance, point->distance(&position));
        }
        if (distance < bestDistance)
        {
            bestDistance = distance;
            bestDestination = destination;
        }
    }
    return bestDestination;
}

std::string SerializeEvent(std::string const& type, ObjectGuid botGuid, std::string const& ownerToken,
                           std::string const& ownerEpoch, uint64 eventSequence, std::string const& requestId,
                           std::string const& payload)
{
    std::string const botToken = AgentBridgeTransport::BotToken(botGuid);
    int64 const eventTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::ostringstream event;
    event << "{\"version\":1,\"event_id\":\"" << EscapeJson(botToken + "-" + std::to_string(eventSequence))
          << "\",\"timestamp_unix_ms\":" << eventTime << ",\"type\":\"" << EscapeJson(type) << "\",\"bot_guid\":\""
          << EscapeJson(botToken) << "\",\"owner_token\":\"" << EscapeJson(ownerToken) << "\",\"owner_epoch\":\""
          << EscapeJson(ownerEpoch) << "\",\"request_id\":\"" << EscapeJson(requestId)
          << "\",\"payload\":" << (payload.empty() ? "{}" : payload) << "}";
    return event.str();
}
}  // namespace

struct AgentRuntime::Impl
{
    struct LocalOperation
    {
        AgentBridgeCommand command;
        ObjectGuid target;
        uint32 mapId = 0;
        uint32 questId = 0;
        uint32 serviceId = 0;
        uint32 serviceLimit = 0;
        uint32 serviceCompleted = 0;
        uint32 serviceBudget = 0;
        uint32 serviceSpent = 0;
        std::map<uint32, uint32> fishingItemsBefore;
        std::vector<uint32> fishingLootItems;
        uint32 fishingStartedMs = 0;
        bool fishingReeled = false;
        std::string diagnosticBlock;
        std::shared_ptr<AgentMailboxWork> mailboxWork;
        std::shared_ptr<AgentAuctionWork> auctionWork;
        std::shared_ptr<AgentMaterialTradeWork> materialTradeWork;
        uint64 materialTradeRevision = 0;
        std::vector<AgentTradeMaterial> tradeMaterials;
        uint32 auctionId = 0;
        uint32 auctionCursor = 0;
        uint32 auctionBuyout = 0;
        ObjectGuid auctionItemGuid;
        std::string mailRecipient;
        std::string mailSubject;
        std::string mailBody;
        ObjectGuid mailItemGuid;
        uint32 mailMoney = 0;
        AgentAuctionMailFilter auctionMailFilter;
        uint32 craftItemId = 0;
        uint32 craftItemsBefore = 0;
        float distance = 2.0f;
        bool attempted = false;
        bool corpseOnly = false;
        bool movementStarted = false;
        bool paused = false;
        uint32 lastProgressMs = 0;
        uint32 recoveryStartedMs = 0;
        uint32 lastReportMs = 0;
        uint32 lastInteractionMs = 0;
        uint32 lastMoveAttemptMs = 0;
        uint32 navigationPathType = std::numeric_limits<uint32>::max();
        uint32 health = 0;
        Position position;
        Position navigationPosition;
    };

    std::unique_ptr<LocalOperation> localOperation;
    EventMap operationEvents;
    ObjectGuid botGuid;
    std::string botGuidToken;
    uint32 eventShard = 0;
    std::string ownerEpoch;
    std::string ownerToken;
    std::string subjectPrefix;
    std::shared_ptr<AgentBridgeInbox> inbox = std::make_shared<AgentBridgeInbox>();
    bool addedTravelStrategy = false;
    bool invitePending = false;
    std::atomic<uint64> tradeRevision{0};
    uint32 lastPassiveSnapshotMs = 0;
    std::atomic<bool> tradeWindowOpen{false};
    bool tradeMovementSuspended = false;
    WorldPacket invitePacket;
    std::mutex inviteMutex;
    bool initialized = false;
    std::atomic<bool> remoteControlActive{false};
    std::atomic<uint64> gatheringObservationTarget{0};
    std::mutex gatheringLocationMutex;
    ObjectGuid gatheringLocationGuid;
    std::string gatheringLocationJson;
    bool groupInitialized = false;
    bool sendingAgentChat = false;
    uint32 lastGroupMembers = 0;
    ObjectGuid lastGroupLeader;
    bool locationInitialized = false;
    uint32 lastMapId = 0;
    uint32 lastZoneId = 0;
    bool transportReady = false;
    uint32 lastSubscribeCheckMs = 0;
    uint32 lastHeartbeatMs = 0;
    bool stopped = false;
    uint64 eventSequence = 1;
    std::mutex outboundMutex;
    std::deque<std::string> pendingEvents;
    std::atomic<bool> hasPendingEvents{false};
    std::map<std::string, std::string> resultCache;
    std::deque<std::string> resultCacheOrder;

    explicit Impl(ObjectGuid guid)
        : botGuid(guid),
          botGuidToken(AgentBridgeTransport::BotToken(guid)),
          eventShard(AgentBridgeTransport::EventShard(botGuidToken)),
          ownerEpoch(std::to_string(
              std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                  .count())),
          ownerToken(AgentBridgeTransport::OwnerToken()),
          subjectPrefix(sPlayerbotAIConfig.agentBridgeSubjectPrefix)
    {
        AgentBridgeTransport::Register(botGuidToken, inbox);
    }

    void Publish(std::string const& type, std::string const& requestId, std::string const& payload)
    {
        std::lock_guard<std::mutex> lock(outboundMutex);
        std::string event = SerializeEvent(type, botGuid, ownerToken, ownerEpoch, eventSequence++, requestId, payload);
        if (event.size() > AGENT_MAX_EVENT_BYTES)
        {
            LOG_WARN("playerbots.agent", "Bridge event {} exceeded the size limit for bot {}", type,
                     botGuid.ToString().c_str());
            return;
        }

        if (!pendingEvents.empty() || !AgentBridgeTransport::Publish(subjectPrefix, ownerToken, botGuid, event))
        {
            if (pendingEvents.size() < AGENT_MAX_PENDING_EVENTS)
            {
                pendingEvents.push_back(std::move(event));
                hasPendingEvents.store(true, std::memory_order_release);
            }
            else
                LOG_WARN("playerbots.agent", "Bridge event buffer full; dropped {} for bot {}", type,
                         botGuid.ToString().c_str());
        }
    }

    void FlushEvents()
    {
        if (!hasPendingEvents.load(std::memory_order_acquire))
            return;
        std::lock_guard<std::mutex> lock(outboundMutex);
        uint32 flushed = 0;
        while (!pendingEvents.empty() && flushed < 8)
        {
            if (!AgentBridgeTransport::Publish(subjectPrefix, ownerToken, botGuid, pendingEvents.front()))
                return;
            pendingEvents.pop_front();
            ++flushed;
        }
        hasPendingEvents.store(!pendingEvents.empty(), std::memory_order_release);
    }

    void PublishResult(std::string const& requestId, std::string const& operationId, std::string const& operation,
                       std::string const& status, std::string const& reason,
                       ObjectGuid resultTarget = ObjectGuid::Empty, bool persistent = false)
    {
        if (status == "rejected")
            LOG_WARN("playerbots.agent", "Operation rejected bot={} request={} operation_id={} operation={} reason={}",
                     botGuid.ToString(), requestId, operationId, operation, reason);
        else
            LOG_DEBUG("playerbots.agent",
                      "Operation result bot={} request={} operation_id={} operation={} status={} reason={}",
                      botGuid.ToString(), requestId, operationId, operation, status, reason);
        std::ostringstream payload;
        payload << "{\"operation_id\":\"" << EscapeJson(operationId) << "\",\"operation\":\"" << EscapeJson(operation)
                << "\",\"status\":\"" << EscapeJson(status) << "\",\"reason\":\"" << EscapeJson(reason)
                << "\",\"target_guid\":\"" << EscapeJson(std::to_string(resultTarget.GetRawValue()))
                << "\",\"persistent\":" << (persistent ? "true" : "false") << "}";
        std::string const payloadJson = payload.str();
        if (!requestId.empty())
        {
            if (!resultCache.contains(requestId))
                resultCacheOrder.push_back(requestId);
            resultCache[requestId] = payloadJson;
            while (resultCacheOrder.size() > AGENT_MAX_PENDING_COMMANDS)
            {
                resultCache.erase(resultCacheOrder.front());
                resultCacheOrder.pop_front();
            }
        }
        Publish("operation_result", requestId, payloadJson);
    }

    void SuppressAutonomousStrategies(PlayerbotAI* botAI)
    {
        static std::array<std::string, 17> const strategies = {
            "new rpg", "rpg",        "travel", "move random", "follow", "grind", "quest", "gather", "pvp",
            "duel",    "start duel", "lfg",    "chat",        "emote",  "loot",  "group", "guild"};
        std::string changes;
        for (std::string const& strategy : strategies)
        {
            if (!botAI->HasStrategy(strategy, BOT_STATE_NON_COMBAT))
                continue;
            if (!changes.empty())
                changes += ",";
            changes += "-" + strategy;
        }
        if (!changes.empty())
            botAI->ChangeStrategy(changes, BOT_STATE_NON_COMBAT);
    }

    bool SetTravelTarget(PlayerbotAI* botAI, TravelDestination* destination, WorldPosition* point)
    {
        if (!destination || !point)
            return false;
        TravelTarget* target = botAI->GetAiObjectContext()->GetValue<TravelTarget*>("travel target")->Get();
        if (!target)
            return false;
        target->setTarget(destination, point);
        target->setForced(true);
        if (!botAI->HasStrategy("travel", BOT_STATE_NON_COMBAT))
        {
            botAI->ChangeStrategy("+travel", BOT_STATE_NON_COMBAT);
            addedTravelStrategy = true;
        }
        return true;
    }

    void ClearTravelTarget(PlayerbotAI* botAI)
    {
        TravelTarget* target = botAI->GetAiObjectContext()->GetValue<TravelTarget*>("travel target")->Get();
        if (target)
        {
            target->setTarget(TravelMgr::instance().nullTravelDestination, TravelMgr::instance().nullWorldPosition);
            target->setForced(false);
        }
        if (addedTravelStrategy && botAI->HasStrategy("travel", BOT_STATE_NON_COMBAT))
            botAI->ChangeStrategy("-travel", BOT_STATE_NON_COMBAT);
        addedTravelStrategy = false;
    }

    std::string BuildSnapshot(PlayerbotAI* botAI, uint32 craftingFocus = 0, uint32 recipeOffset = 0,
                              uint32 recipeItemId = 0)
    {
        Player* bot = botAI->GetBot();
        std::ostringstream result;
        result << "{\"schema_version\":1,\"bot\":{\"guid\":\"" << EscapeJson(AgentBridgeTransport::BotToken(botGuid))
               << "\",\"name\":\"" << EscapeJson(bot->GetName())
               << "\",\"class_id\":" << static_cast<uint32>(bot->getClass())
               << ",\"race_id\":" << static_cast<uint32>(bot->getRace())
               << ",\"team_id\":" << static_cast<uint32>(bot->GetTeamId())
               << ",\"level\":" << static_cast<uint32>(bot->GetLevel())
               << ",\"health_pct\":" << (bot->GetMaxHealth() ? bot->GetHealth() * 100 / bot->GetMaxHealth() : 0)
               << ",\"alive\":" << (bot->IsAlive() ? "true" : "false")
               << ",\"moving\":" << (bot->isMoving() ? "true" : "false")
               << ",\"can_move\":" << (botAI->CanMove() ? "true" : "false")
               << ",\"experience\":" << bot->GetUInt32Value(PLAYER_XP)
               << ",\"in_flight\":" << (bot->IsInFlight() ? "true" : "false")
               << ",\"in_combat\":" << (bot->IsInCombat() ? "true" : "false") << ",\"map_id\":" << bot->GetMapId()
               << ",\"zone_id\":" << bot->GetZoneId() << ",\"position\":[" << bot->GetPositionX() << ","
               << bot->GetPositionY() << "," << bot->GetPositionZ() << "]";

        Group* group = bot->GetGroup();
        result << ",\"group_size\":" << (group ? group->GetMembersCount() : 1) << ",\"group_leader_guid\":\""
               << EscapeJson(group ? AgentBridgeTransport::BotToken(group->GetLeaderGUID()) : "")
               << "\",\"pending_group_invite\":" << (bot->GetGroupInvite() ? "true" : "false")
               << ",\"group_members\":[";
        bool firstMember = true;
        if (group)
        {
            for (GroupReference* reference = group->GetFirstMember(); reference; reference = reference->next())
            {
                Player* member = reference->GetSource();
                if (!member)
                    continue;
                if (!firstMember)
                    result << ",";
                firstMember = false;
                result << "{\"guid\":\"" << EscapeJson(AgentBridgeTransport::BotToken(member->GetGUID()))
                       << "\",\"name\":\"" << EscapeJson(member->GetName())
                       << "\",\"level\":" << static_cast<uint32>(member->GetLevel())
                       << ",\"is_bot\":" << (GET_PLAYERBOT_AI(member) ? "true" : "false") << "}";
            }
        }
        Unit* currentTarget = botAI->GetAiObjectContext()->GetValue<Unit*>("current target")->Get();
        result << "],\"has_available_loot\":"
               << (botAI->GetAiObjectContext()->GetValue<bool>("has available loot")->Get() ? "true" : "false")
               << ",\"current_target\":";
        if (currentTarget)
        {
            LootObject currentLoot(bot, currentTarget->GetGUID());
            result << "{\"guid\":\"" << EscapeJson(std::to_string(currentTarget->GetGUID().GetRawValue()))
                   << "\",\"entry\":" << currentTarget->GetEntry() << ",\"name\":\""
                   << EscapeJson(currentTarget->GetName())
                   << "\",\"alive\":" << (currentTarget->IsAlive() ? "true" : "false") << ",\"health_pct\":"
                   << (currentTarget->GetMaxHealth() ? currentTarget->GetHealth() * 100 / currentTarget->GetMaxHealth()
                                                     : 0)
                   << ",\"loot_possible\":" << (currentLoot.IsLootPossible(bot) ? "true" : "false") << "}";
        }
        else
            result << "null";
        result << "}";

        TravelTarget* travelTarget = botAI->GetAiObjectContext()->GetValue<TravelTarget*>("travel target")->Get();
        TravelDestination* destination = travelTarget ? travelTarget->getDestination() : nullptr;
        if (travelTarget && destination && travelTarget->isActive())
        {
            WorldPosition* targetPosition = travelTarget->getPosition();
            WorldPosition botPosition(bot);
            result << ",\"travel_target\":{\"destination_name\":\"" << EscapeJson(destination->getName())
                   << "\",\"is_traveling\":" << (travelTarget->isTraveling() ? "true" : "false")
                   << ",\"is_working\":" << (travelTarget->isWorking() ? "true" : "false")
                   << ",\"arrived\":" << (destination->isIn(&botPosition) ? "true" : "false");
            if (targetPosition)
                result << ",\"position\":[" << targetPosition->GetPositionX() << "," << targetPosition->GetPositionY()
                       << "," << targetPosition->GetPositionZ() << "],\"map_id\":" << targetPosition->GetMapId();
            result << "}";
        }
        else
            result << ",\"travel_target\":null";

        result << ",\"professions\":{\"herbalism\":" << bot->GetSkillValue(SKILL_HERBALISM)
               << ",\"mining\":" << bot->GetSkillValue(SKILL_MINING)
               << ",\"fishing\":" << bot->GetSkillValue(SKILL_FISHING) << "},\"crafting_recipes\":[";
        std::vector<uint32> recipeSpells;
        std::set<uint32> craftingItems;
        if (craftingFocus)
            craftingItems.insert(craftingFocus);
        for (auto const& [spellId, learned] : bot->GetSpellMap())
        {
            SpellInfo const* info = sSpellMgr->GetSpellInfo(spellId);
            if (!learned || learned->State == PLAYERSPELL_REMOVED || !learned->Active || !info ||
                !info->HasAttribute(SPELL_ATTR0_IS_TRADESKILL))
                continue;
            uint32 outputId = 0;
            if (CraftOutput(bot, spellId, outputId))
                recipeSpells.push_back(spellId);
        }
        std::sort(recipeSpells.begin(), recipeSpells.end());
        uint32 const totalRecipes = uint32(recipeSpells.size());
        uint32 const pageOffset = std::min(recipeOffset, totalRecipes);
        bool dependenciesTruncated = false;
        if (recipeItemId)
        {
            std::map<uint32, uint32> recipeByOutput;
            for (uint32 const spellId : recipeSpells)
            {
                uint32 outputId = 0;
                if (CraftOutput(bot, spellId, outputId))
                    recipeByOutput.try_emplace(outputId, spellId);  // Lowest spell ID wins, as in the planner.
            }
            std::vector<uint32> selected;
            std::deque<std::pair<uint32, uint32>> pending{{recipeItemId, 0}};
            std::set<uint32> visited;
            while (!pending.empty())
            {
                auto const [itemId, depth] = pending.front();
                pending.pop_front();
                if (!visited.insert(itemId).second)
                    continue;
                auto const known = recipeByOutput.find(itemId);
                if (known == recipeByOutput.end())
                    continue;  // External materials are not invented as recipes.
                if (selected.size() >= AGENT_MAX_CRAFTING_RECIPES || depth >= AGENT_MAX_CRAFTING_QUERY_DEPTH)
                {
                    dependenciesTruncated = true;
                    continue;
                }
                selected.push_back(known->second);
                SpellInfo const* info = sSpellMgr->GetSpellInfo(known->second);
                for (uint32 index = 0; index < MAX_SPELL_REAGENTS; ++index)
                    if (info->Reagent[index] > 0 && info->ReagentCount[index])
                        pending.emplace_back(uint32(info->Reagent[index]), depth + 1);
            }
            recipeSpells = std::move(selected);
            craftingItems.insert(recipeItemId);
        }
        else
        {
            uint32 const end = std::min(totalRecipes, pageOffset + AGENT_MAX_CRAFTING_RECIPES);
            recipeSpells = std::vector<uint32>(recipeSpells.begin() + pageOffset, recipeSpells.begin() + end);
        }
        uint32 recipeCount = 0;
        for (uint32 const spellId : recipeSpells)
        {
            SpellInfo const* info = sSpellMgr->GetSpellInfo(spellId);
            for (SpellEffectInfo const& effect : info->GetEffects())
            {
                if (!effect.IsEffect(SPELL_EFFECT_CREATE_ITEM) || !effect.ItemType ||
                    (effect.DieSides != 0 && effect.DieSides != 1) || effect.RealPointsPerLevel != 0.0f)
                    continue;  // Random/level-scaled outputs need a separate planning policy.
                ItemTemplate const* output = sObjectMgr->GetItemTemplate(effect.ItemType);
                if (!output)
                    continue;
                uint32 const yield =
                    std::min(output->GetMaxStackSize(), uint32(std::max(1, effect.BasePoints + effect.DieSides)));
                craftingItems.insert(effect.ItemType);
                if (recipeCount)
                    result << ",";
                result << "{\"spell_id\":" << spellId << ",\"item_id\":" << effect.ItemType << ",\"name\":\""
                       << EscapeJson(output->Name1) << "\",\"yield\":" << yield << ",\"reagents\":[";
                bool firstReagent = true;
                for (uint32 index = 0; index < MAX_SPELL_REAGENTS; ++index)
                {
                    if (info->Reagent[index] <= 0 || !info->ReagentCount[index])
                        continue;
                    craftingItems.insert(uint32(info->Reagent[index]));
                    if (!firstReagent)
                        result << ",";
                    firstReagent = false;
                    result << "{\"item_id\":" << info->Reagent[index] << ",\"count\":" << info->ReagentCount[index]
                           << "}";
                }
                result << "]}";
                ++recipeCount;
            }
            if (recipeCount >= AGENT_MAX_CRAFTING_RECIPES)
                break;
        }
        result << "],\"crafting_recipe_page\":{\"offset\":" << (recipeItemId ? 0 : pageOffset)
               << ",\"total\":" << totalRecipes << ",\"item_id\":" << recipeItemId
               << ",\"dependencies_truncated\":" << (dependenciesTruncated ? "true" : "false") << ",\"next_offset\":";
        if (!recipeItemId && pageOffset + recipeCount < totalRecipes)
            result << pageOffset + recipeCount;
        else
            result << "null";
        result << "},\"inventory\":{\"money_copper\":" << bot->GetMoney() << ",\"items\":[";
        std::map<uint32, uint32> itemCounts;
        for (Item* item : botAI->GetInventoryItems())
            if (item)
                itemCounts[item->GetEntry()] += item->GetCount();
        bool firstItem = true;
        uint32 inventoryCount = 0;
        for (auto const& [itemId, count] : itemCounts)
        {
            if (!firstItem)
                result << ",";
            firstItem = false;
            ItemTemplate const* itemTemplate = sObjectMgr->GetItemTemplate(itemId);
            result << "{\"item_id\":" << itemId << ",\"count\":" << count << ",\"name\":\""
                   << EscapeJson(itemTemplate ? itemTemplate->Name1 : "unknown") << "\"}";
            if (++inventoryCount >= AGENT_MAX_SNAPSHOT_ITEMS)
                break;
        }
        result << "],\"tradeable_stacks\":[";
        bool firstStack = true;
        uint32 stackCount = 0;
        for (Item* item : botAI->GetInventoryItems())
            if (item && !item->IsEquipped() && item->CanBeTraded())
            {
                if (!firstStack)
                    result << ",";
                firstStack = false;
                result << "{\"item_guid\":\"" << item->GetGUID().GetRawValue() << "\",\"item_id\":" << item->GetEntry()
                       << ",\"count\":" << item->GetCount() << ",\"name\":\"" << EscapeJson(item->GetTemplate()->Name1)
                       << "\"}";
                if (++stackCount >= AGENT_MAX_SNAPSHOT_ITEMS)
                    break;
            }
        result << "]}";

        result << ",\"crafting_inventory\":[";
        bool firstCraftingItem = true;
        for (uint32 const itemId : craftingItems)
        {
            if (!firstCraftingItem)
                result << ",";
            firstCraftingItem = false;
            result << "{\"item_id\":" << itemId << ",\"count\":" << itemCounts[itemId] << "}";
        }
        result << "]";

        result << ",\"trade\":{\"revision\":" << tradeRevision.load()
               << ",\"window_open\":" << (bot->GetTradeData() && tradeWindowOpen.load() ? "true" : "false");
        if (TradeData* trade = bot->GetTradeData())
        {
            Player* partner = bot->GetTrader();
            result << ",\"active\":true,\"partner_guid\":\""
                   << EscapeJson(partner ? std::to_string(partner->GetGUID().GetRawValue()) : "")
                   << "\",\"partner_name\":\"" << EscapeJson(partner ? partner->GetName() : "") << "\"";
            auto writeOffer = [&](char const* name, TradeData* offer)
            {
                result << ",\"" << name << "\":{\"money_copper\":" << (offer ? offer->GetMoney() : 0)
                       << ",\"spell_id\":" << (offer ? offer->GetSpell() : 0)
                       << ",\"accepted\":" << (offer && offer->IsAccepted() ? "true" : "false") << ",\"items\":[";
                bool first = true;
                if (offer)
                    for (uint8 slot = 0; slot < TRADE_SLOT_COUNT; ++slot)
                        if (Item* item = offer->GetItem(TradeSlots(slot)))
                        {
                            if (!first)
                                result << ",";
                            first = false;
                            result << "{\"slot\":" << static_cast<uint32>(slot) << ",\"item_id\":" << item->GetEntry()
                                   << ",\"count\":" << item->GetCount() << ",\"name\":\""
                                   << EscapeJson(item->GetTemplate()->Name1) << "\"}";
                        }
                result << "]}";
            };
            writeOffer("bot_offer", trade);
            writeOffer("partner_offer", trade->GetTraderData());
        }
        else
            result << ",\"active\":false";
        result << "}";

        result << ",\"quests\":[";
        bool first = true;
        for (auto const& [questId, status] : bot->getQuestStatusMap())
        {
            if (status.Status != QUEST_STATUS_INCOMPLETE && status.Status != QUEST_STATUS_COMPLETE)
                continue;
            Quest const* quest = sObjectMgr->GetQuestTemplate(questId);
            if (!quest)
                continue;
            if (!first)
                result << ",";
            first = false;
            result << "{\"quest_id\":" << questId << ",\"title\":\"" << EscapeJson(quest->GetTitle())
                   << "\",\"status\":" << static_cast<uint32>(status.Status) << ",\"status_name\":\""
                   << (status.Status == QUEST_STATUS_COMPLETE ? "complete" : "incomplete")
                   << "\",\"type\":" << quest->GetType() << ",\"suggested_players\":" << quest->GetSuggestedPlayers()
                   << ",\"objective_navigation_blocked_reason\":\""
                   << EscapeJson(status.Status == QUEST_STATUS_INCOMPLETE
                                     ? QuestObjectiveNavigationBlockReason(bot, quest)
                                     : "")
                   << "\",\"objectives\":[";
            bool firstObjective = true;
            for (uint32 index = 0; index < QUEST_OBJECTIVES_COUNT; ++index)
            {
                if (!quest->RequiredNpcOrGo[index])
                    continue;
                if (!firstObjective)
                    result << ",";
                firstObjective = false;
                result << "{\"index\":" << index << ",\"entry\":" << quest->RequiredNpcOrGo[index]
                       << ",\"count\":" << status.CreatureOrGOCount[index]
                       << ",\"required\":" << quest->RequiredNpcOrGoCount[index] << "}";
            }
            for (uint32 index = 0; index < QUEST_ITEM_OBJECTIVES_COUNT; ++index)
            {
                if (!quest->RequiredItemId[index])
                    continue;
                if (!firstObjective)
                    result << ",";
                firstObjective = false;
                result << "{\"index\":" << index << ",\"item_id\":" << quest->RequiredItemId[index]
                       << ",\"count\":" << status.ItemCount[index]
                       << ",\"required\":" << quest->RequiredItemCount[index] << ",\"sources\":[";
                auto const source = TravelMgr::instance().questItemSources.find(quest->RequiredItemId[index]);
                bool truncated = false;
                if (source != TravelMgr::instance().questItemSources.end())
                {
                    std::vector<int32> const& sources = source->second;
                    truncated = sources.size() > AGENT_MAX_QUEST_ITEM_SOURCES;
                    for (size_t sourceIndex = 0;
                         sourceIndex < std::min<size_t>(sources.size(), AGENT_MAX_QUEST_ITEM_SOURCES); ++sourceIndex)
                    {
                        if (sourceIndex)
                            result << ",";
                        result << sources[sourceIndex];
                    }
                }
                result << "],\"sources_truncated\":" << (truncated ? "true" : "false") << "}";
            }
            result << "]}";
        }
        result << "],\"nearby_creatures\":[";
        GuidVector creatures = botAI->GetAiObjectContext()->GetValue<GuidVector>("possible targets")->Get();
        bool firstCreature = true;
        uint32 creatureCount = 0;
        for (ObjectGuid const guid : creatures)
        {
            Creature* creature = botAI->GetCreature(guid);
            if (!creature || !creature->IsAlive() || !bot->IsValidAttackTarget(creature))
                continue;
            if (!firstCreature)
                result << ",";
            firstCreature = false;
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue()))
                   << "\",\"entry\":" << creature->GetEntry() << ",\"name\":\"" << EscapeJson(creature->GetName())
                   << "\",\"health_pct\":"
                   << (creature->GetMaxHealth() ? creature->GetHealth() * 100 / creature->GetMaxHealth() : 0)
                   << ",\"distance\":" << bot->GetDistance(creature) << ",\"position\":[" << creature->GetPositionX()
                   << "," << creature->GetPositionY() << "," << creature->GetPositionZ() << "]}";
            if (++creatureCount >= AGENT_MAX_SNAPSHOT_CREATURES)
                break;
        }
        result << "],\"nearby_corpses\":[";
        GuidVector corpses = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest corpses")->Get();
        bool firstCorpse = true;
        uint32 corpseCount = 0;
        for (ObjectGuid const guid : corpses)
        {
            Creature* corpse = botAI->GetCreature(guid);
            if (!corpse || !corpse->IsInWorld() || corpse->IsAlive() || corpse->GetMap() != bot->GetMap() ||
                !corpse->HasFlag(UNIT_DYNAMIC_FLAGS, UNIT_DYNFLAG_LOOTABLE) || !bot->isAllowedToLoot(corpse))
                continue;
            LootObject loot(bot, guid);
            if (!loot.IsLootPossible(bot))
                continue;
            if (!firstCorpse)
                result << ",";
            firstCorpse = false;
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue())) << "\",\"name\":\""
                   << EscapeJson(corpse->GetName()) << "\",\"distance\":" << bot->GetDistance(corpse)
                   << ",\"position\":[" << corpse->GetPositionX() << "," << corpse->GetPositionY() << ","
                   << corpse->GetPositionZ() << "]}";
            if (++corpseCount >= AGENT_MAX_SNAPSHOT_CORPSES)
                break;
        }
        result << "],\"nearby_game_objects\":[";
        GuidVector gameObjects = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest game objects")->Get();
        bool firstObject = true;
        uint32 objectCount = 0;
        for (ObjectGuid const guid : gameObjects)
        {
            GameObject* object = botAI->GetGameObject(guid);
            if (!object || !object->isSpawned())
                continue;
            LootObject loot(bot, guid);
            if (!firstObject)
                result << ",";
            firstObject = false;
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue()))
                   << "\",\"entry\":" << object->GetEntry() << ",\"name\":\"" << EscapeJson(object->GetName())
                   << "\",\"gather_skill\":" << loot.skillId
                   << ",\"can_gather\":" << (loot.IsLootPossible(bot) ? "true" : "false")
                   << ",\"distance\":" << bot->GetDistance(object) << ",\"position\":[" << object->GetPositionX() << ","
                   << object->GetPositionY() << "," << object->GetPositionZ() << "]}";
            if (++objectCount >= AGENT_MAX_SNAPSHOT_GAMEOBJECTS)
                break;
        }
        result << "],\"nearby_mailboxes\":[";
        uint32 mailboxCount = 0;
        for (ObjectGuid const guid : gameObjects)
        {
            GameObject* mailbox = botAI->GetGameObject(guid);
            if (!mailbox || !mailbox->isSpawned() || mailbox->GetGoType() != GAMEOBJECT_TYPE_MAILBOX)
                continue;
            if (mailboxCount)
                result << ",";
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue())) << "\",\"name\":\""
                   << EscapeJson(mailbox->GetName()) << "\",\"distance\":" << bot->GetDistance(mailbox) << "}";
            if (++mailboxCount >= AGENT_MAX_SNAPSHOT_GAMEOBJECTS)
                break;
        }
        result << "],\"nearby_players\":[";
        GuidVector players = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest friendly players")->Get();
        bool firstPlayer = true;
        uint32 playerCount = 0;
        for (ObjectGuid const guid : players)
        {
            Player* player = ObjectAccessor::FindPlayer(guid);
            if (!player || player == bot || player->GetMapId() != bot->GetMapId())
                continue;
            if (!firstPlayer)
                result << ",";
            firstPlayer = false;
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue())) << "\",\"guid_raw\":\""
                   << EscapeJson(std::to_string(guid.GetRawValue())) << "\",\"name\":\""
                   << EscapeJson(player->GetName()) << "\",\"level\":" << static_cast<uint32>(player->GetLevel())
                   << ",\"distance\":" << bot->GetDistance(player) << ",\"map_id\":" << player->GetMapId()
                   << ",\"position\":[" << player->GetPositionX() << "," << player->GetPositionY() << ","
                   << player->GetPositionZ() << "],\"is_bot\":" << (GET_PLAYERBOT_AI(player) ? "true" : "false")
                   << ",\"in_group\":" << (player->GetGroup() ? "true" : "false") << "}";
            if (++playerCount >= AGENT_MAX_SNAPSHOT_PLAYERS)
                break;
        }
        result << "],\"nearby_npcs\":[";
        GuidVector npcGuids = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest npcs")->Get();
        bool firstNpc = true;
        uint32 npcCount = 0;
        for (ObjectGuid const guid : npcGuids)
        {
            Creature* creature = botAI->GetCreature(guid);
            if (!creature || !creature->IsAlive())
                continue;
            if (!firstNpc)
                result << ",";
            firstNpc = false;
            result << "{\"guid\":\"" << EscapeJson(std::to_string(guid.GetRawValue()))
                   << "\",\"entry\":" << creature->GetEntry() << ",\"name\":\"" << EscapeJson(creature->GetName())
                   << "\",\"npc_flags\":" << creature->GetNpcFlags()
                   << ",\"quest_giver\":" << (creature->HasNpcFlag(UNIT_NPC_FLAG_QUESTGIVER) ? "true" : "false")
                   << ",\"distance\":" << bot->GetDistance(creature);
            Trainer::Trainer* trainer =
                creature->HasNpcFlag(UNIT_NPC_FLAG_TRAINER) ? sObjectMgr->GetTrainer(creature->GetEntry()) : nullptr;
            bool const classTrainer =
                trainer && trainer->GetTrainerType() == Trainer::Type::Class && trainer->IsTrainerValidForPlayer(bot);
            result << ",\"class_trainer\":" << (classTrainer ? "true" : "false") << ",\"vendor_offers\":[";
            VendorItemData const* offers =
                creature->HasNpcFlag(UNIT_NPC_FLAG_VENDOR) ? creature->GetVendorItems() : nullptr;
            uint32 publishedOffers = 0;
            if (offers)
                for (uint32 slot = 0; slot < offers->GetItemCount(); ++slot)
                {
                    VendorItem const* offer = offers->GetItem(slot);
                    if (!offer || offer->ExtendedCost)
                        continue;
                    ItemTemplate const* itemTemplate = sObjectMgr->GetItemTemplate(offer->item);
                    if (!itemTemplate)
                        continue;
                    if (publishedOffers)
                        result << ",";
                    uint32 const price =
                        uint32(std::floor(itemTemplate->BuyPrice * bot->GetReputationPriceDiscount(creature)));
                    result << "{\"item_id\":" << offer->item << ",\"name\":\"" << EscapeJson(itemTemplate->Name1)
                           << "\",\"bundle_count\":" << itemTemplate->BuyCount << ",\"price_copper\":" << price << "}";
                    if (++publishedOffers >= AGENT_MAX_VENDOR_OFFERS_PER_NPC)
                        break;
                }
            uint32 const taxiNode =
                creature->HasNpcFlag(UNIT_NPC_FLAG_FLIGHTMASTER)
                    ? sObjectMgr->GetNearestTaxiNode(creature->GetPositionX(), creature->GetPositionY(),
                                                     creature->GetPositionZ(), creature->GetMapId(), bot->GetTeamId())
                    : 0;
            result << "],\"taxi_node_id\":" << taxiNode << ",\"taxi_routes\":[";
            auto const sourceRoutes = sTaxiPathSetBySource.find(taxiNode);
            uint32 publishedRoutes = 0;
            if (sourceRoutes != sTaxiPathSetBySource.end())
                for (auto const& [destinationId, route] : sourceRoutes->second)
                {
                    TaxiNodesEntry const* destination = sTaxiNodesStore.LookupEntry(destinationId);
                    if (!route || !destination || !bot->m_taxi.IsTaximaskNodeKnown(destinationId))
                        continue;
                    if (publishedRoutes)
                        result << ",";
                    uint32 const fare = uint32(std::ceil(route->price * bot->GetReputationPriceDiscount(creature)));
                    result << "{\"to_node\":" << destinationId << ",\"name\":\"" << EscapeJson(destination->name[0])
                           << "\",\"price_copper\":" << fare << "}";
                    if (++publishedRoutes >= AGENT_MAX_VENDOR_OFFERS_PER_NPC)
                        break;
                }
            result << "]}";
            if (++npcCount >= AGENT_MAX_SNAPSHOT_PLAYERS)
                break;
        }
        result << "],\"available_quests\":[";
        uint32 publishedQuests = 0;
        bool firstAvailable = true;
        for (ObjectGuid const guid : npcGuids)
        {
            if (publishedQuests >= AGENT_MAX_SNAPSHOT_AVAILABLE_QUESTS)
                break;
            Creature* creature = botAI->GetCreature(guid);
            if (!creature || !creature->IsAlive() || !creature->HasNpcFlag(UNIT_NPC_FLAG_QUESTGIVER))
                continue;
            bot->PrepareQuestMenu(guid);
            QuestMenu& questMenu = bot->PlayerTalkClass->GetQuestMenu();
            uint32 fromThisGiver = 0;
            for (uint32 i = 0; i < questMenu.GetMenuItemCount(); ++i)
            {
                if (fromThisGiver >= AGENT_MAX_AVAILABLE_QUESTS_PER_GIVER ||
                    publishedQuests >= AGENT_MAX_SNAPSHOT_AVAILABLE_QUESTS)
                    break;
                uint32 const menuQuestId = questMenu.GetItem(i).QuestId;
                Quest const* menuQuest = sObjectMgr->GetQuestTemplate(menuQuestId);
                if (!menuQuest || bot->GetQuestStatus(menuQuestId) != QUEST_STATUS_NONE ||
                    !bot->CanTakeQuest(menuQuest, false))
                    continue;
                if (!firstAvailable)
                    result << ",";
                firstAvailable = false;
                result << "{\"quest_id\":" << menuQuestId << ",\"title\":\"" << EscapeJson(menuQuest->GetTitle())
                       << "\",\"giver_name\":\"" << EscapeJson(creature->GetName()) << "\",\"giver_guid\":\""
                       << EscapeJson(std::to_string(guid.GetRawValue())) << "\"}";
                ++publishedQuests;
                ++fromThisGiver;
            }
        }
        result << "]}";
        return result.str();
    }

    void PublishSnapshot(PlayerbotAI* botAI, AgentBridgeCommand const& command, uint32 craftingFocus = 0,
                         uint32 recipeOffset = 0, uint32 recipeItemId = 0)
    {
        Publish("snapshot", command.requestId, BuildSnapshot(botAI, craftingFocus, recipeOffset, recipeItemId));
    }

    void ClearOperation(PlayerbotAI* botAI)
    {
        gatheringObservationTarget.store(0, std::memory_order_release);
        if (localOperation && localOperation->mailboxWork)
            localOperation->mailboxWork->Cancelled.store(true);
        if (localOperation && localOperation->auctionWork)
            localOperation->auctionWork->Cancelled.store(true);
        if (localOperation && localOperation->materialTradeWork)
            localOperation->materialTradeWork->Cancelled.store(true);
        if (localOperation && localOperation->command.operation == "fish_once")
            botAI->GetBot()->InterruptNonMeleeSpells(false, FISHING_SPELL);
        if (localOperation && localOperation->command.operation == "craft_once" && localOperation->attempted)
            botAI->GetBot()->InterruptNonMeleeSpells(false, localOperation->serviceId);
        localOperation.reset();
        operationEvents.Reset();
        ClearTravelTarget(botAI);
        if (!botAI->GetBot()->IsInCombat() && !botAI->GetBot()->IsInFlight())
        {
            botAI->GetBot()->GetMotionMaster()->Clear();
            botAI->GetBot()->StopMoving();
        }
    }

    void StartOperation(PlayerbotAI* botAI, AgentBridgeCommand const& command, ObjectGuid target = ObjectGuid::Empty,
                        float distance = 2.0f, uint32 questId = 0, bool corpseOnly = false)
    {
        // Task replacement sends cancel before start. Clear the old local owner
        // here as well, so redelivery cannot leave competing movement loops.
        if (localOperation && localOperation->mailboxWork)
            localOperation->mailboxWork->Cancelled.store(true);
        if (localOperation && localOperation->auctionWork)
            localOperation->auctionWork->Cancelled.store(true);
        if (localOperation && localOperation->materialTradeWork)
            localOperation->materialTradeWork->Cancelled.store(true);
        localOperation = std::make_unique<LocalOperation>();
        localOperation->command = command;
        localOperation->target = target;
        gatheringObservationTarget.store(command.operation == "gather_target" ? target.GetRawValue() : 0,
                                         std::memory_order_release);
        if (command.operation == "gather_target")
        {
            WorldObject* source = target.IsGameObject() ? static_cast<WorldObject*>(botAI->GetGameObject(target))
                                                        : static_cast<WorldObject*>(botAI->GetUnit(target));
            std::ostringstream location;
            if (source)
                location << ",\"source_guid\":\"" << target.GetRawValue() << "\",\"map_id\":" << source->GetMapId()
                         << ",\"position\":[" << source->GetPositionX() << "," << source->GetPositionY() << ","
                         << source->GetPositionZ() << "]";
            std::lock_guard<std::mutex> guard(gatheringLocationMutex);
            gatheringLocationGuid = target;
            gatheringLocationJson = location.str();
        }
        localOperation->mapId = botAI->GetBot()->GetMapId();
        localOperation->distance = distance;
        localOperation->questId = questId;
        localOperation->corpseOnly = corpseOnly;
        localOperation->position.Relocate(botAI->GetBot());
        localOperation->lastProgressMs = getMSTime();
        localOperation->recoveryStartedMs = localOperation->lastProgressMs;
        operationEvents.Reset();
        operationEvents.ScheduleEvent(AGENT_OPERATION_EVENT, Milliseconds(1));
        PublishResult(command.requestId, command.operationId, command.operation, "accepted", "local loop started",
                      target, true);
    }

    void FinishOperation(PlayerbotAI* botAI, bool success, std::string const& reason)
    {
        AgentBridgeCommand const command = localOperation->command;
        ObjectGuid const target = localOperation->target;
        ClearOperation(botAI);
        PublishResult(command.requestId, command.operationId, command.operation, success ? "completed" : "rejected",
                      reason, target, true);
        // Terminal events drive task transitions; the LLM timer does not pace them.
        Publish("snapshot", "", BuildSnapshot(botAI));
    }

    static bool IsNPCService(std::string const& operation)
    {
        return operation == "repair_equipment" || operation == "sell_junk" || operation == "buy_vendor_item" ||
               operation == "train_class_spells" || operation == "discover_flight_path" || operation == "take_flight" ||
               operation == "inspect_auctions" || operation == "buy_auction" || operation == "bid_auction";
    }

    static std::map<uint32, uint32> InventoryCounts(PlayerbotAI* botAI)
    {
        std::map<uint32, uint32> counts;
        for (Item* item : botAI->GetInventoryItems())
            if (item)
                counts[item->GetEntry()] += item->GetCount();
        return counts;
    }

    static bool CraftOutput(Player* bot, uint32 spellId, uint32& itemId)
    {
        SpellInfo const* info = sSpellMgr->GetSpellInfo(spellId);
        auto const learned = bot->GetSpellMap().find(spellId);
        if (!info || learned == bot->GetSpellMap().end() || !learned->second ||
            learned->second->State == PLAYERSPELL_REMOVED || !learned->second->Active || !bot->HasSpell(spellId) ||
            !info->HasAttribute(SPELL_ATTR0_IS_TRADESKILL))
            return false;
        uint32 outputs = 0;
        for (SpellEffectInfo const& effect : info->GetEffects())
        {
            if (effect.IsEffect(SPELL_EFFECT_CREATE_RANDOM_ITEM))
                return false;
            if (!effect.IsEffect(SPELL_EFFECT_CREATE_ITEM))
                continue;
            if (!effect.ItemType || (effect.DieSides != 0 && effect.DieSides != 1) || effect.RealPointsPerLevel != 0.0f)
                return false;
            itemId = effect.ItemType;
            ++outputs;
        }
        return outputs == 1 && sObjectMgr->GetItemTemplate(itemId);
    }

    void UpdateCrafting(PlayerbotAI* botAI, uint32 now)
    {
        Player* bot = botAI->GetBot();
        LocalOperation& loop = *localOperation;
        if (loop.attempted)
        {
            if (bot->IsNonMeleeSpellCast(false))
            {
                if (getMSTimeDiff(loop.lastInteractionMs, now) >= AGENT_CRAFTING_CAST_TIMEOUT_MS)
                    FinishOperation(botAI, false, "crafting cast timed out; inspect inventory");
                return;
            }
            if (bot->GetItemCount(loop.craftItemId, false) > loop.craftItemsBefore)
                FinishOperation(botAI, true, "crafted output acquired");
            else
                FinishOperation(botAI, false, "crafting cast ended without acquired output");
            return;
        }
        if (!botAI->CanMove() || bot->IsNonMeleeSpellCast(false))
        {
            if (getMSTimeDiff(loop.lastProgressMs, now) >= AGENT_OPERATION_STALL_MS)
                FinishOperation(botAI, false, "crafting could not start");
            return;
        }
        if (bot->isMoving())
        {
            bot->GetMotionMaster()->Clear();
            bot->StopMoving();
            return;
        }
        uint32 itemId = 0;
        if (!CraftOutput(bot, loop.serviceId, itemId) || itemId != loop.craftItemId)
        {
            FinishOperation(botAI, false, "crafting recipe changed or unavailable");
            return;
        }
        SpellInfo const* info = sSpellMgr->GetSpellInfo(loop.serviceId);
        std::map<uint32, uint64> required;
        for (uint32 index = 0; index < MAX_SPELL_REAGENTS; ++index)
            if (info->Reagent[index] > 0 && info->ReagentCount[index])
                required[uint32(info->Reagent[index])] += info->ReagentCount[index];
        for (auto const& [reagentId, quantity] : required)
            if (bot->GetItemCount(reagentId, false) < quantity)
            {
                FinishOperation(botAI, false, "crafting materials are unavailable");
                return;
            }
        loop.craftItemsBefore = bot->GetItemCount(itemId, false);
        loop.lastInteractionMs = now;
        loop.attempted = botAI->CastSpell(loop.serviceId, bot);
        if (!loop.attempted)
            FinishOperation(botAI, false, "native crafting cast failed");
    }

    void UpdateFishing(PlayerbotAI* botAI, uint32 now)
    {
        Player* bot = botAI->GetBot();
        LocalOperation& loop = *localOperation;
        if (!loop.attempted)
        {
            if (bot->isMoving())
            {
                bot->GetMotionMaster()->Clear();
                bot->StopMoving();
                return;
            }
            if (!botAI->CanMove() || bot->IsNonMeleeSpellCast(false))
                return;
            WorldPosition water = FindWaterRadial(bot, bot->GetPositionX(), bot->GetPositionY(), bot->GetPositionZ(),
                                                  bot->GetMap(), bot->GetPhaseMask(), 10.0f, 20.0f, 2.5f, true, 16);
            if (!water.IsValid())
            {
                FinishOperation(botAI, false, "no fishable water within casting range");
                return;
            }
            bot->SetFacingTo(bot->GetAngle(water.GetPositionX(), water.GetPositionY()));
            if (!botAI->CastSpell(FISHING_SPELL, bot))
            {
                FinishOperation(botAI, false, "fishing cast failed");
                return;
            }
            loop.attempted = true;
            loop.fishingStartedMs = now;
            loop.lastProgressMs = now;
            return;
        }
        if (loop.fishingReeled && getMSTimeDiff(loop.lastInteractionMs, now) >= 1000)
        {
            loop.lastInteractionMs = now;
            for (uint32 const itemId : loop.fishingLootItems)
                if (bot->GetItemCount(itemId, false) > loop.fishingItemsBefore[itemId])
                {
                    FinishOperation(botAI, true, "fishing catch acquired");
                    return;
                }
        }
        if (getMSTimeDiff(loop.fishingStartedMs, now) >= AGENT_FISHING_CAST_TIMEOUT_MS)
        {
            FinishOperation(botAI, false, "fishing cast timed out without acquired loot");
            return;
        }
        if (loop.fishingReeled)
            return;  // Loot response is asynchronous; do not recast or count a click as a catch.
        GameObject* bobber = bot->GetGameObject(FISHING_SPELL);
        if (!bobber)
        {
            if (getMSTimeDiff(loop.fishingStartedMs, now) >= AGENT_FISHING_BOBBER_SPAWN_MS)
                FinishOperation(botAI, false, "fishing bobber disappeared or failed to spawn");
            return;
        }
        if (bobber->GetOwnerGUID() != bot->GetGUID() || bobber->GetGoType() != GAMEOBJECT_TYPE_FISHINGNODE)
        {
            FinishOperation(botAI, false, "fishing bobber owner or type mismatch");
            return;
        }
        if (bobber->getLootState() == GO_READY)
        {
            loop.fishingItemsBefore = InventoryCounts(botAI);
            loop.fishingReeled = true;
            loop.lastProgressMs = now;
            ObjectGuid const bobberGuid = bobber->GetGUID();
            bobber->Use(bot);
            // A pool can own the loot instead of the bobber. Capture the actual
            // native loot source, so an unrelated inventory gain is not a catch.
            GameObject* source = botAI->GetGameObject(bot->GetLootGUID());
            if (source && (source->GetGUID() == bobberGuid || source->GetGoType() == GAMEOBJECT_TYPE_FISHINGHOLE))
            {
                for (LootItem const& item : source->loot.items)
                    loop.fishingLootItems.push_back(item.itemid);
                for (LootItem const& item : source->loot.quest_items)
                    loop.fishingLootItems.push_back(item.itemid);
            }
            if (loop.fishingLootItems.empty())
                FinishOperation(botAI, false, "fishing produced no collectible loot");
        }
    }

    static NPCFlags NPCServiceFlag(std::string const& operation)
    {
        if (operation == "inspect_auctions" || operation == "buy_auction" || operation == "bid_auction")
            return UNIT_NPC_FLAG_AUCTIONEER;
        if (operation == "discover_flight_path" || operation == "take_flight")
            return UNIT_NPC_FLAG_FLIGHTMASTER;
        if (operation == "repair_equipment")
            return UNIT_NPC_FLAG_REPAIR;
        if (operation == "train_class_spells")
            return UNIT_NPC_FLAG_TRAINER;
        return UNIT_NPC_FLAG_VENDOR;
    }

    void UpdateMailbox(PlayerbotAI* botAI, uint32 now)
    {
        LocalOperation& loop = *localOperation;
        if (loop.mailboxWork)
        {
            bool completed, success, empty;
            uint32 auctionOutcome;
            {
                std::lock_guard<std::mutex> guard(loop.mailboxWork->Mutex);
                completed = loop.mailboxWork->Completed;
                success = loop.mailboxWork->Success;
                empty = loop.mailboxWork->Empty;
                auctionOutcome = loop.mailboxWork->AuctionOutcome;
            }
            if (!completed)
                return;
            loop.mailboxWork.reset();
            if (!success)
            {
                FinishOperation(botAI, false,
                                "mail collection was not verified; mailbox, capacity, or funds unavailable");
                return;
            }
            if (loop.command.operation == "send_mail")
            {
                FinishOperation(botAI, true, "native mail submission verified; recipient delivery remains pending");
                return;
            }
            if (empty)
            {
                FinishOperation(botAI, true, "no delivered non-COD attachments or money remain");
                return;
            }
            if (loop.auctionMailFilter.AuctionId)
            {
                std::ostringstream payload;
                payload << "{\"operation_id\":\"" << EscapeJson(loop.command.operationId)
                        << "\",\"auction_id\":" << loop.auctionMailFilter.AuctionId
                        << ",\"item_id\":" << loop.auctionMailFilter.ItemId
                        << ",\"count\":" << loop.auctionMailFilter.Count
                        << ",\"bid_copper\":" << loop.auctionMailFilter.Bid << ",\"outcome\":" << auctionOutcome << "}";
                Publish("auction_proceeds", loop.command.requestId, payload.str());
                FinishOperation(botAI, auctionOutcome != 0, "auction proceeds collected");
                return;
            }
            ++loop.serviceCompleted;
            loop.lastProgressMs = now;
        }
        if (loop.serviceCompleted >= loop.serviceLimit)
        {
            FinishOperation(botAI, true, "bounded mailbox collection completed");
            return;
        }
        if (getMSTimeDiff(loop.lastInteractionMs, now) < 1000)
            return;
        loop.lastInteractionMs = now;
        loop.mailboxWork = std::make_shared<AgentMailboxWork>();
        if (loop.command.operation == "send_mail")
        {
            if (!PlayerbotWorldThreadProcessor::instance().QueueOperation(std::make_unique<AgentMailboxOperation>(
                    botAI->GetBot()->GetGUID(), loop.target, loop.mailboxWork, loop.mailRecipient, loop.mailSubject,
                    loop.mailBody, loop.mailItemGuid, loop.mailMoney, loop.serviceBudget)))
                FinishOperation(botAI, false, "world-thread mailbox queue unavailable");
            return;
        }
        if (!PlayerbotWorldThreadProcessor::instance().QueueOperation(std::make_unique<AgentMailboxOperation>(
                botAI->GetBot()->GetGUID(), loop.target, loop.mailboxWork, loop.mailItemGuid, loop.auctionMailFilter)))
            FinishOperation(botAI, false, "world-thread mailbox queue unavailable");
    }

    static uint64 EquipmentRepairQuote(Player* bot, Creature* vendor)
    {
        uint64 total = 0;
        for (uint8 slot = EQUIPMENT_SLOT_START; slot < EQUIPMENT_SLOT_END; ++slot)
        {
            Item* item = bot->GetItemByPos(INVENTORY_SLOT_BAG_0, slot);
            if (!item)
                continue;
            uint32 const maximum = item->GetUInt32Value(ITEM_FIELD_MAXDURABILITY);
            uint32 const current = item->GetUInt32Value(ITEM_FIELD_DURABILITY);
            if (current >= maximum)
                continue;
            ItemTemplate const* itemTemplate = item->GetTemplate();
            DurabilityCostsEntry const* costs = sDurabilityCostsStore.LookupEntry(itemTemplate->ItemLevel);
            DurabilityQualityEntry const* quality =
                sDurabilityQualityStore.LookupEntry((itemTemplate->Quality + 1) * 2);
            if (!costs || !quality)
                return std::numeric_limits<uint64>::max();
            uint32 const multiplier =
                costs->multiplier[ItemSubClassToDurabilityMultiplierId(itemTemplate->Class, itemTemplate->SubClass)];
            uint32 price = uint32((maximum - current) * multiplier * double(quality->quality_mod));
            price = uint32(price * bot->GetReputationPriceDiscount(vendor) * sWorld->getRate(RATE_REPAIRCOST));
            total += std::max(uint32(1), price);
        }
        return total;
    }

    void UpdateNPCService(PlayerbotAI* botAI, Creature* npc, uint32 now)
    {
        Player* bot = botAI->GetBot();
        LocalOperation& loop = *localOperation;
        std::string const operation = loop.command.operation;
        if (!npc || !bot->GetNPCIfCanInteractWith(npc->GetGUID(), NPCServiceFlag(operation)))
        {
            FinishOperation(botAI, false, "NPC service unavailable");
            return;
        }
        if (operation == "inspect_auctions" || operation == "buy_auction" || operation == "bid_auction")
        {
            if (!loop.auctionWork)
            {
                loop.auctionWork = std::make_shared<AgentAuctionWork>();
                if (!PlayerbotWorldThreadProcessor::instance().QueueOperation(std::make_unique<AgentAuctionOperation>(
                        bot->GetGUID(), loop.target, loop.auctionWork, loop.serviceId, loop.auctionCursor,
                        loop.auctionId, loop.serviceLimit, loop.auctionBuyout, loop.serviceBudget, loop.auctionItemGuid,
                        operation == "bid_auction")))
                    FinishOperation(botAI, false, "world-thread auction queue unavailable");
                return;
            }
            std::vector<AgentAuctionOffer> offers;
            bool success, more;
            uint32 nextCursor;
            uint64 purchased;
            {
                std::lock_guard<std::mutex> guard(loop.auctionWork->Mutex);
                if (!loop.auctionWork->Completed)
                    return;
                success = loop.auctionWork->Success;
                more = loop.auctionWork->More;
                nextCursor = loop.auctionWork->NextCursor;
                purchased = loop.auctionWork->PurchasedItemGuid;
                offers = loop.auctionWork->Offers;
            }
            if (!success)
            {
                FinishOperation(botAI, false, "auction changed or purchase unavailable; inspect fresh state");
                return;
            }
            std::ostringstream payload;
            payload << "{\"operation_id\":\"" << EscapeJson(loop.command.operationId) << "\",\"npc_guid\":\""
                    << loop.target.GetRawValue() << "\",\"map_id\":" << loop.mapId << ",\"item_id\":" << loop.serviceId
                    << ",\"inventory_count\":" << bot->GetItemCount(loop.serviceId, false)
                    << ",\"cursor\":" << loop.auctionCursor << ",\"next_cursor\":" << nextCursor
                    << ",\"more\":" << (more ? "true" : "false") << ",\"purchased_item_guid\":\"" << purchased
                    << "\",\"auction_id\":" << loop.auctionId << ",\"bid_copper\":" << loop.auctionBuyout
                    << ",\"offers\":[";
            for (size_t index = 0; index < offers.size(); ++index)
            {
                if (index)
                    payload << ",";
                AgentAuctionOffer const& offer = offers[index];
                payload << "{\"auction_id\":" << offer.AuctionId << ",\"item_id\":" << offer.ItemId
                        << ",\"count\":" << offer.Count << ",\"buyout_copper\":" << offer.Buyout << ",\"item_guid\":\""
                        << offer.ItemGuid << "\",\"minimum_bid_copper\":" << offer.MinimumBid
                        << ",\"expires_at\":" << offer.ExpiresAt << "}";
            }
            payload << "]}";
            Publish("auction_observation", loop.command.requestId, payload.str());
            FinishOperation(botAI, true,
                            operation == "inspect_auctions" ? "bounded auction page observed"
                            : operation == "bid_auction"    ? "auction bid verified; outcome pending"
                                                            : "auction buyout submitted; collect won mail");
            return;
        }
        if (getMSTimeDiff(loop.lastInteractionMs, now) < 1000)
            return;
        loop.lastInteractionMs = now;
        if (operation == "discover_flight_path" || operation == "take_flight")
        {
            uint32 const source = sObjectMgr->GetNearestTaxiNode(
                npc->GetPositionX(), npc->GetPositionY(), npc->GetPositionZ(), npc->GetMapId(), bot->GetTeamId());
            if (operation == "discover_flight_path")
            {
                bot->GetSession()->SendLearnNewTaxiNode(npc);
                FinishOperation(botAI, source && bot->m_taxi.IsTaximaskNodeKnown(source),
                                "flight node discovery verified");
                return;
            }
            uint32 path = 0;
            uint32 cost = 0;
            sObjectMgr->GetTaxiPath(source, loop.serviceId, path, cost);
            uint32 const fare = uint32(std::ceil(cost * bot->GetReputationPriceDiscount(npc)));
            if (!source || source == loop.serviceId || !path || !bot->m_taxi.IsTaximaskNodeKnown(source) ||
                !bot->m_taxi.IsTaximaskNodeKnown(loop.serviceId) || fare > loop.serviceBudget || fare > bot->GetMoney())
            {
                FinishOperation(botAI, false, "flight nodes, direct route, or fare unavailable");
                return;
            }
            if (!bot->ActivateTaxiPathTo({source, loop.serviceId}, npc, 1))
            {
                FinishOperation(botAI, false, "native taxi takeoff failed");
                return;
            }
            loop.attempted = true;
            loop.lastProgressMs = now;
            return;
        }
        if (operation == "repair_equipment")
        {
            uint64 const quote = EquipmentRepairQuote(bot, npc);
            if (quote > loop.serviceBudget || quote > bot->GetMoney())
            {
                FinishOperation(botAI, false, "equipment repair exceeds the available budget");
                return;
            }
            for (uint8 slot = EQUIPMENT_SLOT_START; slot < EQUIPMENT_SLOT_END; ++slot)
                bot->DurabilityRepair(uint16((INVENTORY_SLOT_BAG_0 << 8) | slot), true,
                                      bot->GetReputationPriceDiscount(npc), false);
            FinishOperation(botAI, EquipmentRepairQuote(bot, npc) == 0, "equipment repair verified");
            return;
        }
        if (loop.serviceCompleted >= loop.serviceLimit)
        {
            FinishOperation(botAI, true, "bounded NPC service batch completed");
            return;
        }
        if (operation == "sell_junk")
        {
            for (Item* item : botAI->GetInventoryItems())
            {
                if (!item || item->IsEquipped() || item->IsInTrade())
                    continue;
                ItemTemplate const* itemTemplate = item->GetTemplate();
                if (itemTemplate->Quality != ITEM_QUALITY_POOR || !itemTemplate->SellPrice ||
                    itemTemplate->Class == ITEM_CLASS_QUEST || itemTemplate->StartQuest ||
                    itemTemplate->TotemCategory || bot->HasQuestForItem(item->GetEntry()) ||
                    (itemTemplate->Class == ITEM_CLASS_WEAPON &&
                     (itemTemplate->SubClass == ITEM_SUBCLASS_WEAPON_MISC ||
                      itemTemplate->SubClass == ITEM_SUBCLASS_WEAPON_FISHING_POLE)))
                    continue;
                ObjectGuid const itemGuid = item->GetGUID();
                WorldPacket packet(CMSG_SELL_ITEM);
                packet << npc->GetGUID() << itemGuid << item->GetCount();
                WorldPackets::Item::SellItem sale(std::move(packet));
                sale.Read();
                bot->GetSession()->HandleSellItemOpcode(sale);
                if (bot->GetItemByGuid(itemGuid))
                {
                    FinishOperation(botAI, false, "junk sale was not verified");
                    return;
                }
                ++loop.serviceCompleted;
                loop.lastProgressMs = now;
                return;
            }
            FinishOperation(botAI, true, "no eligible grey junk remains");
            return;
        }
        if (operation == "buy_vendor_item")
        {
            VendorItemData const* stock = npc->GetVendorItems();
            if (stock)
                for (uint32 slot = 0; slot < stock->GetItemCount(); ++slot)
                {
                    VendorItem const* offer = stock->GetItem(slot);
                    if (!offer || offer->item != loop.serviceId || offer->ExtendedCost)
                        continue;
                    ItemTemplate const* itemTemplate = sObjectMgr->GetItemTemplate(offer->item);
                    if (!itemTemplate)
                        continue;
                    uint32 const price =
                        uint32(std::floor(itemTemplate->BuyPrice * bot->GetReputationPriceDiscount(npc)));
                    if (price > loop.serviceBudget - loop.serviceSpent || price > bot->GetMoney())
                    {
                        FinishOperation(botAI, false, "purchase exceeds the remaining budget");
                        return;
                    }
                    uint32 const before = bot->GetItemCount(offer->item, false);
                    uint32 const moneyBefore = bot->GetMoney();
                    bot->BuyItemFromVendorSlot(npc->GetGUID(), slot, offer->item, 1, NULL_BAG, NULL_SLOT);
                    if (bot->GetItemCount(offer->item, false) <= before)
                    {
                        FinishOperation(botAI, false, "vendor purchase was not verified");
                        return;
                    }
                    loop.serviceSpent += moneyBefore - bot->GetMoney();
                    ++loop.serviceCompleted;
                    loop.lastProgressMs = now;
                    return;
                }
            FinishOperation(botAI, false, "item is not offered for ordinary gold at this vendor");
            return;
        }
        Trainer::Trainer* trainer = sObjectMgr->GetTrainer(npc->GetEntry());
        if (!trainer || trainer->GetTrainerType() != Trainer::Type::Class || !trainer->IsTrainerValidForPlayer(bot))
        {
            FinishOperation(botAI, false, "a valid class trainer is required");
            return;
        }
        bool unaffordable = false;
        for (Trainer::Spell const& spell : trainer->GetSpells())
        {
            if (!trainer->CanTeachSpell(bot, &spell))
                continue;
            uint32 const price = uint32(spell.MoneyCost * bot->GetReputationPriceDiscount(npc));
            if (price > loop.serviceBudget - loop.serviceSpent || price > bot->GetMoney())
            {
                unaffordable = true;
                continue;
            }
            uint32 const moneyBefore = bot->GetMoney();
            trainer->TeachSpell(npc, bot, spell.SpellId);
            bool learned = bot->HasSpell(spell.SpellId);
            if (spell.IsCastable())
            {
                learned = false;
                if (SpellInfo const* info = sSpellMgr->GetSpellInfo(spell.SpellId))
                {
                    bool hasLearnEffect = false;
                    bool knowsAll = true;
                    for (SpellEffectInfo const& effect : info->GetEffects())
                        if (effect.IsEffect(SPELL_EFFECT_LEARN_SPELL))
                        {
                            hasLearnEffect = true;
                            knowsAll = knowsAll && bot->HasSpell(effect.TriggerSpell);
                        }
                    learned = hasLearnEffect && knowsAll;
                }
            }
            if (!learned)
            {
                FinishOperation(botAI, false, "trainer did not confirm the learned spell");
                return;
            }
            loop.serviceSpent += moneyBefore - bot->GetMoney();
            ++loop.serviceCompleted;
            loop.lastProgressMs = now;
            return;
        }
        FinishOperation(
            botAI, !unaffordable,
            unaffordable ? "eligible training exceeds the remaining budget" : "eligible class training completed");
    }

    void LogOperationBlock(std::string const& block)
    {
        if (!localOperation || localOperation->diagnosticBlock == block)
            return;
        LOG_DEBUG("playerbots.agent",
                  "Operation priority bot={} operation_id={} operation={} previous_block={} block={}",
                  botGuid.ToString(), localOperation->command.operationId, localOperation->command.operation,
                  localOperation->diagnosticBlock, block);
        localOperation->diagnosticBlock = block;
    }

    void UpdateOperation(PlayerbotAI* botAI, uint32 elapsed)
    {
        if (!localOperation)
            return;
        operationEvents.Update(elapsed);
        if (!operationEvents.ExecuteEvent())
            return;
        operationEvents.ScheduleEvent(AGENT_OPERATION_EVENT, Milliseconds(AGENT_OPERATION_TICK_MS));
        Player* bot = botAI->GetBot();
        LocalOperation& loop = *localOperation;
        std::string const& operation = loop.command.operation;
        uint32 const now = getMSTime();
        if (operation == "recover_death")
        {
            if (bot->IsAlive())
            {
                FinishOperation(botAI, true, "resurrection verified");
                return;
            }
            if (bot->InBattleground() || bot->InArena() ||
                getMSTimeDiff(loop.recoveryStartedMs, now) >= AGENT_RECOVERY_TIMEOUT_MS)
            {
                FinishOperation(botAI, false, "ordinary corpse recovery unavailable or timed out");
                return;
            }
            if (bot->IsBeingTeleported())
                return;
            if (!bot->HasPlayerFlag(PLAYER_FLAGS_GHOST))
            {
                if (!loop.attempted)
                {
                    loop.attempted = true;
                    WorldPacket packet(CMSG_REPOP_REQUEST);
                    packet << uint8(0);
                    bot->GetSession()->HandleRepopRequestOpcode(packet);
                    loop.lastProgressMs = now;
                }
                else if (getMSTimeDiff(loop.lastProgressMs, now) >= AGENT_OPERATION_STALL_MS)
                    FinishOperation(botAI, false, "spirit release was not verified");
                return;
            }
            Corpse* corpse = bot->GetCorpse();
            if (!corpse || corpse->GetMapId() != bot->GetMapId())
            {
                FinishOperation(botAI, false, "corpse is unavailable or requires cross-map recovery");
                return;
            }
            if (loop.position.GetExactDist(bot) >= 1.0f)
            {
                loop.position.Relocate(bot);
                loop.lastProgressMs = now;
            }
            if (corpse->IsWithinDist(bot, CORPSE_RECLAIM_RADIUS - 5.0f, true))
            {
                // The normal handler enforces reclaim delay, distance and phase.
                // Wait for its cooldown; never resurrect or repair for free.
                if (time_t(corpse->GetGhostTime() +
                           bot->GetCorpseReclaimDelay(corpse->GetType() == CORPSE_RESURRECTABLE_PVP)) >
                    time_t(GameTime::GetGameTime().count()))
                    loop.lastProgressMs = now;
                else if (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000)
                {
                    loop.lastMoveAttemptMs = now;
                    WorldPacket packet(CMSG_RECLAIM_CORPSE);
                    packet << bot->GetGUID();
                    bot->GetSession()->HandleReclaimCorpseOpcode(packet);
                }
            }
            else if (botAI->CanMove() && !bot->isMoving() &&
                     (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000))
            {
                loop.lastMoveAttemptMs = now;
                AgentMoveToTargetAction movement(botAI);
                loop.movementStarted = movement.MoveToPosition(corpse->GetMapId(), corpse->GetPositionX(),
                                                               corpse->GetPositionY(), corpse->GetPositionZ());
            }
            if (getMSTimeDiff(loop.lastProgressMs, now) >= AGENT_OPERATION_STALL_MS)
            {
                FinishOperation(botAI, false, "corpse recovery made no positional progress");
                return;
            }
            if (getMSTimeDiff(loop.lastReportMs, now) >= AGENT_OPERATION_PROGRESS_MS)
            {
                loop.lastReportMs = now;
                Publish("primitive_progress", "",
                        "{\"operation_id\":\"" + EscapeJson(loop.command.operationId) +
                            "\",\"snapshot\":" + BuildSnapshot(botAI) + "}");
            }
            return;
        }
        bool const combat = operation == "engage_target";
        bool const follow = operation == "follow_player";
        bool const assist = operation == "assist_leader";
        bool const travelOperation = operation == "navigate_to_destination" ||
                                     operation == "navigate_to_quest_objective" ||
                                     operation == "navigate_to_quest_turnin" ||
                                     operation == "navigate_to_quest_giver" || operation == "take_flight";
        if (!bot->IsAlive() || (!travelOperation && bot->GetMapId() != loop.mapId))
        {
            FinishOperation(botAI, false, "bot died or left the operation map");
            return;
        }
        if (operation == "receive_trade_material")
        {
            if (!loop.materialTradeWork)
            {
                loop.materialTradeWork = std::make_shared<AgentMaterialTradeWork>();
                if (!PlayerbotWorldThreadProcessor::instance().QueueOperation(
                        std::make_unique<AgentMaterialTradeOperation>(bot->GetGUID(), loop.target,
                                                                      loop.materialTradeRevision, loop.tradeMaterials,
                                                                      loop.serviceBudget, loop.materialTradeWork)))
                    FinishOperation(botAI, false, "world-thread material trade queue unavailable");
                return;
            }
            bool success;
            {
                std::lock_guard<std::mutex> guard(loop.materialTradeWork->Mutex);
                if (!loop.materialTradeWork->Completed)
                    return;
                success = loop.materialTradeWork->Success;
            }
            FinishOperation(botAI, success,
                            success ? "trade material transfer verified"
                                    : "trade offer changed or transfer unverified; inspect inventory");
            return;
        }
        if (bot->GetTradeData() || (bot->IsInCombat() && !combat && !assist))
        {
            if (operation == "craft_once" && loop.attempted)
            {
                FinishOperation(botAI, false, "crafting interrupted by combat or trade; inspect inventory");
                return;
            }
            LogOperationBlock(bot->GetTradeData() ? "trade" : "combat");
            if (operation == "fish_once")
            {
                bot->InterruptNonMeleeSpells(false, FISHING_SPELL);
                if (loop.attempted)
                {
                    FinishOperation(botAI, false, "fishing interrupted by combat or trade");
                    return;
                }
            }
            if (!loop.paused && !bot->IsInCombat())
            {
                bot->GetMotionMaster()->Clear();
                bot->StopMoving();
            }
            loop.paused = true;
            loop.movementStarted = false;
            loop.lastProgressMs = now;
            return;
        }
        loop.paused = false;
        if (operation == "craft_once")
        {
            UpdateCrafting(botAI, now);
            return;
        }
        if (operation == "take_flight" && loop.attempted)
        {
            LogOperationBlock("");
            if (bot->IsInFlight())
            {
                loop.lastProgressMs = now;
                if (getMSTimeDiff(loop.lastReportMs, now) >= AGENT_OPERATION_PROGRESS_MS)
                {
                    loop.lastReportMs = now;
                    Publish("primitive_progress", "",
                            "{\"operation_id\":\"" + EscapeJson(loop.command.operationId) +
                                "\",\"snapshot\":" + BuildSnapshot(botAI) + "}");
                }
                return;
            }
            TaxiNodesEntry const* destination = sTaxiNodesStore.LookupEntry(loop.serviceId);
            bool const arrived = destination && destination->map_id == bot->GetMapId() &&
                                 bot->GetDistance(destination->x, destination->y, destination->z) <= 40.0f;
            FinishOperation(botAI, arrived, arrived ? "flight destination reached" : "flight ended before destination");
            return;
        }
        if (operation == "fish_once")
        {
            LogOperationBlock("");
            UpdateFishing(botAI, now);
            return;  // Fishing is a channeled spell; the general casting guard must not stall it.
        }
        if (!botAI->CanMove() || bot->IsNonMeleeSpellCast(false))
        {
            LogOperationBlock(bot->IsNonMeleeSpellCast(false) ? "casting" : "movement_disabled");
            return;
        }
        LogOperationBlock("");
        if (loop.position.GetExactDist(bot) >= 1.0f)
        {
            loop.position.Relocate(bot);
            loop.lastProgressMs = now;
        }
        if (!follow && !assist && getMSTimeDiff(loop.lastProgressMs, now) >= AGENT_OPERATION_STALL_MS)
        {
            FinishOperation(botAI, false, "local operation made no progress");
            return;
        }
        if (getMSTimeDiff(loop.lastReportMs, now) >= AGENT_OPERATION_PROGRESS_MS)
        {
            loop.lastReportMs = now;
            Publish("primitive_progress", "",
                    "{\"operation_id\":\"" + EscapeJson(loop.command.operationId) +
                        "\",\"snapshot\":" + BuildSnapshot(botAI) + "}");
        }

        if (operation == "navigate_to_destination" || operation == "navigate_to_quest_objective" ||
            operation == "navigate_to_quest_turnin" || operation == "navigate_to_quest_giver")
        {
            TravelTarget* travel = botAI->GetAiObjectContext()->GetValue<TravelTarget*>("travel target")->Get();
            if (!travel || travel->getDestination() == TravelMgr::instance().nullTravelDestination)
                FinishOperation(botAI, false, "travel target unavailable");
            else if (!travel->isTraveling())
            {
                WorldPosition position(bot);
                bool const arrived = travel->isWorking() || travel->getDestination()->isIn(&position);
                FinishOperation(botAI, arrived, arrived ? "arrived" : "travel ended before arrival");
            }
            else if (!bot->isMoving() && !bot->IsInFlight() &&
                     (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000))
            {
                WorldPosition const* destination = travel->getPosition();
                if (destination && destination->GetMapId() == bot->GetMapId())
                {
                    loop.lastMoveAttemptMs = now;
                    AgentMoveToTargetAction movement(botAI);
                    uint32 pathType = 0;
                    loop.movementStarted =
                        movement.MoveToPosition(destination->GetMapId(), destination->GetPositionX(),
                                                destination->GetPositionY(), destination->GetPositionZ(), &pathType);
                    if (!loop.movementStarted && loop.navigationPathType != pathType)
                        LOG_WARN("playerbots.agent",
                                 "Navigation path rejected bot={} operation_id={} path_type={} map={} "
                                 "start=[{},{},{}] goal=[{},{},{}]",
                                 bot->GetName(), loop.command.operationId, pathType, bot->GetMapId(),
                                 bot->GetPositionX(), bot->GetPositionY(), bot->GetPositionZ(),
                                 destination->GetPositionX(), destination->GetPositionY(), destination->GetPositionZ());
                    loop.navigationPathType = pathType;
                }
            }
            return;
        }
        if (operation == "move_random")
        {
            if (loop.movementStarted && !bot->isMoving())
            {
                bool const moved = loop.navigationPosition.GetExactDist(bot) >= 1.0f;
                FinishOperation(botAI, moved, moved ? "search segment reached" : "search segment made no movement");
            }
            return;
        }
        if (operation == "navigate_to_position")
        {
            Position const& destination = loop.navigationPosition;
            if (bot->GetExactDist(destination.GetPositionX(), destination.GetPositionY(), destination.GetPositionZ()) <=
                loop.distance)
            {
                FinishOperation(botAI, true, "observed position reached");
                return;
            }
            if (!bot->isMoving() && (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000))
            {
                loop.lastMoveAttemptMs = now;
                AgentMoveToTargetAction movement(botAI);
                loop.movementStarted = movement.MoveToPosition(loop.mapId, destination.GetPositionX(),
                                                               destination.GetPositionY(), destination.GetPositionZ());
            }
            return;
        }
        if (operation == "assist_leader")
        {
            Group* group = bot->GetGroup();
            Player* leader = group ? ObjectAccessor::FindPlayer(group->GetLeaderGUID()) : nullptr;
            if (!leader || leader == bot || leader->GetMapId() != bot->GetMapId())
            {
                FinishOperation(botAI, false, "no group leader to assist");
                return;
            }
            // The leader's live victim decides the target every tick, so the
            // group switches targets as fast as the leader does. Without a
            // leader target, help another group member under attack.
            Unit* victim = leader->IsInCombat() ? leader->GetVictim() : nullptr;
            if (!victim || !victim->IsAlive() || !bot->IsValidAttackTarget(victim))
            {
                Group* memberGroup = bot->GetGroup();
                if (memberGroup)
                {
                    for (GroupReference* ref = memberGroup->GetFirstMember(); ref; ref = ref->next())
                    {
                        Player* member = ref->GetSource();
                        if (!member || member == bot || !member->IsAlive() || member->GetMapId() != bot->GetMapId())
                            continue;
                        if (member->GetDistance(bot) > sPlayerbotAIConfig.reactDistance)
                            continue;
                        if (!member->IsInCombat())
                            continue;
                        Unit* memberVictim = member->GetVictim();
                        if (memberVictim && memberVictim->IsAlive() && bot->IsValidAttackTarget(memberVictim))
                        {
                            victim = memberVictim;
                            break;
                        }
                    }
                }
            }
            if (!victim || !victim->IsAlive() || !bot->IsValidAttackTarget(victim))
            {
                if (!bot->IsInCombat() &&
                    (!bot->isMoving() ||
                     bot->GetMotionMaster()->GetCurrentMovementGeneratorType() != FOLLOW_MOTION_TYPE))
                {
                    if (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000)
                    {
                        loop.lastMoveAttemptMs = now;
                        AgentMoveToTargetAction movement(botAI);
                        loop.movementStarted = movement.MoveToTarget(leader, sPlayerbotAIConfig.followDistance);
                    }
                }
                loop.lastProgressMs = now;
                return;
            }
            if (bot->GetVictim() != victim || (!bot->IsInCombat() && !bot->isMoving()))
                loop.attempted = botAI->DoSpecificAction("agent attack target",
                                                         Event("agent attack target", victim->GetGUID()), true);
            loop.lastProgressMs = now;
            return;
        }

        WorldObject* target = loop.target.IsGameObject() ? static_cast<WorldObject*>(botAI->GetGameObject(loop.target))
                                                         : static_cast<WorldObject*>(botAI->GetUnit(loop.target));
        if (!target || !target->IsInWorld() || target->GetMap() != bot->GetMap() ||
            (target->ToUnit() && target->ToUnit()->IsDuringRemoveFromWorld()))
        {
            FinishOperation(
                botAI,
                !loop.corpseOnly && loop.attempted && (operation == "loot_target" || operation == "gather_target"),
                "operation target disappeared");
            return;
        }
        if (loop.corpseOnly)
        {
            Creature* corpse = target->ToCreature();
            if (!corpse || corpse->IsAlive())
            {
                FinishOperation(botAI, false, "corpse target unavailable");
                return;
            }
            if (!corpse->HasFlag(UNIT_DYNAMIC_FLAGS, UNIT_DYNFLAG_LOOTABLE))
            {
                FinishOperation(botAI, true, "corpse loot exhausted");
                return;
            }
            if (!bot->isAllowedToLoot(corpse))
            {
                FinishOperation(botAI, false, "corpse loot rights unavailable");
                return;
            }
        }
        if (combat)
        {
            Unit* unit = target->ToUnit();
            if (!unit || !unit->IsAlive())
            {
                FinishOperation(botAI, true, "combat target defeated");
                return;
            }
            if (!bot->IsValidAttackTarget(unit))
            {
                FinishOperation(botAI, false, "combat target no longer attackable");
                return;
            }
            if (loop.health != unit->GetHealth())
            {
                loop.health = unit->GetHealth();
                loop.lastProgressMs = now;
            }
            if (!loop.attempted || (!bot->IsInCombat() && !bot->GetVictim()))
                loop.attempted =
                    botAI->DoSpecificAction("agent attack target", Event("agent attack target", loop.target), true);
            return;
        }
        if (Unit* unit = target->ToUnit();
            unit && !unit->IsAlive() && (operation == "approach_target" || operation == "navigate_to_player" || follow))
        {
            FinishOperation(botAI, false, "movement target died");
            return;
        }
        if (bot->GetDistance(target) > loop.distance)
        {
            if (!loop.movementStarted ||
                (!bot->isMoving() && bot->GetMotionMaster()->GetCurrentMovementGeneratorType() != FOLLOW_MOTION_TYPE))
            {
                // Path searches are expensive; retry at most once per second.
                if (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000)
                {
                    loop.lastMoveAttemptMs = now;
                    AgentMoveToTargetAction movement(botAI);
                    loop.movementStarted = movement.MoveToTarget(target, loop.distance);
                }
            }
            return;
        }
        if (follow)
        {
            loop.lastProgressMs = now;
            if (!loop.movementStarted)
            {
                if (!loop.lastMoveAttemptMs || getMSTimeDiff(loop.lastMoveAttemptMs, now) >= 1000)
                {
                    loop.lastMoveAttemptMs = now;
                    AgentMoveToTargetAction movement(botAI);
                    loop.movementStarted = movement.MoveToTarget(target, loop.distance);
                }
            }
            return;
        }
        if (operation == "approach_target" || operation == "navigate_to_player")
        {
            FinishOperation(botAI, true, "arrived at target");
            return;
        }

        if (bot->isMoving())
        {
            bot->GetMotionMaster()->Clear();
            bot->StopMoving();
            loop.movementStarted = false;
            return;
        }
        if (IsNPCService(operation))
        {
            UpdateNPCService(botAI, target->ToCreature(), now);
            return;
        }
        if (operation == "collect_mail" || operation == "send_mail")
        {
            UpdateMailbox(botAI, now);
            return;
        }
        if (operation == "loot_target" || operation == "gather_target")
        {
            LootObject loot(bot, loop.target);
            if (!loot.IsLootPossible(bot))
            {
                FinishOperation(botAI, !loop.corpseOnly && loop.attempted,
                                loop.corpseOnly ? "corpse loot unavailable"
                                                : (loop.attempted ? "loot or gather completed" : "loot unavailable"));
                return;
            }
            if (getMSTimeDiff(loop.lastInteractionMs, now) < 1000)
                return;
            loop.lastInteractionMs = now;
            botAI->GetAiObjectContext()->GetValue<LootObject>("loot target")->Set(loot);
            if (botAI->DoSpecificAction("open loot", Event("open loot"), true))
                loop.attempted = true;
            return;
        }
        if (operation == "use_gameobject")
        {
            GameObject* object = target->ToGameObject();
            if (!object || !object->isSpawned())
                FinishOperation(botAI, false, "game object unavailable");
            else
            {
                object->Use(bot);
                FinishOperation(botAI, true, "game object interaction performed");
            }
            return;
        }
        if (operation == "interact_quest_giver")
        {
            if (bot->GetQuestStatus(loop.questId) == QUEST_STATUS_REWARDED)
            {
                FinishOperation(botAI, true, "quest rewarded");
                return;
            }
            if (getMSTimeDiff(loop.lastInteractionMs, now) < 1000)
                return;
            loop.lastInteractionMs = now;
            WorldPacket packet(CMSG_QUESTGIVER_COMPLETE_QUEST);
            packet << loop.target;
            loop.attempted = botAI->DoSpecificAction("talk to quest giver", Event("talk to quest giver", packet), true);
        }
        if (operation == "accept_quest")
        {
            if (bot->GetQuestStatus(loop.questId) != QUEST_STATUS_NONE)
            {
                FinishOperation(botAI, true, "quest accepted");
                return;
            }
            if (getMSTimeDiff(loop.lastInteractionMs, now) < 1000)
                return;
            loop.lastInteractionMs = now;
            // Same session path QuestAction::AcceptQuest uses on the bot AI thread.
            WorldPacket packet(CMSG_QUESTGIVER_ACCEPT_QUEST);
            packet << loop.target << loop.questId << uint32(0);
            packet.rpos(0);
            bot->GetSession()->HandleQuestgiverAcceptQuestOpcode(packet);
        }
    }

    void ExecuteCommand(PlayerbotAI* botAI, AgentBridgeCommand const& command)
    {
        if (command.botGuid != botGuidToken)
            return;
        int64 const nowUnixMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        if (command.ownerToken != ownerToken || command.ownerEpoch != ownerEpoch ||
            (command.deadlineUnixMs > 0 && command.deadlineUnixMs < nowUnixMs))
        {
            PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                          "stale owner epoch or expired command");
            return;
        }
        if (command.arguments.size() > AGENT_MAX_COMMAND_BYTES)
        {
            PublishResult(command.requestId, command.operationId, command.operation, "rejected", "arguments too large");
            return;
        }

        if (agent_bridge::ClusterEconomyServiceOwned() &&
            (command.operation == "inspect_auctions" || command.operation == "buy_auction" ||
             command.operation == "bid_auction" || command.operation == "collect_mail" ||
             command.operation == "send_mail"))
        {
            PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                          "cluster auction/mail authority requires a service handoff; legacy handlers are disabled");
            return;
        }

        auto cachedResult = resultCache.find(command.requestId);
        if (!command.requestId.empty() && cachedResult != resultCache.end())
        {
            Publish("operation_result", command.requestId, cachedResult->second);
            return;
        }

        try
        {
            boost::property_tree::ptree arguments;
            std::istringstream input(command.arguments.empty() ? "{}" : command.arguments);
            boost::property_tree::read_json(input, arguments);

            if (botAI->GetBot()->GetTradeData() &&
                (command.operation == "move_random" || command.operation == "approach_target" ||
                 command.operation == "engage_target" || command.operation == "follow_player" ||
                 command.operation == "assist_leader" || command.operation.compare(0, 9, "navigate_") == 0))
            {
                PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                              "movement and combat tasks are paused during trade");
                return;
            }

            if (command.operation == "snapshot")
            {
                uint32 const offset = GetUInt(arguments, "recipe_offset");
                uint32 const itemId = GetUInt(arguments, "recipe_item_id");
                if (offset > AGENT_MAX_CRAFTING_RECIPE_OFFSET || (offset && itemId))
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid crafting recipe inspection range");
                    return;
                }
                PublishSnapshot(botAI, command, GetUInt(arguments, "craft_item_id"), offset, itemId);
                return;
            }
            if (command.operation == "send_chat")
            {
                SendChat(botAI, arguments, command);
                return;
            }
            if (command.operation == "receive_trade_material")
            {
                Player* partner = botAI->GetBot()->GetTrader();
                uint32 const itemId = GetUInt(arguments, "item_id");
                uint32 const count = GetUInt(arguments, "count");
                uint64 const revision = GetUInt64(arguments, "trade_revision");
                ObjectGuid const partnerGuid(GetUInt64(arguments, "target_guid"));
                if (!partner || partner->GetGUID() != partnerGuid || !itemId || !count ||
                    count > AGENT_MAX_SNAPSHOT_ITEMS || !arguments.get_optional<uint64>("trade_revision") ||
                    revision != tradeRevision.load())
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "material trade unavailable or changed");
                    return;
                }
                StartOperation(botAI, command, partnerGuid);
                localOperation->materialTradeRevision = revision;
                localOperation->serviceId = itemId;
                localOperation->serviceLimit = count;
                localOperation->serviceBudget = GetUInt(arguments, "max_spend_copper");
                if (auto materials = arguments.get_child_optional("materials"))
                {
                    uint64 total = 0;
                    std::set<uint32> seen;
                    for (auto const& [key, material] : *materials)
                    {
                        uint32 const materialId = GetUInt(material, "item_id");
                        uint32 const quantity = GetUInt(material, "count");
                        if (!materialId || !quantity || !seen.insert(materialId).second ||
                            localOperation->tradeMaterials.size() >= TRADE_SLOT_TRADED_COUNT)
                        {
                            FinishOperation(botAI, false, "invalid material trade composition");
                            return;
                        }
                        total += quantity;
                        localOperation->tradeMaterials.push_back({materialId, quantity});
                    }
                    if (total != count)
                    {
                        FinishOperation(botAI, false, "material trade quantity mismatch");
                        return;
                    }
                }
                else
                    localOperation->tradeMaterials.push_back({itemId, count});
                return;
            }
            if (command.operation == "begin_trade" || command.operation == "accept_trade" ||
                command.operation == "cancel_trade" || command.operation == "offer_trade_money" ||
                command.operation == "offer_trade_item")
            {
                Player* bot = botAI->GetBot();
                Player* partner = bot->GetTrader();
                uint64 const revision = GetUInt64(arguments, "trade_revision");
                if (!partner || !bot->GetTradeData() || !arguments.get_optional<uint64>("trade_revision") ||
                    revision != tradeRevision.load())
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "trade unavailable or offer changed; request a fresh snapshot");
                    return;
                }
                uint32 const slot = arguments.get<uint32>("trade_slot", 0);
                if (slot >= TRADE_SLOT_TRADED_COUNT)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid trade slot");
                    return;
                }
                PlayerbotWorldThreadProcessor::instance().QueueOperation(std::make_unique<AgentTradeOperation>(
                    bot->GetGUID(), partner->GetGUID(), revision, command.operation,
                    arguments.get<uint32>("money_copper", 0), static_cast<uint8>(slot),
                    ObjectGuid(GetUInt64(arguments, "item_guid")), command.requestId, command.operationId));
                PublishResult(command.requestId, command.operationId, command.operation, "accepted",
                              "trade operation queued; verify live trade state");
                return;
            }
            if (command.operation == "recover_death")
            {
                StartOperation(botAI, command);
                return;
            }
            if (command.operation == "move_random")
            {
                if (botAI->GetBot()->IsInCombat() || !botAI->CanMove())
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "bot is in combat or movement is restricted");
                    return;
                }
                // Explicit search commands must not inherit the legacy RPG-target
                // usefulness gate or the per-tick wait windows.
                MoveRandomAction search(botAI);
                bool const moved = search.MoveRandomExplicit(sPlayerbotAIConfig.tooCloseDistance + urand(10, 30));
                if (moved)
                {
                    StartOperation(botAI, command);
                    localOperation->navigationPosition.Relocate(botAI->GetBot());
                    localOperation->movementStarted = true;
                }
                else
                    PublishResult(
                        command.requestId, command.operationId, command.operation, "rejected",
                        botAI->GetBot()->isMoving() ? "bot is already moving" : "local search path unavailable");
                return;
            }
            if (command.operation == "assist_leader")
            {
                Player* bot = botAI->GetBot();
                Group* group = bot->GetGroup();
                Player* leader = group ? ObjectAccessor::FindPlayer(group->GetLeaderGUID()) : nullptr;
                if (!leader || leader == bot)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "no group leader to assist");
                    return;
                }
                StartOperation(botAI, command);
                return;
            }
            if (command.operation == "change_group_leader")
            {
                Player* bot = botAI->GetBot();
                Group* group = bot->GetGroup();
                Player* target = ObjectAccessor::FindPlayerByName(GetString(arguments, "player_name"));
                if (!group || !group->IsLeader(bot->GetGUID()) || !target || target->GetGroup() != group ||
                    target == bot)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "you must be the group leader and the target must be a member");
                    return;
                }
                group->ChangeLeader(target->GetGUID());
                group->SendUpdate();
                PublishResult(command.requestId, command.operationId, command.operation, "completed",
                              "leadership transferred", target->GetGUID());
                return;
            }
            if (command.operation == "cancel")
            {
                ClearOperation(botAI);
                PublishResult(command.requestId, command.operationId, command.operation, "completed",
                              "cancel processed");
                return;
            }
            if (command.operation == "invite_to_group")
            {
                Player* target = ObjectAccessor::FindPlayerByName(GetString(arguments, "player_name"));
                if (target && target->GetGroup())
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  EscapeJson(std::string(target->GetName())) +
                                      " is already in a group; pick someone with in_group=false");
                    return;
                }
                bool const invited =
                    target && target != botAI->GetBot() &&
                    botAI->DoSpecificAction("agent invite to group", Event("agent invite to group", "", target), true);
                PublishResult(command.requestId, command.operationId, command.operation,
                              invited ? "completed" : "rejected", invited ? "invite sent" : "player unavailable");
                return;
            }
            if (command.operation == "accept_group_invite")
            {
                WorldPacket invite;
                {
                    std::lock_guard<std::mutex> lock(inviteMutex);
                    if (!invitePending)
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                      "no invitation pending");
                        return;
                    }
                    invite = invitePacket;
                }
                bool const accepted =
                    botAI->DoSpecificAction("accept invitation", Event("accept invitation", invite), true);
                if (accepted)
                {
                    std::lock_guard<std::mutex> lock(inviteMutex);
                    invitePending = false;
                    invitePacket = WorldPacket();
                    SuppressAutonomousStrategies(botAI);
                }
                PublishResult(command.requestId, command.operationId, command.operation,
                              accepted ? "completed" : "rejected", accepted ? "invite accepted" : "accept failed");
                return;
            }
            if (command.operation == "decline_group_invite")
            {
                Group* invitedGroup = botAI->GetBot()->GetGroupInvite();
                if (!invitedGroup)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "no invitation pending");
                    return;
                }
                if (Player* inviter = ObjectAccessor::FindPlayer(invitedGroup->GetLeaderGUID()))
                {
                    WorldPacket decline(SMSG_GROUP_DECLINE, 10);
                    decline << botAI->GetBot()->GetName();
                    inviter->SendDirectMessage(&decline);
                }
                botAI->GetBot()->UninviteFromGroup();
                {
                    std::lock_guard<std::mutex> lock(inviteMutex);
                    invitePending = false;
                    invitePacket = WorldPacket();
                }
                PublishResult(command.requestId, command.operationId, command.operation, "completed",
                              "invite declined");
                return;
            }
            if (command.operation == "craft_once")
            {
                uint32 const spellId = GetUInt(arguments, "spell_id");
                uint32 itemId = 0;
                if (!CraftOutput(botAI->GetBot(), spellId, itemId) || itemId != GetUInt(arguments, "item_id"))
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "a known deterministic trade recipe and matching output are required");
                    return;
                }
                StartOperation(botAI, command);
                localOperation->serviceId = spellId;
                localOperation->craftItemId = itemId;
                return;
            }
            if (command.operation == "fish_once")
            {
                Player* bot = botAI->GetBot();
                Item* pole = bot->GetItemByPos(INVENTORY_SLOT_BAG_0, EQUIPMENT_SLOT_MAINHAND);
                bool const validPole = pole && pole->GetTemplate()->Class == ITEM_CLASS_WEAPON &&
                                       pole->GetTemplate()->SubClass == ITEM_SUBCLASS_WEAPON_FISHING_POLE;
                if (!validPole || !bot->HasSpell(FISHING_SPELL) || !bot->GetSkillValue(SKILL_FISHING) ||
                    bot->GetGameObject(FISHING_SPELL))
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "fishing requires learned skill, an equipped pole, and no existing bobber");
                    return;
                }
                StartOperation(botAI, command);
                return;
            }
            if (command.operation == "collect_mail" || command.operation == "send_mail")
            {
                ObjectGuid const mailboxGuid(GetUInt64(arguments, "target_guid"));
                GameObject* mailbox = botAI->GetGameObject(mailboxGuid);
                uint32 const limit = GetUInt(arguments, "count");
                if (!mailbox || mailbox->GetGoType() != GAMEOBJECT_TYPE_MAILBOX || !mailbox->isSpawned() ||
                    limit == 0 || limit > AGENT_MAX_SNAPSHOT_ITEMS)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid mailbox collection request");
                    return;
                }
                StartOperation(botAI, command, mailboxGuid);
                localOperation->serviceLimit = limit;
                localOperation->mailItemGuid = ObjectGuid(GetUInt64(arguments, "collect_item_guid"));
                localOperation->auctionMailFilter = {GetUInt(arguments, "collect_auction_id"),
                                                     GetUInt(arguments, "item_id"), GetUInt(arguments, "item_count"),
                                                     GetUInt(arguments, "bid_copper")};
                if (localOperation->auctionMailFilter.AuctionId &&
                    (!localOperation->auctionMailFilter.ItemId || !localOperation->auctionMailFilter.Count ||
                     !localOperation->auctionMailFilter.Bid || localOperation->mailItemGuid.IsEmpty()))
                {
                    FinishOperation(botAI, false, "invalid auction proceeds request");
                    return;
                }
                if (command.operation == "send_mail")
                {
                    localOperation->mailRecipient = GetString(arguments, "recipient");
                    localOperation->mailSubject = GetString(arguments, "subject");
                    localOperation->mailBody = GetString(arguments, "body");
                    localOperation->mailItemGuid = ObjectGuid(GetUInt64(arguments, "item_guid"));
                    localOperation->mailMoney = GetUInt(arguments, "money_copper");
                    localOperation->serviceBudget = GetUInt(arguments, "max_spend_copper");
                    auto const validText = [](std::string const& text, size_t maximum)
                    { return text.size() <= maximum && text.find('\0') == std::string::npos; };
                    if (localOperation->mailRecipient.empty() || !validText(localOperation->mailRecipient, 48) ||
                        !validText(localOperation->mailSubject, 128) || !validText(localOperation->mailBody, 4096))
                        FinishOperation(botAI, false, "invalid mail text or recipient");
                }
                return;
            }
            if (IsNPCService(command.operation))
            {
                ObjectGuid const npcGuid(GetUInt64(arguments, "target_guid"));
                Creature* npc = botAI->GetCreature(npcGuid);
                uint32 const limit = GetUInt(arguments, "count");
                uint32 const budget = GetUInt(arguments, "max_spend_copper");
                uint32 const itemId =
                    GetUInt(arguments, command.operation == "take_flight" ? "destination_node" : "item_id");
                if (!npc || !npc->IsAlive() || !npc->HasNpcFlag(NPCServiceFlag(command.operation)) || limit == 0 ||
                    limit > AGENT_MAX_SNAPSHOT_ITEMS ||
                    ((command.operation == "buy_vendor_item" || command.operation == "inspect_auctions" ||
                      command.operation == "buy_auction" || command.operation == "bid_auction") &&
                     !itemId) ||
                    ((command.operation == "buy_auction" || command.operation == "bid_auction") &&
                     (!GetUInt(arguments, "auction_id") || !GetUInt(arguments, "buyout_copper"))))
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid NPC service request");
                    return;
                }
                StartOperation(botAI, command, npcGuid);
                localOperation->serviceId = itemId;
                localOperation->serviceLimit = limit;
                localOperation->serviceBudget = budget;
                localOperation->auctionId =
                    command.operation == "inspect_auctions" ? 0 : GetUInt(arguments, "auction_id");
                localOperation->auctionCursor = GetUInt(arguments, "cursor");
                localOperation->auctionBuyout = GetUInt(arguments, "buyout_copper");
                localOperation->auctionItemGuid = ObjectGuid(GetUInt64(arguments, "item_guid"));
                return;
            }
            if (command.operation == "interact_quest_giver")
            {
                ObjectGuid giverGuid(GetUInt64(arguments, "target_guid"));
                uint32 const questId = GetUInt(arguments, "quest_id");
                Creature* giver = botAI->GetCreature(giverGuid);
                if (!giverGuid)
                {
                    // The controller's capped NPC projection can omit the receiver.
                    // Resolve this quest's eligible NPC from the native nearby cache.
                    GuidVector const nearby = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest npcs")->Get();
                    float distance = std::numeric_limits<float>::max();
                    for (ObjectGuid const guid : nearby)
                    {
                        Creature* candidate = botAI->GetCreature(guid);
                        if (!candidate || !candidate->IsAlive() || !candidate->HasNpcFlag(UNIT_NPC_FLAG_QUESTGIVER) ||
                            !candidate->hasInvolvedQuest(questId))
                            continue;
                        float const candidateDistance = botAI->GetBot()->GetDistance(candidate);
                        if (candidateDistance < distance)
                        {
                            giver = candidate;
                            giverGuid = guid;
                            distance = candidateDistance;
                        }
                    }
                }
                if (!giver || !giver->IsAlive() || !giver->HasNpcFlag(UNIT_NPC_FLAG_QUESTGIVER) ||
                    !giver->hasInvolvedQuest(questId) ||
                    botAI->GetBot()->GetQuestStatus(questId) != QUEST_STATUS_COMPLETE)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "quest giver or completed quest unavailable");
                    return;
                }
                StartOperation(botAI, command, giverGuid, 2.0f, questId);
                return;
            }
            if (command.operation == "use_gameobject")
            {
                ObjectGuid const objectGuid(GetUInt64(arguments, "target_guid"));
                GameObject* object = botAI->GetGameObject(objectGuid);
                bool const usable = object && object->isSpawned() &&
                                    object->GetMapId() == botAI->GetBot()->GetMapId() &&
                                    botAI->GetBot()->IsWithinDist(object, sPlayerbotAIConfig.sightDistance);
                if (usable)
                    StartOperation(botAI, command, objectGuid);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "object unavailable", objectGuid);
                return;
            }
            if (command.operation == "navigate_to_position")
            {
                uint32 const mapId = GetUInt(arguments, "map_id");
                float const x = arguments.get<float>("x");
                float const y = arguments.get<float>("y");
                float const z = arguments.get<float>("z");
                constexpr float coordinateLimit = 20000.0f;
                if (mapId != botAI->GetBot()->GetMapId() || !std::isfinite(x) || !std::isfinite(y) ||
                    !std::isfinite(z) || std::abs(x) > coordinateLimit || std::abs(y) > coordinateLimit ||
                    std::abs(z) > coordinateLimit)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid same-map navigation position");
                    return;
                }
                StartOperation(botAI, command, ObjectGuid::Empty, 10.0f);
                localOperation->navigationPosition.Relocate(x, y, z);
                return;
            }
            if (command.operation == "navigate_to_destination")
            {
                std::string const name = GetString(arguments, "destination");
                TravelDestination* destination =
                    ChooseTravelTargetAction::FindDestination(botAI->GetBot(), name, true, true, true, true, true);
                WorldPosition position(botAI->GetBot());
                std::vector<WorldPosition*> points =
                    destination ? destination->nextPoint(&position, true) : std::vector<WorldPosition*>();
                bool const started = !points.empty() && SetTravelTarget(botAI, destination, points.front());
                if (started)
                    StartOperation(botAI, command);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "destination unavailable");
                return;
            }
            if (command.operation == "navigate_to_quest_objective" || command.operation == "navigate_to_quest_turnin" ||
                command.operation == "navigate_to_quest_giver")
            {
                uint32 const questId = GetUInt(arguments, "quest_id");
                uint32 const objectiveIndex = GetUInt(arguments, "objective_index");
                bool const turnIn = command.operation == "navigate_to_quest_turnin";
                bool const start = command.operation == "navigate_to_quest_giver";
                if (!turnIn && !start)
                {
                    std::string const block =
                        QuestObjectiveNavigationBlockReason(botAI->GetBot(), sObjectMgr->GetQuestTemplate(questId));
                    if (!block.empty())
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "rejected", block);
                        return;
                    }
                }
                TravelDestination* destination = FindQuestDestination(botAI->GetBot(), questId, objectiveIndex,
                                                                      GetUInt(arguments, "item_id"), turnIn, start);
                WorldPosition* nearest = FindQuestNavigationPoint(botAI->GetBot(), destination);
                bool const started = nearest && SetTravelTarget(botAI, destination, nearest);
                if (started)
                    StartOperation(botAI, command);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "quest destination unavailable");
                return;
            }
            if (command.operation == "accept_quest")
            {
                uint32 const questId = GetUInt(arguments, "quest_id");
                Quest const* quest = sObjectMgr->GetQuestTemplate(questId);
                Player* bot = botAI->GetBot();
                ObjectGuid giverGuid;
                if (quest && bot->GetQuestStatus(questId) == QUEST_STATUS_NONE && bot->CanTakeQuest(quest, false) &&
                    bot->SatisfyQuestLog(false) && bot->CanAddQuest(quest, false))
                {
                    GuidVector const nearby = botAI->GetAiObjectContext()->GetValue<GuidVector>("nearest npcs")->Get();
                    for (ObjectGuid const guid : nearby)
                    {
                        Creature* giver = botAI->GetCreature(guid);
                        if (giver && giver->IsAlive() && giver->HasNpcFlag(UNIT_NPC_FLAG_QUESTGIVER) &&
                            giver->hasQuest(questId))
                        {
                            giverGuid = guid;
                            break;
                        }
                    }
                }
                if (giverGuid)
                    StartOperation(botAI, command, giverGuid, 2.0f, questId);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "quest not available to take");
                return;
            }
            if (command.operation == "navigate_to_player" || command.operation == "follow_player")
            {
                Player* target = nullptr;
                std::string const targetGuidRaw = GetString(arguments, "target_guid");
                if (!targetGuidRaw.empty())
                    target = ObjectAccessor::FindPlayer(ObjectGuid(GetUInt64(arguments, "target_guid")));
                if (!target)
                    target = ObjectAccessor::FindPlayerByName(GetString(arguments, "player_name"));
                if (!target || target == botAI->GetBot() || target->GetMapId() != botAI->GetBot()->GetMapId())
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "player unavailable or on another map");
                    return;
                }
                float const distance = arguments.get<float>("distance", 5.0f);
                if (!std::isfinite(distance) || distance < 1.0f || distance > 50.0f)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid movement distance");
                    return;
                }
                StartOperation(botAI, command, target->GetGUID(), distance);
                return;
            }
            if (command.operation == "approach_target")
            {
                ObjectGuid const targetGuid(GetUInt64(arguments, "target_guid"));
                Unit* target = botAI->GetUnit(targetGuid);
                bool const valid =
                    target && target->IsInWorld() && target->IsAlive() && botAI->GetBot()->IsValidAttackTarget(target);
                if (valid)
                    StartOperation(botAI, command, targetGuid);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "combat target approach unavailable", targetGuid);
                return;
            }
            if (command.operation == "engage_target" || command.operation == "loot_target" ||
                command.operation == "gather_target")
            {
                ObjectGuid const requestedTarget(GetUInt64(arguments, "target_guid"));
                bool const corpseOnly = command.operation == "loot_target" && arguments.get<bool>("corpse_only", false);
                if (!requestedTarget)
                {
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "invalid target GUID");
                    return;
                }
                if (command.operation == "engage_target")
                {
                    Unit* target = botAI->GetUnit(requestedTarget);
                    if (!target || !target->IsInWorld() || !target->IsAlive() ||
                        !botAI->GetBot()->IsValidAttackTarget(target))
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                      "target is not attackable");
                        return;
                    }
                    StartOperation(botAI, command, requestedTarget);
                    return;
                }

                if (corpseOnly)
                {
                    Creature* corpse = botAI->GetCreature(requestedTarget);
                    if (!corpse || !corpse->IsInWorld() || corpse->IsAlive() ||
                        corpse->GetMap() != botAI->GetBot()->GetMap())
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                      "corpse target unavailable", requestedTarget);
                        return;
                    }
                    if (!corpse->HasFlag(UNIT_DYNAMIC_FLAGS, UNIT_DYNFLAG_LOOTABLE))
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "completed",
                                      "corpse has no remaining loot", requestedTarget, true);
                        return;
                    }
                }
                LootObject loot(botAI->GetBot(), requestedTarget);
                if (!loot.IsLootPossible(botAI->GetBot()))
                {
                    Unit* corpse = botAI->GetUnit(requestedTarget);
                    if (command.operation == "loot_target" && corpse && !corpse->IsAlive() &&
                        !corpse->HasFlag(UNIT_DYNAMIC_FLAGS, UNIT_DYNFLAG_LOOTABLE))
                    {
                        PublishResult(command.requestId, command.operationId, command.operation, "completed",
                                      "corpse has no remaining loot", requestedTarget, true);
                        return;
                    }
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "target is not available to loot or gather");
                    return;
                }
                botAI->GetAiObjectContext()->GetValue<LootObjectStack*>("available loot")->Get()->Add(requestedTarget);
                StartOperation(botAI, command, requestedTarget, 2.0f, 0, corpseOnly);
                return;
            }

            PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                          "unsupported operation");
        }
        catch (std::exception const& exception)
        {
            PublishResult(command.requestId, command.operationId, command.operation, "rejected", exception.what());
        }
    }

    void SendChat(PlayerbotAI* botAI, boost::property_tree::ptree const& arguments, AgentBridgeCommand const& command)
    {
        std::string message = GetString(arguments, "message");
        std::string const channel = GetString(arguments, "channel", "say");
        if (message.size() > sPlayerbotAIConfig.agentBridgeMaxMessageLength)
            message = TruncateUtf8(std::move(message), sPlayerbotAIConfig.agentBridgeMaxMessageLength);
        bool sent = false;
        sendingAgentChat = true;
        if (channel == "say")
            sent = botAI->Say(message);
        else if (channel == "yell")
            sent = botAI->Yell(message);
        else if (channel == "whisper")
            sent = botAI->Whisper(message, GetString(arguments, "recipient"));
        else if (channel == "party")
            sent = botAI->SayToPartyFromAgent(message);
        else if (channel == "raid")
            sent = botAI->SayToRaidFromAgent(message);
        else if (channel == "guild")
            sent = botAI->SayToGuild(message);
        else if (channel == "officer")
            sent = botAI->SayToGuildOfficers(message);
        else if (channel == "world")
            sent = botAI->SayToWorld(message);
        else if (channel == "general")
            sent = botAI->SayToChannel(message, GENERAL);
        else if (channel == "trade")
            sent = botAI->SayToChannel(message, TRADE);
        else if (channel == "lfg")
            sent = botAI->SayToChannel(message, LOOKING_FOR_GROUP);
        else if (channel == "local_defense")
            sent = botAI->SayToChannel(message, LOCAL_DEFENSE);
        else if (channel == "world_defense")
            sent = botAI->SayToChannel(message, WORLD_DEFENSE);
        else if (channel == "guild_recruitment")
            sent = botAI->SayToChannel(message, GUILD_RECRUITMENT);
        sendingAgentChat = false;
        if (sent)
            Publish("chat_sent", command.requestId,
                    "{\"channel\":\"" + EscapeJson(channel) + "\",\"recipient\":\"" +
                        EscapeJson(GetString(arguments, "recipient")) + "\",\"message\":\"" + EscapeJson(message) +
                        "\"}");
        PublishResult(command.requestId, command.operationId, command.operation, sent ? "completed" : "rejected",
                      sent ? "chat sent" : "channel unavailable");
    }
};

AgentRuntime::AgentRuntime(ObjectGuid botGuid) : m_impl(std::make_unique<Impl>(botGuid))
{
    m_impl->eventSequence = static_cast<uint64>(getMSTime()) << 32;
    m_impl->transportReady = AgentBridgeTransport::EnsureSubscribed(m_impl->subjectPrefix, m_impl->ownerToken);
    m_impl->lastSubscribeCheckMs = getMSTime();
    if (!m_impl->transportReady)
        LOG_WARN("playerbots.agent", "ToCloud9 bridge unavailable for bot {}", botGuid.ToString().c_str());
}

AgentRuntime::~AgentRuntime()
{
    if (m_impl)
    {
        if (m_impl->localOperation && m_impl->localOperation->mailboxWork)
            m_impl->localOperation->mailboxWork->Cancelled.store(true);
        if (m_impl->initialized && !m_impl->stopped)
            m_impl->Publish("bot_offline", "", "{}");
        AgentBridgeTransport::Unregister(m_impl->botGuidToken, m_impl->inbox);
    }
}

void AgentRuntime::OnLootResponse(WorldPacket const& packet)
{
    // SendPacket hooks can run outside the bot's map tick. Only read the atomic
    // target and the packet here; Publish protects its outbound state with a mutex.
    uint64 const expected = m_impl->gatheringObservationTarget.load(std::memory_order_acquire);
    if (!expected || packet.size() < 14)
        return;
    try
    {
        WorldPacket response(packet);
        response.rpos(0);
        ObjectGuid source;
        uint8 itemCount = 0;
        response >> source;
        response.read_skip<uint8>();   // loot type
        response.read_skip<uint32>();  // money
        response >> itemCount;
        if (source.GetRawValue() != expected || (!source.IsGameObject() && !source.IsCreature()) || !source.GetEntry())
            return;
        std::set<uint32> observed;
        for (uint32 index = 0; index < std::min(uint32(itemCount), AGENT_MAX_OBSERVED_DROP_ITEMS); ++index)
        {
            uint32 itemId = 0;
            uint32 count = 0;
            uint8 slotType = 0;
            response.read_skip<uint8>();  // loot slot
            response >> itemId >> count;
            response.read_skip<uint32>();  // display id
            response.read_skip<uint32>();  // random suffix
            response.read_skip<uint32>();  // random property
            response >> slotType;
            if (itemId && count && (slotType == LOOT_SLOT_TYPE_ALLOW_LOOT || slotType == LOOT_SLOT_TYPE_OWNER))
                observed.insert(itemId);
        }
        if (observed.empty())
            return;
        std::string location;
        {
            std::lock_guard<std::mutex> guard(m_impl->gatheringLocationMutex);
            if (m_impl->gatheringLocationGuid != source)
                return;
            location = m_impl->gatheringLocationJson;
        }
        std::ostringstream payload;
        payload << "{\"source_kind\":\"" << (source.IsGameObject() ? "gameobject" : "creature")
                << "\",\"source_entry\":" << source.GetEntry() << ",\"item_ids\":[";
        bool first = true;
        for (uint32 const itemId : observed)
        {
            if (!first)
                payload << ",";
            first = false;
            payload << itemId;
        }
        payload << "]" << location << "}";
        m_impl->Publish("gathering_source_observed", "", payload.str());
    }
    catch (std::exception const&)
    {
        LOG_DEBUG("playerbots.agent", "Ignored malformed gathering loot observation for {}",
                  m_impl->botGuid.ToString());
    }
}

void AgentRuntime::OnAuctionBidderNotification(WorldPacket const& packet)
{
    if (!m_impl->remoteControlActive.load(std::memory_order_acquire) || packet.size() < 28)
        return;
    WorldPacket notification(packet);
    notification.rpos(0);
    uint32 auctionId = 0, amount = 0, itemId = 0;
    ObjectGuid bidder;
    notification.read_skip<uint32>();  // house
    notification >> auctionId >> bidder >> amount;
    notification.read_skip<uint32>();  // next-bid increment
    notification >> itemId;
    std::ostringstream payload;
    payload << "{\"auction_id\":" << auctionId << ",\"item_id\":" << itemId << ",\"bid_copper\":" << amount
            << ",\"own_bidder\":" << (bidder == m_impl->botGuid ? "true" : "false") << "}";
    m_impl->Publish("auction_bid_notification", "", payload.str());
}

bool AgentRuntime::IsSupported()
{
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    return true;
#else
    return false;
#endif
}

bool AgentRuntime::IsControllerPresent()
{
    uint32 const last = lastControllerHeartbeatMs.load(std::memory_order_acquire);
    return last && getMSTimeDiff(last, getMSTime()) < AGENT_CONTROLLER_TIMEOUT_MS;
}

bool AgentRuntime::EnsureBotCommandSubscription(std::string const& subjectPrefix, std::string const& ownerToken)
{
    return AgentBridgeTransport::EnsureSubscribed(subjectPrefix, ownerToken);
}

bool AgentRuntime::IsConfigured(PlayerbotAI* botAI) const
{
    if (!botAI || !botAI->GetBot() || !sPlayerbotAIConfig.agentBridgeEnabled || !IsSupported())
        return false;
    return sPlayerbotAIConfig.agentBridgeBotGuid == 0 ||
           botAI->GetBot()->GetGUID().GetCounter() == sPlayerbotAIConfig.agentBridgeBotGuid;
}

bool AgentRuntime::IsEnabled(PlayerbotAI* botAI) const
{
    return IsConfigured(botAI) && m_impl->remoteControlActive.load(std::memory_order_acquire) &&
           AgentBridgeTransport::ControllerRecent(m_impl->ownerToken, m_impl->eventShard);
}

bool AgentRuntime::IsSendingChat() const { return m_impl->sendingAgentChat; }

bool AgentRuntime::IsStopped() const { return m_impl->stopped; }

void AgentRuntime::Stop(PlayerbotAI* botAI)
{
    if (!botAI || !botAI->GetBot() || m_impl->stopped)
        return;
    m_impl->ClearOperation(botAI);
    if (!botAI->GetBot()->IsInCombat())
        botAI->GetBot()->StopMoving();
    // No autonomous strategy restore: bots without a controller log out
    // instead of falling back to legacy AI.
    m_impl->remoteControlActive.store(false, std::memory_order_release);
    m_impl->Publish("bot_offline", "", "{}");
    m_impl->stopped = true;
}

void AgentRuntime::OnChatMessage(PlayerbotAI* botAI, uint8 type, uint32 language, ObjectGuid sender,
                                 std::string const& senderName, std::string const& channel, std::string const& message)
{
    if (!IsConfigured(botAI) || m_impl->stopped)
        return;

    // Temporary diagnostic: whisper-only, no message text.
    if (type == CHAT_MSG_WHISPER)
        LOG_INFO("playerbots.agent", "Whisper OnChatMessage bot={} sender_name={}", botAI->GetBot()->GetName(),
                 senderName);

    std::ostringstream payload;
    payload << "{\"channel_type\":" << static_cast<uint32>(type) << ",\"chat_type\":\"" << ChatTypeName(type)
            << "\",\"language\":" << language << ",\"channel\":\"" << EscapeJson(channel) << "\",\"sender_guid\":\""
            << EscapeJson(AgentBridgeTransport::BotToken(sender)) << "\",\"sender_name\":\"" << EscapeJson(senderName)
            << "\",\"message\":\"" << EscapeJson(message) << "\"}";
    m_impl->Publish("chat_received", "", payload.str());
}

void AgentRuntime::OnGroupInvite(PlayerbotAI* botAI, WorldPacket const& packet)
{
    if (!IsEnabled(botAI) || m_impl->stopped)
        return;

    WorldPacket copy(packet);
    copy.rpos(0);
    uint8 inviteType = 0;
    std::string inviterName;
    copy >> inviteType >> inviterName;
    {
        std::lock_guard<std::mutex> lock(m_impl->inviteMutex);
        m_impl->invitePacket = packet;
        m_impl->invitePending = true;
    }
    m_impl->Publish(
        "group_invite_received", "",
        "{\"inviter_name\":\"" + EscapeJson(inviterName) + "\",\"invite_type\":" + std::to_string(inviteType) + "}");
}

void AgentRuntime::OnQuestProgress(PlayerbotAI* botAI, uint32 opcode)
{
    if (!IsConfigured(botAI) || m_impl->stopped)
        return;

    m_impl->Publish("quest_progress", "", "{\"opcode\":" + std::to_string(opcode) + "}");
}

uint64 AgentRuntime::GetTradeRevision() const { return m_impl->tradeRevision.load(); }

void AgentRuntime::OnStrategiesReset(PlayerbotAI* botAI)
{
    if (!IsEnabled(botAI))
        return;
    m_impl->SuppressAutonomousStrategies(botAI);
    if (m_impl->addedTravelStrategy)
        botAI->ChangeStrategy("+travel", BOT_STATE_NON_COMBAT);
}

void AgentRuntime::OnTradeOperationResult(std::string const& requestId, std::string const& operationId,
                                          std::string const& operation, bool success)
{
    std::ostringstream payload;
    payload << "{\"operation_id\":\"" << EscapeJson(operationId) << "\",\"operation\":\"" << EscapeJson(operation)
            << "\",\"status\":\"" << (success ? "completed" : "rejected") << "\",\"reason\":\""
            << (success ? "trade state updated" : "trade changed or operation unavailable; inspect fresh state")
            << "\"}";
    m_impl->Publish("operation_result", requestId, payload.str());
}

void AgentRuntime::OnTradeStatus(PlayerbotAI* botAI, WorldPacket const& packet)
{
    if (!IsEnabled(botAI) || m_impl->stopped)
        return;
    ++m_impl->tradeRevision;
    WorldPacket copy(packet);
    copy.rpos(0);
    uint32 status = 0;
    if (packet.GetOpcode() == SMSG_TRADE_STATUS)
    {
        copy >> status;
        if (status == TRADE_STATUS_OPEN_WINDOW)
            m_impl->tradeWindowOpen.store(true);
        else if (status == TRADE_STATUS_BEGIN_TRADE)
        {
            m_impl->tradeWindowOpen.store(false);
            Player* bot = botAI->GetBot();
            if (Player* partner = bot->GetTrader())
                PlayerbotWorldThreadProcessor::instance().QueueOperation(std::make_unique<AgentTradeOperation>(
                    bot->GetGUID(), partner->GetGUID(), m_impl->tradeRevision.load(), "begin_trade", 0, 0,
                    ObjectGuid::Empty, "trade-open-" + std::to_string(m_impl->tradeRevision.load()), ""));
        }
        else if (status == TRADE_STATUS_TRADE_CANCELED || status == TRADE_STATUS_TRADE_COMPLETE ||
                 status == TRADE_STATUS_CLOSE_WINDOW || status == TRADE_STATUS_TRADE_REJECTED)
            m_impl->tradeWindowOpen.store(false);
    }
    m_impl->Publish("trade_updated", "", "{\"status\":" + std::to_string(status) + "}");
}

void AgentRuntime::Update(PlayerbotAI* botAI, uint32 elapsed)
{
    if (!IsConfigured(botAI) || !botAI->GetBot()->IsInWorld())
        return;

    Player* bot = botAI->GetBot();
    uint32 const now = getMSTime();
    if (m_impl->stopped)
        return;
    if (!m_impl->transportReady && getMSTimeDiff(m_impl->lastSubscribeCheckMs, now) >= 5000)
    {
        m_impl->lastSubscribeCheckMs = now;
        m_impl->transportReady = AgentBridgeTransport::EnsureSubscribed(m_impl->subjectPrefix, m_impl->ownerToken);
    }
    if (!m_impl->transportReady)
        return;
    m_impl->FlushEvents();
    if (!m_impl->initialized)
    {
        m_impl->initialized = true;
        m_impl->Publish("bot_online", "",
                        "{\"map_id\":" + std::to_string(bot->GetMapId()) +
                            ",\"zone_id\":" + std::to_string(bot->GetZoneId()) + "}");
    }

    Group* group = bot->GetGroup();
    uint32 const groupMembers = group ? group->GetMembersCount() : 0;
    ObjectGuid const leader = group ? group->GetLeaderGUID() : ObjectGuid::Empty;
    if (!m_impl->groupInitialized)
    {
        m_impl->lastGroupMembers = groupMembers;
        m_impl->lastGroupLeader = leader;
        m_impl->groupInitialized = true;
    }
    else if (m_impl->lastGroupMembers != groupMembers || m_impl->lastGroupLeader != leader)
    {
        m_impl->lastGroupMembers = groupMembers;
        m_impl->lastGroupLeader = leader;
        if (m_impl->remoteControlActive.load() && botAI->HasStrategy("follow", BOT_STATE_NON_COMBAT))
            botAI->ChangeStrategy("-follow", BOT_STATE_NON_COMBAT);
        m_impl->Publish("group_changed", "",
                        "{\"members\":" + std::to_string(groupMembers) + ",\"leader_guid\":\"" +
                            EscapeJson(AgentBridgeTransport::BotToken(leader)) + "\"}");
    }
    if (!m_impl->locationInitialized)
    {
        m_impl->lastMapId = bot->GetMapId();
        m_impl->lastZoneId = bot->GetZoneId();
        m_impl->locationInitialized = true;
    }
    else if (m_impl->lastMapId != bot->GetMapId() || m_impl->lastZoneId != bot->GetZoneId())
    {
        m_impl->lastMapId = bot->GetMapId();
        m_impl->lastZoneId = bot->GetZoneId();
        m_impl->Publish("location_changed", "",
                        "{\"map_id\":" + std::to_string(bot->GetMapId()) +
                            ",\"zone_id\":" + std::to_string(bot->GetZoneId()) + "}");
    }

    if (!m_impl->lastHeartbeatMs || getMSTimeDiff(m_impl->lastHeartbeatMs, now) >= AGENT_HEARTBEAT_MS)
    {
        m_impl->lastHeartbeatMs = now;
        m_impl->Publish("bot_heartbeat", "",
                        "{\"map_id\":" + std::to_string(bot->GetMapId()) +
                            ",\"zone_id\":" + std::to_string(bot->GetZoneId()) + ",\"bridge_active\":" +
                            (m_impl->remoteControlActive.load(std::memory_order_acquire) ? "true" : "false") + "}");
    }

    bool const controllerAvailable = AgentBridgeTransport::ControllerRecent(m_impl->ownerToken, m_impl->eventShard);
    bool const remoteControlActive = m_impl->remoteControlActive.load(std::memory_order_acquire);
    if (controllerAvailable && !remoteControlActive)
    {
        m_impl->SuppressAutonomousStrategies(botAI);
        m_impl->remoteControlActive.store(true, std::memory_order_release);
        m_impl->Publish("agent_control_active", "", "{}");
    }
    else if (!controllerAvailable && remoteControlActive)
    {
        // Losing the controller never restores autonomous strategies; the bot
        // idles here and UpdateAIInternal logs it out on its next tick.
        m_impl->ClearOperation(botAI);
        if (!bot->IsInCombat())
            bot->StopMoving();
        m_impl->remoteControlActive.store(false, std::memory_order_release);
        m_impl->Publish("agent_control_inactive", "", "{}");
    }

    if (!m_impl->remoteControlActive.load(std::memory_order_acquire))
    {
        // Social directory reconstruction must not require model ownership.
        // Read-only snapshots are bounded; all task/mutation commands stay disabled.
        for (uint32 processed = 0; processed < 8; ++processed)
        {
            AgentBridgeCommand command;
            if (!AgentBridgeTransport::Pop(m_impl->inbox, command))
                break;
            if (command.operation != "snapshot")
                continue;
            constexpr uint32 PASSIVE_SNAPSHOT_INTERVAL_MS = 5000;
            if (m_impl->lastPassiveSnapshotMs &&
                getMSTimeDiff(m_impl->lastPassiveSnapshotMs, now) < PASSIVE_SNAPSHOT_INTERVAL_MS)
                continue;
            m_impl->lastPassiveSnapshotMs = now;
            m_impl->ExecuteCommand(botAI, command);
        }
        return;
    }

    for (uint32 processed = 0; processed < 8; ++processed)
    {
        AgentBridgeCommand command;
        if (!AgentBridgeTransport::Pop(m_impl->inbox, command))
            break;
        m_impl->ExecuteCommand(botAI, command);
    }
    if (bot->GetTradeData())
    {
        if (!m_impl->tradeMovementSuspended)
        {
            // Suspend physical movement, but keep the task/operation to resume.
            bot->GetMotionMaster()->Clear();
            bot->StopMoving();
            m_impl->tradeMovementSuspended = true;
        }
    }
    else
        m_impl->tradeMovementSuspended = false;

    m_impl->UpdateOperation(botAI, elapsed);
}
