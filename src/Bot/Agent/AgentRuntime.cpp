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

#include "AgentBridgeShared.h"
#include "AgentRuntime.h"
#include "AgentTradeOperation.h"
#include "AiObjectContext.h"
#include "ChooseTravelTargetAction.h"
#include "Creature.h"
#include "Duration.h"
#include "Event.h"
#include "EventMap.h"
#include "GameObject.h"
#include "Group.h"
#include "Item.h"
#include "LootObjectStack.h"
#include "MotionMaster.h"
#include "MovementActions.h"
#include "ObjectAccessor.h"
#include "ObjectMgr.h"
#include "Player.h"
#include "PlayerbotAI.h"
#include "PlayerbotAIConfig.h"
#include "PlayerbotWorldThreadProcessor.h"
#include "Playerbots.h"
#include "QuestDef.h"
#include "SharedDefines.h"
#include "Timer.h"
#include "TravelMgr.h"
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
constexpr uint32 AGENT_MAX_SNAPSHOT_CREATURES = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_GAMEOBJECTS = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_PLAYERS = 12;
constexpr uint32 AGENT_MAX_SNAPSHOT_ITEMS = 40;
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
            HeartbeatTimes()[eventShard].store(getMSTime(), std::memory_order_release);
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

TravelDestination* FindQuestDestination(Player* bot, uint32 questId, uint32 objectiveIndex, bool turnIn)
{
    QuestStatus const status = bot->GetQuestStatus(questId);
    bool const completed = status == QUEST_STATUS_COMPLETE;
    if (turnIn != completed)
        return nullptr;

    std::vector<TravelDestination*> destinations =
        TravelMgr::instance().getQuestTravelDestinations(bot, questId, true, true, 0.0f, false);
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
            if (!objective || objective->GetObjectiveIndex() != objectiveIndex || !objective->isActive(bot))
                continue;
        }
        if (destination->distanceTo(&position) < bestDistance)
        {
            bestDistance = destination->distanceTo(&position);
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
        float distance = 2.0f;
        bool attempted = false;
        bool movementStarted = false;
        bool paused = false;
        uint32 lastProgressMs = 0;
        uint32 lastReportMs = 0;
        uint32 lastInteractionMs = 0;
        uint32 lastMoveAttemptMs = 0;
        uint32 health = 0;
        Position position;
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
    std::atomic<bool> tradeWindowOpen{false};
    bool tradeMovementSuspended = false;
    WorldPacket invitePacket;
    std::mutex inviteMutex;
    bool initialized = false;
    std::atomic<bool> remoteControlActive{false};
    std::vector<std::string> suppressedLegacyStrategies;
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
            if (std::find(suppressedLegacyStrategies.begin(), suppressedLegacyStrategies.end(), strategy) ==
                suppressedLegacyStrategies.end())
                suppressedLegacyStrategies.push_back(strategy);
            if (!changes.empty())
                changes += ",";
            changes += "-" + strategy;
        }
        if (!changes.empty())
            botAI->ChangeStrategy(changes, BOT_STATE_NON_COMBAT);
    }

    void RestoreAutonomousStrategies(PlayerbotAI* botAI)
    {
        std::string changes;
        for (std::string const& strategy : suppressedLegacyStrategies)
        {
            if (botAI->HasStrategy(strategy, BOT_STATE_NON_COMBAT))
                continue;
            if (!changes.empty())
                changes += ",";
            changes += "+" + strategy;
        }
        if (!changes.empty())
            botAI->ChangeStrategy(changes, BOT_STATE_NON_COMBAT);
        suppressedLegacyStrategies.clear();
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

    std::string BuildSnapshot(PlayerbotAI* botAI)
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
               << ",\"fishing\":" << bot->GetSkillValue(SKILL_FISHING)
               << "},\"inventory\":{\"money_copper\":" << bot->GetMoney() << ",\"items\":[";
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
                       << ",\"accepted\":" << (offer && offer->IsAccepted() ? "true" : "false") << ",\"items\":[";
                bool first = true;
                if (offer)
                    for (uint8 slot = 0; slot < TRADE_SLOT_TRADED_COUNT; ++slot)
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
                   << ",\"objectives\":[";
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
                result << "{\"item_id\":" << quest->RequiredItemId[index] << ",\"count\":" << status.ItemCount[index]
                       << ",\"required\":" << quest->RequiredItemCount[index] << "}";
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
                   << player->GetPositionZ() << "],\"is_bot\":" << (GET_PLAYERBOT_AI(player) ? "true" : "false") << "}";
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
                   << ",\"distance\":" << bot->GetDistance(creature) << "}";
            if (++npcCount >= AGENT_MAX_SNAPSHOT_PLAYERS)
                break;
        }
        result << "]}";
        return result.str();
    }

    void PublishSnapshot(PlayerbotAI* botAI, AgentBridgeCommand const& command)
    {
        Publish("snapshot", command.requestId, BuildSnapshot(botAI));
    }

    void ClearOperation(PlayerbotAI* botAI)
    {
        localOperation.reset();
        operationEvents.Reset();
        ClearTravelTarget(botAI);
        if (!botAI->GetBot()->IsInCombat())
        {
            botAI->GetBot()->GetMotionMaster()->Clear();
            botAI->GetBot()->StopMoving();
        }
    }

    void StartOperation(PlayerbotAI* botAI, AgentBridgeCommand const& command, ObjectGuid target = ObjectGuid::Empty,
                        float distance = 2.0f, uint32 questId = 0)
    {
        // Task replacement sends cancel before start. Clear the old local owner
        // here as well, so redelivery cannot leave competing movement loops.
        localOperation = std::make_unique<LocalOperation>();
        localOperation->command = command;
        localOperation->target = target;
        localOperation->mapId = botAI->GetBot()->GetMapId();
        localOperation->distance = distance;
        localOperation->questId = questId;
        localOperation->position.Relocate(botAI->GetBot());
        localOperation->lastProgressMs = getMSTime();
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
        bool const combat = operation == "engage_target";
        bool const follow = operation == "follow_player";
        bool const assist = operation == "assist_leader";
        bool const travelOperation = operation == "navigate_to_destination" ||
                                     operation == "navigate_to_quest_objective" ||
                                     operation == "navigate_to_quest_turnin";
        if (!bot->IsAlive() || (!travelOperation && bot->GetMapId() != loop.mapId))
        {
            FinishOperation(botAI, false, "bot died or left the operation map");
            return;
        }
        if (bot->GetTradeData() || (bot->IsInCombat() && !combat && !assist))
        {
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
        if (!botAI->CanMove() || bot->IsNonMeleeSpellCast(false))
            return;
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
            operation == "navigate_to_quest_turnin")
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
            return;
        }
        if (operation == "move_random")
        {
            if (loop.movementStarted && !bot->isMoving())
                FinishOperation(botAI, true, "search segment reached");
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
            FinishOperation(botAI, loop.attempted && (operation == "loot_target" || operation == "gather_target"),
                            "operation target disappeared");
            return;
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
        if (operation == "loot_target" || operation == "gather_target")
        {
            LootObject loot(bot, loop.target);
            if (!loot.IsLootPossible(bot))
            {
                FinishOperation(botAI, loop.attempted,
                                loop.attempted ? "loot or gather completed" : "loot unavailable");
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
                PublishSnapshot(botAI, command);
                return;
            }
            if (command.operation == "send_chat")
            {
                SendChat(botAI, arguments, command);
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
            if (command.operation == "interact_quest_giver")
            {
                ObjectGuid const giverGuid(GetUInt64(arguments, "target_guid"));
                uint32 const questId = GetUInt(arguments, "quest_id");
                Creature* giver = botAI->GetCreature(giverGuid);
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
            if (command.operation == "navigate_to_quest_objective" || command.operation == "navigate_to_quest_turnin")
            {
                uint32 const questId = GetUInt(arguments, "quest_id");
                uint32 const objectiveIndex = GetUInt(arguments, "objective_index");
                TravelDestination* destination = FindQuestDestination(botAI->GetBot(), questId, objectiveIndex,
                                                                      command.operation == "navigate_to_quest_turnin");
                WorldPosition position(botAI->GetBot());
                std::vector<WorldPosition*> points =
                    destination ? destination->nextPoint(&position, true) : std::vector<WorldPosition*>();
                bool const started = !points.empty() && SetTravelTarget(botAI, destination, points.front());
                if (started)
                    StartOperation(botAI, command);
                else
                    PublishResult(command.requestId, command.operationId, command.operation, "rejected",
                                  "quest destination unavailable");
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
                StartOperation(botAI, command, requestedTarget);
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
        if (m_impl->initialized && !m_impl->stopped)
            m_impl->Publish("bot_offline", "", "{}");
        AgentBridgeTransport::Unregister(m_impl->botGuidToken, m_impl->inbox);
    }
}

bool AgentRuntime::IsSupported()
{
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    return true;
#else
    return false;
#endif
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

void AgentRuntime::Stop(PlayerbotAI* botAI)
{
    if (!botAI || !botAI->GetBot() || m_impl->stopped)
        return;
    m_impl->ClearOperation(botAI);
    if (!botAI->GetBot()->IsInCombat())
        botAI->GetBot()->StopMoving();
    if (m_impl->remoteControlActive.load(std::memory_order_acquire))
    {
        m_impl->RestoreAutonomousStrategies(botAI);
        m_impl->remoteControlActive.store(false, std::memory_order_release);
    }
    m_impl->Publish("bot_offline", "", "{}");
    m_impl->stopped = true;
}

void AgentRuntime::OnChatMessage(PlayerbotAI* botAI, uint8 type, uint32 language, ObjectGuid sender,
                                 std::string const& senderName, std::string const& channel, std::string const& message)
{
    if (!IsConfigured(botAI) || m_impl->stopped)
        return;

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
        {
            if (std::find(m_impl->suppressedLegacyStrategies.begin(), m_impl->suppressedLegacyStrategies.end(),
                          "follow") == m_impl->suppressedLegacyStrategies.end())
                m_impl->suppressedLegacyStrategies.push_back("follow");
            botAI->ChangeStrategy("-follow", BOT_STATE_NON_COMBAT);
        }
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
        m_impl->ClearOperation(botAI);
        if (!bot->IsInCombat())
            bot->StopMoving();
        m_impl->RestoreAutonomousStrategies(botAI);
        m_impl->remoteControlActive.store(false, std::memory_order_release);
        m_impl->Publish("agent_control_inactive", "", "{}");
    }

    if (!m_impl->remoteControlActive.load(std::memory_order_acquire))
    {
        AgentBridgeTransport::Clear(m_impl->inbox);
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
