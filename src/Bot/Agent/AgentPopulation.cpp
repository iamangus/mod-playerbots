/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#include "AgentPopulation.h"
#include "AgentBridgeShared.h"
#include <boost/bind/placeholders.hpp>

namespace boost::property_tree::json_parser::detail
{
    using boost::placeholders::_1;
}

#include "AccountMgr.h"
#include "CharacterCache.h"
#include "DatabaseEnv.h"
#include "GameTime.h"
#include "Log.h"
#include "ObjectMgr.h"
#include "Player.h"
#include "PlayerbotAIConfig.h"
#include "Playerbots.h"
#include "PlayerbotsDatabase.h"
#include "RandomPlayerbotFactory.h"
#include "RandomPlayerbotMgr.h"
#include "SharedDefines.h"
#include "Timer.h"
#include "WorldSession.h"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include <deque>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace
{
constexpr size_t POPULATION_MAX_COMMAND_BYTES = 8192;
constexpr size_t POPULATION_MAX_PENDING_COMMANDS = 64;
constexpr size_t POPULATION_MAX_RESULT_CACHE = 128;
constexpr uint32 POPULATION_SUBSCRIBE_RETRY_MS = 5000;

struct PopulationCommand
{
    std::string requestId;
    std::string operation;
    std::string arguments;
};

struct PopulationInbox
{
    std::mutex mutex;
    std::deque<PopulationCommand> commands;
};

std::string PopulationBotToken()
{
    return "population";
}

std::string PopulationCommandSubject(std::string const& prefix, std::string const& ownerToken)
{
    return prefix + ".population.commands." + ownerToken;
}

std::string PopulationEventSubject(std::string const& prefix, std::string const& ownerToken)
{
    return prefix + ".population.events." + std::to_string(agent_bridge::EventShard(PopulationBotToken())) +
           "." + ownerToken + "." + PopulationBotToken();
}

std::string GetTreeString(boost::property_tree::ptree const& tree, std::string const& key,
                          std::string const& fallback = "")
{
    return tree.get<std::string>(key, fallback);
}

uint32 GetTreeUInt(boost::property_tree::ptree const& tree, std::string const& key, uint32 fallback = 0)
{
    return tree.get<uint32>(key, fallback);
}

std::string BuildEvent(std::string const& type, std::string const& ownerToken, uint64 sequence,
                       std::string const& requestId, std::string const& payload)
{
    int64 const eventTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string const botToken = PopulationBotToken();
    std::ostringstream event;
    event << "{\"version\":1,\"event_id\":\"" << agent_bridge::EscapeJson(botToken + "-" + std::to_string(sequence))
          << "\",\"timestamp_unix_ms\":" << eventTime << ",\"type\":\"" << agent_bridge::EscapeJson(type)
          << "\",\"bot_guid\":\"" << agent_bridge::EscapeJson(botToken) << "\",\"owner_token\":\""
          << agent_bridge::EscapeJson(ownerToken) << "\",\"owner_epoch\":\"\",\"request_id\":\""
          << agent_bridge::EscapeJson(requestId) << "\",\"payload\":"
          << (payload.empty() ? "{}" : payload) << "}";
    return event.str();
}
}

struct AgentPopulation::Impl
{
    std::string subjectPrefix = sPlayerbotAIConfig.agentBridgeSubjectPrefix;
    std::string ownerToken = agent_bridge::OwnerToken();
    PopulationInbox inbox;
    bool transportReady = false;
    uint32 lastSubscribeCheckMs = 0;
    uint64 eventSequence = 1;
    std::map<std::string, std::string> resultCache;
    std::deque<std::string> resultCacheOrder;
    uint32 lastSnapshotMs = 0;

    bool PublishEvent(std::string const& type, std::string const& requestId, std::string const& payload)
    {
        return agent_bridge::Publish(PopulationEventSubject(subjectPrefix, ownerToken),
                                     BuildEvent(type, ownerToken, eventSequence++, requestId, payload));
    }

    void CacheAndPublish(std::string const& requestId, std::string const& payload)
    {
        if (!requestId.empty())
        {
            if (!resultCache.contains(requestId))
                resultCacheOrder.push_back(requestId);
            resultCache[requestId] = payload;
            while (resultCacheOrder.size() > POPULATION_MAX_RESULT_CACHE)
            {
                resultCache.erase(resultCacheOrder.front());
                resultCacheOrder.pop_front();
            }
        }
        if (!PublishEvent("population_result", requestId, payload))
            LOG_WARN("playerbots.agent", "Failed to publish population result event for request {}",
                     requestId.c_str());
    }

    // Picks a bot account with free character slots, creating a new
    // prefix-account when every existing account is full. Returns the account
    // id or 0 on failure.
    uint32 AcquireBotAccount()
    {
        for (uint32 accountId : sPlayerbotAIConfig.randomBotAccounts)
        {
            if (AccountMgr::GetCharactersCount(accountId) < 10)
                return accountId;
        }

        // Find the highest numbered existing prefix account so the next index
        // is unique even after manual account deletions.
        uint32 maxIndex = 0;
        QueryResult accounts = LoginDatabase.Query(
            "SELECT username FROM account WHERE username LIKE '{}%'",
            sPlayerbotAIConfig.randomBotAccountPrefix.c_str());
        if (accounts)
        {
            do
            {
                std::string const username = accounts->Fetch()->Get<std::string>();
                if (username.size() <= sPlayerbotAIConfig.randomBotAccountPrefix.size())
                    continue;
                try
                {
                    uint32 const index = static_cast<uint32>(
                        std::stoul(username.substr(sPlayerbotAIConfig.randomBotAccountPrefix.size())));
                    maxIndex = std::max(maxIndex, index);
                }
                catch (std::exception const&)
                {
                    // Usernames that do not carry a numeric suffix cannot be
                    // ordered; skip them.
                }
            } while (accounts->NextRow());
        }

        std::string const accountName = sPlayerbotAIConfig.randomBotAccountPrefix + std::to_string(maxIndex + 1);
        std::string password;
        if (sPlayerbotAIConfig.randomBotRandomPassword)
        {
            for (int i = 0; i < 10; ++i)
                password += static_cast<char>(urand('!', 'z'));
        }
        else
            password = accountName;

        sAccountMgr->CreateAccount(accountName, password);
        while (LoginDatabase.QueueSize())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        LoginDatabasePreparedStatement* stmt =
            LoginDatabase.GetPreparedStatement(LOGIN_GET_ACCOUNT_ID_BY_USERNAME);
        stmt->SetData(0, accountName);
        PreparedQueryResult result = LoginDatabase.Query(stmt);
        if (!result)
        {
            LOG_ERROR("playerbots.agent", "Created bot account {} could not be read back", accountName.c_str());
            return 0;
        }

        uint32 const accountId = result->Fetch()->Get<uint32>();
        sRandomPlayerbotMgr.RegisterBotAccount(accountId);
        LOG_INFO("playerbots.agent", "Created bot account {} (id {})", accountName.c_str(), accountId);
        return accountId;
    }

    void HandleCreateBot(boost::property_tree::ptree const& arguments, std::string const& requestId)
    {
        std::ostringstream failure;
        auto reject = [&](std::string const& reason)
        {
            failure << "{\"status\":\"rejected\",\"reason\":\"" << agent_bridge::EscapeJson(reason) << "\"}";
            CacheAndPublish(requestId, failure.str());
        };

        uint8 const race = static_cast<uint8>(GetTreeUInt(arguments, "race"));
        uint8 const cls = static_cast<uint8>(GetTreeUInt(arguments, "class"));
        uint8 const gender = static_cast<uint8>(std::min<uint32>(GetTreeUInt(arguments, "gender", 0), 1));
        std::string const name = agent_bridge::TruncateUtf8(GetTreeString(arguments, "name"), 12);

        if (!race || !cls)
        {
            reject("race and class are required");
            return;
        }
        if (cls == CLASS_DEATH_KNIGHT)
        {
            reject("death knights cannot be created at level 1");
            return;
        }
        if (!RandomPlayerbotFactory::IsValidRaceClassCombination(race, cls, sWorld->getIntConfig(CONFIG_EXPANSION)) ||
            ((1 << (race - 1)) & sWorld->getIntConfig(CONFIG_CHARACTER_CREATING_DISABLED_RACEMASK)) ||
            ((1 << (cls - 1)) & sWorld->getIntConfig(CONFIG_CHARACTER_CREATING_DISABLED_CLASSMASK)))
        {
            reject("invalid or disabled race/class combination");
            return;
        }
        if (!name.empty() && sObjectMgr->CheckPlayerName(name) != CHAR_NAME_SUCCESS)
        {
            reject("requested name is invalid or already in use");
            return;
        }

        uint32 const accountId = AcquireBotAccount();
        if (!accountId)
        {
            reject("no bot account could be allocated");
            return;
        }

        RandomPlayerbotFactory factory;
        WorldSession* session = new WorldSession(accountId, "", 0x0, nullptr, SEC_PLAYER,
                                                 EXPANSION_WRATH_OF_THE_LICH_KING, time_t(0), LOCALE_enUS, 0,
                                                 false, false, 0, true);
        Player* bot = factory.CreateBot(session, race, cls, gender, name);
        if (!bot)
        {
            delete session;
            reject("character creation failed (name unavailable or creation rejected)");
            return;
        }

        std::ostringstream payload;
        payload << "{\"status\":\"created\",\"guid\":\"" << bot->GetGUID().GetCounter() << "\",\"name\":\""
                << agent_bridge::EscapeJson(bot->GetName()) << "\",\"race\":" << static_cast<uint32>(bot->getRace())
                << ",\"class\":" << static_cast<uint32>(bot->getClass()) << ",\"gender\":"
                << static_cast<uint32>(bot->getGender()) << ",\"level\":" << static_cast<uint32>(bot->GetLevel())
                << ",\"account_id\":" << accountId << ",\"map_id\":" << bot->GetMapId() << ",\"position\":["
                << bot->GetPositionX() << "," << bot->GetPositionY() << "," << bot->GetPositionZ()
                << "]}";

        bot->CleanupsBeforeDelete();
        delete bot;
        delete session;

        CacheAndPublish(requestId, payload.str());
    }

    std::vector<uint32> LoadBotAccountIds()
    {
        std::vector<uint32> accounts;
        QueryResult result =
            PlayerbotsDatabase.Query("SELECT accountId FROM playerbots_account_type WHERE accountType = 1");
        if (result)
        {
            do
            {
                accounts.push_back(result->Fetch()->Get<uint32>());
            } while (result->NextRow());
        }
        return accounts;
    }

    void HandlePopulationSnapshot(std::string const& requestId)
    {
        uint32 const now = getMSTime();
        if (lastSnapshotMs && getMSTimeDiff(lastSnapshotMs, now) <
                                  sPlayerbotAIConfig.agentBridgePopulationSnapshotInterval)
        {
            CacheAndPublish(requestId, "{\"status\":\"rejected\",\"reason\":\"snapshot throttled\"}");
            return;
        }
        lastSnapshotMs = now;

        std::vector<uint32> const accounts = LoadBotAccountIds();
        std::ostringstream payload;
        payload << "{\"status\":\"completed\",\"total\":0,\"counts\":[],\"zones\":[]}";
        std::string payloadStr = payload.str();

        if (!accounts.empty())
        {
            std::ostringstream accountList;
            for (size_t i = 0; i < accounts.size(); ++i)
            {
                if (i)
                    accountList << ",";
                accountList << accounts[i];
            }

            uint64 total = 0;
            std::ostringstream counts;
            QueryResult characterCounts = CharacterDatabase.Query(
                "SELECT race, class, FLOOR(level / 10) AS level_band, COUNT(*) FROM characters "
                "WHERE account IN ({}) GROUP BY race, class, level_band",
                accountList.str());
            if (characterCounts)
            {
                bool first = true;
                do
                {
                    Field* fields = characterCounts->Fetch();
                    if (!first)
                        counts << ",";
                    first = false;
                    counts << "{\"race\":" << fields[0].Get<uint8>() << ",\"class\":" << fields[1].Get<uint8>()
                           << ",\"level_band\":" << fields[2].Get<uint8>() << ",\"count\":"
                           << fields[3].Get<uint64>() << "}";
                    total += fields[3].Get<uint64>();
                } while (characterCounts->NextRow());
            }

            std::ostringstream zones;
            QueryResult zoneCounts = CharacterDatabase.Query(
                "SELECT hb.zoneId, COUNT(*) FROM character_homebind hb JOIN characters c ON c.guid = hb.guid "
                "WHERE c.account IN ({}) AND c.level <= {} GROUP BY hb.zoneId",
                accountList.str(), sPlayerbotAIConfig.agentBridgePlayerCohortMaxLevel);
            if (zoneCounts)
            {
                bool first = true;
                do
                {
                    Field* fields = zoneCounts->Fetch();
                    if (!first)
                        zones << ",";
                    first = false;
                    zones << "{\"zone_id\":" << fields[0].Get<uint32>() << ",\"low_level\":"
                          << fields[1].Get<uint64>() << "}";
                } while (zoneCounts->NextRow());
            }

            std::ostringstream full;
            full << "{\"status\":\"completed\",\"total\":" << total << ",\"counts\":[" << counts.str()
                 << "],\"zones\":[" << zones.str() << "]}";
            payloadStr = full.str();
        }

        CacheAndPublish(requestId, payloadStr);
    }

    void ExecuteCommand(PopulationCommand const& command)
    {
        if (!command.requestId.empty())
        {
            auto cached = resultCache.find(command.requestId);
            if (cached != resultCache.end())
            {
                PublishEvent("population_result", command.requestId, cached->second);
                return;
            }
        }

        try
        {
            boost::property_tree::ptree arguments;
            std::istringstream input(command.arguments.empty() ? "{}" : command.arguments);
            boost::property_tree::read_json(input, arguments);

            if (command.operation == "create_bot_character")
                HandleCreateBot(arguments, command.requestId);
            else if (command.operation == "population_snapshot")
                HandlePopulationSnapshot(command.requestId);
            else
                CacheAndPublish(command.requestId,
                                "{\"status\":\"rejected\",\"reason\":\"unsupported population operation\"}");
        }
        catch (std::exception const& exception)
        {
            LOG_WARN("playerbots.agent", "Population command failed: {}", exception.what());
            CacheAndPublish(command.requestId,
                            "{\"status\":\"rejected\",\"reason\":\"internal population error\"}");
        }
    }

    static void ReceiveCommand(char const* /*subject*/, char const* payload, int payloadLength)
    {
        if (!payload || payloadLength <= 0 || static_cast<size_t>(payloadLength) > POPULATION_MAX_COMMAND_BYTES)
            return;

        try
        {
            boost::property_tree::ptree root;
            std::istringstream input(std::string(payload, static_cast<size_t>(payloadLength)));
            boost::property_tree::read_json(input, root);

            PopulationCommand command;
            command.requestId = GetTreeString(root, "request_id");
            command.operation = GetTreeString(root, "operation");
            if (command.operation.empty() || command.requestId.empty())
                return;

            if (auto arguments = root.get_child_optional("arguments"))
            {
                std::ostringstream serialized;
                boost::property_tree::write_json(serialized, *arguments, false);
                command.arguments = serialized.str();
            }

            AgentPopulation::instance()->m_impl->EnqueueCommand(command);
        }
        catch (std::exception const& exception)
        {
            LOG_WARN("playerbots.agent", "Rejected invalid population command: {}", exception.what());
        }
    }

    void EnqueueCommand(PopulationCommand const& command)
    {
        std::lock_guard<std::mutex> lock(inbox.mutex);
        if (inbox.commands.size() < POPULATION_MAX_PENDING_COMMANDS)
            inbox.commands.push_back(command);
        else
            LOG_WARN("playerbots.agent", "Population command queue full; dropped {} request", command.operation);
    }
};

AgentPopulation* AgentPopulation::instance()
{
    static AgentPopulation instance;
    if (!instance.m_impl)
        instance.m_impl = new Impl();
    return &instance;
}

bool AgentPopulation::IsEnabled() const
{
    return agent_bridge::SidecarSupported() && sPlayerbotAIConfig.agentBridgeEnabled &&
           sPlayerbotAIConfig.agentBridgePopulationEnabled;
}

void AgentPopulation::Update(uint32 diff)
{
    (void)diff;
    if (!IsEnabled())
        return;

    if (!m_impl->transportReady && (!m_impl->lastSubscribeCheckMs ||
                                    getMSTimeDiff(m_impl->lastSubscribeCheckMs, getMSTime()) >=
                                        POPULATION_SUBSCRIBE_RETRY_MS))
    {
        m_impl->lastSubscribeCheckMs = getMSTime();
        m_impl->transportReady = agent_bridge::Subscribe(
            PopulationCommandSubject(m_impl->subjectPrefix, m_impl->ownerToken), &Impl::ReceiveCommand);
        if (m_impl->transportReady)
            LOG_INFO("playerbots.agent", "Population bridge subscribed on {}",
                     PopulationCommandSubject(m_impl->subjectPrefix, m_impl->ownerToken).c_str());
    }
    if (!m_impl->transportReady)
        return;

    for (uint32 processed = 0; processed < 4; ++processed)
    {
        PopulationCommand command;
        {
            std::lock_guard<std::mutex> lock(m_impl->inbox.mutex);
            if (m_impl->inbox.commands.empty())
                break;
            command = std::move(m_impl->inbox.commands.front());
            m_impl->inbox.commands.pop_front();
        }
        m_impl->ExecuteCommand(command);
    }
}

void AgentPopulation::OnPlayerLogin(Player* player)
{
    if (!player || !IsEnabled())
        return;
    if (player->GetSession()->IsBot())
        return;
    if (player->GetLevel() > sPlayerbotAIConfig.agentBridgePlayerCohortMaxLevel)
        return;

    uint32 const guid = player->GetGUID().GetCounter();
    QueryResult seen = PlayerbotsDatabase.Query("SELECT guid FROM playerbots_agent_players WHERE guid = {}", guid);
    if (seen)
        return;

    PlayerbotsDatabase.Execute("INSERT INTO playerbots_agent_players (guid, seen_at) VALUES ({}, {})", guid,
                               static_cast<uint32>(GameTime::GetGameTime().count()));

    std::ostringstream payload;
    payload << "{\"player_guid\":\"" << guid << "\",\"name\":\"" << agent_bridge::EscapeJson(player->GetName())
            << "\",\"race\":" << static_cast<uint32>(player->getRace()) << ",\"class\":"
            << static_cast<uint32>(player->getClass()) << ",\"level\":" << static_cast<uint32>(player->GetLevel())
            << ",\"map_id\":" << player->GetMapId() << ",\"zone_id\":" << player->GetZoneId() << "}";
    if (!m_impl->PublishEvent("player_first_entry", "", payload.str()))
        LOG_WARN("playerbots.agent", "Failed to publish player_first_entry for {}", player->GetName().c_str());
}
