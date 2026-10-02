/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#include "AgentPopulation.h"

#include <boost/bind/placeholders.hpp>

#include "AgentBridgeShared.h"
#include "AgentRuntime.h"

namespace boost::property_tree::json_parser::detail
{
using boost::placeholders::_1;
}

#include <deque>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
#include "Realm.h"
#include "SharedDefines.h"
#include "Timer.h"
#include "WorldSession.h"
#include "WorldSessionMgr.h"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

namespace
{
constexpr size_t POPULATION_MAX_COMMAND_BYTES = 8192;
constexpr size_t POPULATION_MAX_PENDING_COMMANDS = 64;
constexpr size_t POPULATION_MAX_RESULT_CACHE = 128;
constexpr uint32 POPULATION_SUBSCRIBE_RETRY_MS = 5000;
constexpr uint32 POPULATION_HEARTBEAT_MS = 30000;
constexpr uint32 SOCIAL_REFRESH_MS = 60000;
constexpr uint32 SOCIAL_PRESENCE_TTL_MS = 180000;
constexpr size_t SOCIAL_MAX_BOTS = 256;
constexpr uint32 POPULATION_POLICY_CENSUS_MIN_INTERVAL_MS = 15000;
constexpr size_t POPULATION_POLICY_CENSUS_MAX_BOTS = 2000;
constexpr size_t POPULATION_POLICY_CENSUS_MAX_HUMANS = 500;
constexpr size_t POPULATION_POLICY_MAX_GUIDS = 32;

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

std::string PopulationBotToken() { return "population"; }

std::string PopulationCommandSubject(std::string const& prefix, std::string const& ownerToken)
{
    return prefix + ".population.commands." + ownerToken;
}

std::string PopulationEventSubject(std::string const& prefix, std::string const& ownerToken)
{
    return prefix + ".population.events." + std::to_string(agent_bridge::EventShard(PopulationBotToken())) + "." +
           ownerToken + "." + PopulationBotToken();
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
    int64 const eventTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::string const botToken = PopulationBotToken();
    std::ostringstream event;
    event << "{\"version\":1,\"event_id\":\"" << agent_bridge::EscapeJson(botToken + "-" + std::to_string(sequence))
          << "\",\"timestamp_unix_ms\":" << eventTime << ",\"type\":\"" << agent_bridge::EscapeJson(type)
          << "\",\"bot_guid\":\"" << agent_bridge::EscapeJson(botToken) << "\",\"owner_token\":\""
          << agent_bridge::EscapeJson(ownerToken) << "\",\"owner_epoch\":\"\",\"request_id\":\""
          << agent_bridge::EscapeJson(requestId) << "\",\"payload\":" << (payload.empty() ? "{}" : payload) << "}";
    return event.str();
}
}  // namespace

struct AgentPopulation::Impl
{
    std::string subjectPrefix = sPlayerbotAIConfig.agentBridgeSubjectPrefix;
    std::string ownerToken = agent_bridge::OwnerToken();
    PopulationInbox inbox;
    bool transportReady = false;
    bool botCommandsReady = false;
    uint32 lastSubscribeCheckMs = 0;
    uint64 eventSequence = 1;
    std::map<std::string, std::string> resultCache;
    std::deque<std::string> resultCacheOrder;
    uint32 lastSnapshotMs = 0;
    uint32 lastHeartbeatMs = 0;
    uint32 lastPolicyCensusMs = 0;
    uint32 lastSocialManifestMs = 0;
    uint32 lastSocialPresenceMs = 0;
    std::string socialManifestRequest;
    int64 lastSocialObservation = 0;
    std::unordered_set<uint32> socialManagedBots;
    std::unordered_set<uint32> socialPreferredBots;
    std::unordered_map<uint32, uint32> socialBotAccounts;
    std::unordered_set<uint32> socialPreferredAccounts;

    void HandleSocialManifest(std::string const& requestId)
    {
        uint32 const now = getMSTime();
        if (!sPlayerbotAIConfig.agentBridgeSocialProgressionEnabled ||
            (lastSocialManifestMs && getMSTimeDiff(lastSocialManifestMs, now) < SOCIAL_REFRESH_MS))
            return;
        lastSocialManifestMs = now;
        std::unordered_set<uint32> managed;
        std::unordered_map<uint32, uint32> botAccounts;
        size_t inspectedAccounts = 0;
        std::ostringstream payload;
        payload << "{\"realm_id\":" << realm.Id.Realm << ",\"bot_guids\":[";
        for (uint32 account : LoadBotAccountIds())
        {
            if (inspectedAccounts++ >= SOCIAL_MAX_BOTS)
                break;
            CharacterDatabasePreparedStatement* statement =
                CharacterDatabase.GetPreparedStatement(CHAR_SEL_CHARS_BY_ACCOUNT_ID);
            statement->SetData(0, account);
            PreparedQueryResult result = CharacterDatabase.Query(statement);
            if (!result)
                continue;
            do
            {
                uint32 const guid = result->Fetch()[0].Get<uint32>();
                if (!managed.insert(guid).second)
                    continue;
                botAccounts[guid] = account;
                if (managed.size() > 1)
                    payload << ",";
                payload << guid;
            } while (managed.size() < SOCIAL_MAX_BOTS && result->NextRow());
            if (managed.size() >= SOCIAL_MAX_BOTS)
                break;
        }
        payload << "],\"partial\":"
                << (managed.size() >= SOCIAL_MAX_BOTS || inspectedAccounts > SOCIAL_MAX_BOTS ? "true" : "false") << "}";
        socialManagedBots = std::move(managed);
        socialBotAccounts = std::move(botAccounts);
        socialManifestRequest = requestId;
        PublishEvent("social_population_manifest", requestId, payload.str());
    }

    void HandleSocialPreferences(boost::property_tree::ptree const& arguments)
    {
        if (!sPlayerbotAIConfig.agentBridgeSocialProgressionEnabled)
            return;
        int64 const now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        int64 const observed = arguments.get<int64>("observed_at", 0);
        if (GetTreeString(arguments, "manifest_request") != socialManifestRequest ||
            observed <= lastSocialObservation || observed > now || now - observed > SOCIAL_PRESENCE_TTL_MS)
            return;
        auto guids = arguments.get_child_optional("preferred_guids");
        if (!guids || guids->size() > SOCIAL_MAX_BOTS)
            return;
        std::unordered_set<uint32> preferred;
        std::unordered_set<uint32> preferredAccounts;
        for (auto const& entry : *guids)
        {
            uint32 const guid = entry.second.get_value<uint32>();
            if (!socialManagedBots.contains(guid))
                return;
            preferred.insert(guid);
            preferredAccounts.insert(socialBotAccounts.at(guid));
        }
        socialPreferredBots = std::move(preferred);
        socialPreferredAccounts = std::move(preferredAccounts);
        lastSocialObservation = observed;
        lastSocialPresenceMs = getMSTime() - static_cast<uint32>(now - observed);
        LOG_DEBUG("playerbots.agent", "Social scheduler accepted {} preferred bots from manifest {}",
                  socialPreferredBots.size(), socialManifestRequest);
    }

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
            LOG_WARN("playerbots.agent", "Failed to publish population result event for request {}", requestId.c_str());
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
        QueryResult accounts = LoginDatabase.Query("SELECT username FROM account WHERE username LIKE '{}%'",
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

        LoginDatabasePreparedStatement* stmt = LoginDatabase.GetPreparedStatement(LOGIN_GET_ACCOUNT_ID_BY_USERNAME);
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
        WorldSession* session =
            new WorldSession(accountId, "", 0x0, nullptr, SEC_PLAYER, EXPANSION_WRATH_OF_THE_LICH_KING, time_t(0),
                             LOCALE_enUS, 0, false, false, 0, true);
        Player* bot = factory.CreateBot(session, race, cls, gender, name);
        if (!bot)
        {
            delete session;
            reject("character creation failed (name unavailable or creation rejected)");
            return;
        }

        std::ostringstream payload;
        payload << "{\"status\":\"created\",\"guid\":" << bot->GetGUID().GetCounter() << ",\"name\":\""
                << agent_bridge::EscapeJson(bot->GetName()) << "\",\"race\":" << static_cast<uint32>(bot->getRace())
                << ",\"class\":" << static_cast<uint32>(bot->getClass())
                << ",\"gender\":" << static_cast<uint32>(bot->getGender())
                << ",\"level\":" << static_cast<uint32>(bot->GetLevel()) << ",\"account_id\":" << accountId
                << ",\"map_id\":" << bot->GetMapId() << ",\"position\":[" << bot->GetPositionX() << ","
                << bot->GetPositionY() << "," << bot->GetPositionZ() << "]}";

        bot->CleanupsBeforeDelete();
        delete bot;
        delete session;

        CacheAndPublish(requestId, payload.str());
    }

    std::vector<uint32> LoadBotAccountIds()
    {
        std::vector<uint32> accounts;
        QueryResult result =
            PlayerbotsDatabase.Query("SELECT account_id FROM playerbots_account_type WHERE account_type = 1");
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
        if (lastSnapshotMs &&
            getMSTimeDiff(lastSnapshotMs, now) < sPlayerbotAIConfig.agentBridgePopulationSnapshotInterval)
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
                           << ",\"level_band\":" << fields[2].Get<uint8>() << ",\"count\":" << fields[3].Get<uint64>()
                           << "}";
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
                    zones << "{\"zone_id\":" << fields[0].Get<uint32>() << ",\"low_level\":" << fields[1].Get<uint64>()
                          << "}";
                } while (zoneCounts->NextRow());
            }

            std::ostringstream full;
            full << "{\"status\":\"completed\",\"total\":" << total << ",\"counts\":[" << counts.str()
                 << "],\"zones\":[" << zones.str() << "]}";
            payloadStr = full.str();
        }

        CacheAndPublish(requestId, payloadStr);
    }

    // Publishes the live census the external population planner consumes:
    // managed-pool characters (online with live positions and engagement
    // flags, offline with their saved position) and online real players.
    // Bots never appear in the humans list, so bot presence cannot generate
    // player-vicinity demand. Entries are bounded and partial coverage is
    // disclosed instead of silently truncating.
    void HandlePolicyCensus(std::string const& requestId)
    {
        uint32 const now = getMSTime();
        if (lastPolicyCensusMs && getMSTimeDiff(lastPolicyCensusMs, now) < POPULATION_POLICY_CENSUS_MIN_INTERVAL_MS)
        {
            CacheAndPublish(requestId, "{\"status\":\"rejected\",\"reason\":\"census throttled\"}");
            return;
        }
        lastPolicyCensusMs = now;

        int64 const observedMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();

        std::ostringstream bots;
        size_t botCount = 0;
        std::unordered_set<uint32> onlineGuids;

        for (auto const& [botGuid, player] : sRandomPlayerbotMgr.GetAllBots())
        {
            if (!player || !player->IsInWorld() || botCount >= POPULATION_POLICY_CENSUS_MAX_BOTS)
                continue;
            uint32 const guid = botGuid.GetCounter();
            onlineGuids.insert(guid);
            if (botCount++)
                bots << ",";
            bots << "{\"guid\":" << guid << ",\"name\":\"" << agent_bridge::EscapeJson(player->GetName())
                 << "\",\"race\":" << static_cast<uint32>(player->getRace())
                 << ",\"class\":" << static_cast<uint32>(player->getClass())
                 << ",\"level\":" << static_cast<uint32>(player->GetLevel())
                 << ",\"faction\":" << (IsAlliance(player->getRace()) ? 1 : 2) << ",\"online\":true,\"busy\":"
                 << ((player->IsInCombat() || player->GetTradeData() || player->GetGroup() ||
                      player->HasUnitState(UNIT_STATE_IN_FLIGHT))
                         ? "true"
                         : "false")
                 << ",\"map_id\":" << player->GetMapId() << ",\"instance_id\":" << player->GetInstanceId()
                 << ",\"zone_id\":" << player->GetZoneId() << ",\"x\":" << player->GetPositionX()
                 << ",\"y\":" << player->GetPositionY() << ",\"z\":" << player->GetPositionZ()
                 << ",\"observed_ms\":" << observedMs << "}";
        }

        // Offline pool characters keep their saved logout position so the
        // planner can reuse bots already located at a demand region.
        bool partialBots = false;
        std::vector<uint32> const accounts = LoadBotAccountIds();
        if (!accounts.empty())
        {
            std::ostringstream accountList;
            for (size_t i = 0; i < accounts.size(); ++i)
            {
                if (i)
                    accountList << ",";
                accountList << accounts[i];
            }
            QueryResult offline = CharacterDatabase.Query(
                "SELECT guid, name, race, class, level, map, position_x, position_y, position_z FROM characters "
                "WHERE account IN ({}) AND online = 0",
                accountList.str());
            if (offline)
            {
                do
                {
                    if (botCount >= POPULATION_POLICY_CENSUS_MAX_BOTS)
                    {
                        partialBots = true;
                        break;
                    }
                    Field* fields = offline->Fetch();
                    uint32 const guid = fields[0].Get<uint32>();
                    if (onlineGuids.contains(guid))
                        continue;
                    uint32 const race = fields[2].Get<uint8>();
                    if (botCount++)
                        bots << ",";
                    bots << "{\"guid\":" << guid << ",\"name\":\""
                         << agent_bridge::EscapeJson(fields[1].Get<std::string>())
                         << "\",\"race\":" << static_cast<uint32>(race)
                         << ",\"class\":" << static_cast<uint32>(fields[3].Get<uint8>())
                         << ",\"level\":" << static_cast<uint32>(fields[4].Get<uint8>())
                         << ",\"faction\":" << (IsAlliance(static_cast<uint8>(race)) ? 1 : 2)
                         << ",\"online\":false,\"busy\":false"
                         << ",\"map_id\":" << fields[5].Get<uint32>() << ",\"instance_id\":0"
                         << ",\"zone_id\":0"
                         << ",\"x\":" << fields[6].Get<float>() << ",\"y\":" << fields[7].Get<float>()
                         << ",\"z\":" << fields[8].Get<float>() << ",\"observed_ms\":" << observedMs << "}";
                } while (offline->NextRow());
            }
        }

        std::ostringstream humans;
        size_t humanCount = 0;
        bool partialHumans = false;
        for (auto const& [accountId, session] : sWorldSessionMgr->GetAllSessions())
        {
            Player* player = session ? session->GetPlayer() : nullptr;
            if (!player || !player->IsInWorld() || player->IsGameMaster() || GET_PLAYERBOT_AI(player) != nullptr ||
                sRandomPlayerbotMgr.IsRandomBot(player))
                continue;
            if (humanCount >= POPULATION_POLICY_CENSUS_MAX_HUMANS)
            {
                partialHumans = true;
                break;
            }
            if (humanCount++)
                humans << ",";
            humans << "{\"guid\":" << player->GetGUID().GetRawValue() << ",\"name\":\""
                   << agent_bridge::EscapeJson(player->GetName())
                   << "\",\"faction\":" << (IsAlliance(player->getRace()) ? 1 : 2)
                   << ",\"map_id\":" << player->GetMapId() << ",\"instance_id\":" << player->GetInstanceId()
                   << ",\"x\":" << player->GetPositionX() << ",\"y\":" << player->GetPositionY()
                   << ",\"zone_id\":" << player->GetZoneId() << ",\"observed_ms\":" << observedMs << "}";
        }

        std::ostringstream payload;
        payload << "{\"status\":\"completed\",\"observed_ms\":" << observedMs << ",\"realm_id\":" << realm.Id.Realm
                << ",\"partial\":" << ((partialBots || partialHumans) ? "true" : "false") << ",\"bots\":[" << bots.str()
                << "],\"humans\":[" << humans.str() << "]}";
        CacheAndPublish(requestId, payload.str());
    }

    // Deliberate per-bot admission requested by the population planner.
    // Bounded per command; native checks refuse duplicates and unmanaged
    // characters, and results are authoritative, not assumed.
    void HandlePolicyAdmitBots(boost::property_tree::ptree const& arguments, std::string const& requestId)
    {
        auto guids = arguments.get_child_optional("guids");
        if (!guids || guids->empty() || guids->size() > POPULATION_POLICY_MAX_GUIDS)
        {
            CacheAndPublish(requestId, "{\"status\":\"rejected\",\"reason\":\"guids out of range\"}");
            return;
        }
        uint32 admitted = 0;
        uint32 rejected = 0;
        for (auto const& entry : *guids)
        {
            if (sRandomPlayerbotMgr.AdmitManagedBot(entry.second.get_value<uint32>()))
                ++admitted;
            else
                ++rejected;
        }
        std::ostringstream payload;
        payload << "{\"status\":\"completed\",\"admitted\":" << admitted << ",\"rejected\":" << rejected << "}";
        CacheAndPublish(requestId, payload.str());
    }

    // Deliberate per-bot pause requested by the population planner. Engaged
    // bots are refused natively; the controller applies its own logout
    // grace before dispatching.
    void HandlePolicyLogoutBots(boost::property_tree::ptree const& arguments, std::string const& requestId)
    {
        auto guids = arguments.get_child_optional("guids");
        if (!guids || guids->empty() || guids->size() > POPULATION_POLICY_MAX_GUIDS)
        {
            CacheAndPublish(requestId, "{\"status\":\"rejected\",\"reason\":\"guids out of range\"}");
            return;
        }
        uint32 loggedOut = 0;
        uint32 rejected = 0;
        for (auto const& entry : *guids)
        {
            if (sRandomPlayerbotMgr.LogoutManagedBot(entry.second.get_value<uint32>()))
                ++loggedOut;
            else
                ++rejected;
        }
        std::ostringstream payload;
        payload << "{\"status\":\"completed\",\"logged_out\":" << loggedOut << ",\"rejected\":" << rejected << "}";
        CacheAndPublish(requestId, payload.str());
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
            else if (command.operation == "population_policy_census")
                HandlePolicyCensus(command.requestId);
            else if (command.operation == "population_policy_admit_bots")
                HandlePolicyAdmitBots(arguments, command.requestId);
            else if (command.operation == "population_policy_logout_bots")
                HandlePolicyLogoutBots(arguments, command.requestId);
            else if (command.operation == "social_population_manifest")
                HandleSocialManifest(command.requestId);
            else if (command.operation == "social_schedule_preferences")
                HandleSocialPreferences(arguments);
            else
                CacheAndPublish(command.requestId,
                                "{\"status\":\"rejected\",\"reason\":\"unsupported population operation\"}");
        }
        catch (std::exception const& exception)
        {
            LOG_WARN("playerbots.agent", "Population command failed: {}", exception.what());
            CacheAndPublish(command.requestId, "{\"status\":\"rejected\",\"reason\":\"internal population error\"}");
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

bool AgentPopulation::PrefersOnline(uint32 botGuid) const
{
    return IsEnabled() && sPlayerbotAIConfig.agentBridgeSocialProgressionEnabled && m_impl &&
           m_impl->lastSocialPresenceMs &&
           getMSTimeDiff(m_impl->lastSocialPresenceMs, getMSTime()) <= SOCIAL_PRESENCE_TTL_MS &&
           m_impl->socialPreferredBots.contains(botGuid);
}

bool AgentPopulation::PrefersOnlineAccount(uint32 accountId) const
{
    return IsEnabled() && sPlayerbotAIConfig.agentBridgeSocialProgressionEnabled && m_impl &&
           m_impl->lastSocialPresenceMs &&
           getMSTimeDiff(m_impl->lastSocialPresenceMs, getMSTime()) <= SOCIAL_PRESENCE_TTL_MS &&
           m_impl->socialPreferredAccounts.contains(accountId);
}

void AgentPopulation::Update(uint32 diff)
{
    (void)diff;
    if (!IsEnabled())
        return;

    if (!m_impl->transportReady || !m_impl->botCommandsReady ||
        (!m_impl->lastSubscribeCheckMs ||
         getMSTimeDiff(m_impl->lastSubscribeCheckMs, getMSTime()) >= POPULATION_SUBSCRIBE_RETRY_MS))
    {
        m_impl->lastSubscribeCheckMs = getMSTime();
        if (!m_impl->transportReady)
        {
            m_impl->transportReady = agent_bridge::Subscribe(
                PopulationCommandSubject(m_impl->subjectPrefix, m_impl->ownerToken), &Impl::ReceiveCommand);
            if (m_impl->transportReady)
                LOG_INFO("playerbots.agent", "Population bridge subscribed on {}",
                         PopulationCommandSubject(m_impl->subjectPrefix, m_impl->ownerToken).c_str());
        }
        // Subscribe the bot command subject on the world thread so controller
        // heartbeats open the required-controller login gate before any bot is
        // online. Idempotent and shared with the per-bot runtime refresh.
        if (sPlayerbotAIConfig.agentBridgeEnabled)
            m_impl->botCommandsReady =
                AgentRuntime::EnsureBotCommandSubscription(m_impl->subjectPrefix, m_impl->ownerToken);
    }
    if (!m_impl->transportReady)
        return;

    uint32 const now = getMSTime();
    if (!m_impl->lastHeartbeatMs || getMSTimeDiff(m_impl->lastHeartbeatMs, now) >= POPULATION_HEARTBEAT_MS)
    {
        m_impl->lastHeartbeatMs = now;
        m_impl->PublishEvent("population_owner_online", "", "{}");
    }

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
            << "\",\"race\":" << static_cast<uint32>(player->getRace())
            << ",\"class\":" << static_cast<uint32>(player->getClass())
            << ",\"level\":" << static_cast<uint32>(player->GetLevel()) << ",\"map_id\":" << player->GetMapId()
            << ",\"zone_id\":" << player->GetZoneId() << "}";
    if (!m_impl->PublishEvent("player_first_entry", "", payload.str()))
        LOG_WARN("playerbots.agent", "Failed to publish player_first_entry for {}", player->GetName().c_str());
}
