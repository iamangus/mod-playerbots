/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#include "AgentBridgeShared.h"
#include "Log.h"
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
#include "libsidecar.h"
#endif
#include <cctype>
#include <cstdlib>
#include <sstream>

namespace agent_bridge
{
bool SidecarSupported()
{
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    return true;
#else
    return false;
#endif
}

std::string OwnerToken()
{
    char const* preferredHost = std::getenv("TC9_PREFERRED_HOSTNAME");
    if (!preferredHost || !*preferredHost)
        preferredHost = std::getenv("HOSTNAME");

    std::string token = preferredHost ? preferredHost : "local";
    for (char& character : token)
    {
        unsigned char const value = static_cast<unsigned char>(character);
        if (!std::isalnum(value) && character != '_' && character != '-')
            character = '_';
    }
    return token;
}

uint32 EventShard(std::string const& botToken)
{
    constexpr uint32 SHARD_COUNT = 256;
    uint32 hash = 2166136261U;
    for (unsigned char character : botToken)
    {
        hash ^= character;
        hash *= 16777619U;
    }
    return hash % SHARD_COUNT;
}

bool Subscribe(std::string const& subject, MessageHandler handler)
{
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    if (!handler || subject.empty())
        return false;
    if (TC9CheckAbiCompatible(TC9_VERSION_MAJOR, TC9_VERSION_MINOR) != 0)
    {
        LOG_ERROR("playerbots.agent", "ToCloud9 sidecar ABI is incompatible with the playerbot bridge");
        return false;
    }
    return TC9NatsSubscribe(subject.c_str(), handler) == 0;
#else
    (void)subject;
    (void)handler;
    return false;
#endif
}

bool Publish(std::string const& subject, std::string const& payload)
{
#if defined(PLAYERBOTS_WITH_TOCLOUD9_SIDECAR)
    if (subject.empty())
        return false;
    return TC9NatsPublish(subject.c_str(), payload.data(), static_cast<int>(payload.size())) == 0;
#else
    (void)subject;
    (void)payload;
    return false;
#endif
}

std::string EscapeJson(std::string const& value)
{
    std::ostringstream out;
    for (unsigned char character : value)
    {
        switch (character)
        {
            case '"': out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\b': out << "\\b"; break;
            case '\f': out << "\\f"; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default:
                if (character < 0x20)
                    out << "\\u00" << std::hex << static_cast<uint32>(character) << std::dec;
                else
                    out << static_cast<char>(character);
        }
    }
    return out.str();
}

std::string TruncateUtf8(std::string value, size_t maxBytes)
{
    if (value.size() <= maxBytes)
        return value;

    size_t end = maxBytes;
    while (end > 0 && end < value.size() &&
           (static_cast<unsigned char>(value[end]) & 0xC0) == 0x80)
        --end;
    value.resize(end);
    return value;
}
}