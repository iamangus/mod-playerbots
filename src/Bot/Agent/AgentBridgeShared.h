/*
 * This file is part of the mod-playerbots module for AzerothCore. See AUTHORS file for Copyright
 * information; released under GNU GPL v2 license, redistribute/modify under version 2 of the License,
 * or (at your option) any later version.
 */

#ifndef PLAYERBOTS_AGENTBRIDGESHARED_H
#define PLAYERBOTS_AGENTBRIDGESHARED_H

#include "Common.h"
#include <string>

namespace agent_bridge
{
using MessageHandler = void (*)(char const* subject, char const* payload, int payloadLength);

// True when the module was built with ToCloud9's libsidecar integration.
bool SidecarSupported();

// Stable token for this worldserver instance (TC9_PREFERRED_HOSTNAME or HOSTNAME).
std::string OwnerToken();

// FNV-1a shard of a bot token; matches the external controller's shard math.
uint32 EventShard(std::string const& botToken);

// Subscribe a NATS subject via the sidecar; returns false when unavailable.
bool Subscribe(std::string const& subject, MessageHandler handler);

// Publish a payload to a NATS subject via the sidecar; returns false when unavailable.
bool Publish(std::string const& subject, std::string const& payload);

std::string EscapeJson(std::string const& value);
std::string TruncateUtf8(std::string value, size_t maxBytes);
}

#endif