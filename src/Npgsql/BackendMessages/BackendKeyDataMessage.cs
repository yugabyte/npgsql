﻿using YBNpgsql.Internal;

namespace YBNpgsql.BackendMessages;

sealed class BackendKeyDataMessage : IBackendMessage
{
    public BackendMessageCode Code => BackendMessageCode.BackendKeyData;

    internal int BackendProcessId { get; private set; }
    internal int BackendSecretKey { get; private set; }

    internal BackendKeyDataMessage(NpgsqlReadBuffer buf)
    {
        BackendProcessId = buf.ReadInt32();
        BackendSecretKey = buf.ReadInt32();
    }
}