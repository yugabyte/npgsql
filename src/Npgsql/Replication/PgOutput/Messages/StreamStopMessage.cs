using YBNpgsqlTypes;
using System;

namespace YBNpgsql.Replication.PgOutput.Messages;

/// <summary>
/// Logical Replication Protocol stream stop message
/// </summary>
public sealed class StreamStopMessage : PgOutputReplicationMessage
{
    internal StreamStopMessage() {}

    internal new StreamStopMessage Populate(NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock)
    {
        base.Populate(walStart, walEnd, serverClock);
        return this;
    }
}