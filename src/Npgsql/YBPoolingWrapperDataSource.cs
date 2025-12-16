using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using YBNpgsql.Internal;
using YBNpgsql.Util;

namespace YBNpgsql;

/// <summary>
///
/// </summary>
sealed class YBPoolingWrapperDataSource: PoolingDataSource
{
    static readonly object lockObject = new object();
    ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>  connStringToConnectorsMap = null!;
    ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>  connStringToIdleConnectorsMap = null!;

    internal YBPoolingWrapperDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfiguration) :
        base(settings, dataSourceConfiguration)
    {
        connStringToConnectorsMap = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>();
        connStringToIdleConnectorsMap = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>();

    }

    internal override async ValueTask<NpgsqlConnector?> OpenNewConnector(
        NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken, NpgsqlConnectionStringBuilder originalConnString)
    {
        var originalConnStringCopy = originalConnString.Clone();
        /*
         * The connectors are created using the Settings initialized in the pool.
         * When connection strings are different, it will still use the first connection string which was initialized in the pool to create the connection.
         * So check if connection string is same as the original connection string
         * if not replace the host in the original conn string with the chosen host in the pool Settings
         */
        if (!Settings.Equals(originalConnStringCopy))
        {
            originalConnStringCopy.Host = Settings.Host;
            Settings = originalConnStringCopy;
        }
        // 1. Call base logic → this increments _numConnectors and populates Connectors[]
        var connector = await base.OpenNewConnector(conn, timeout, async, cancellationToken).ConfigureAwait(false);

        if (connector is not null)
        {
            lock (lockObject)
            {
                if (connStringToConnectorsMap.TryGetValue(originalConnString, out var list))
                {
                    for (var i = 0; i < MaxConnections; i++)
                        if (Interlocked.CompareExchange(ref list[i], connector, null) == null)
                            break;
                    connStringToConnectorsMap[originalConnString] = list;
                }
                else
                {
                    list = new NpgsqlConnector[MaxConnections];
                    for (var i = 0; i < MaxConnections; i++)
                        if (Interlocked.CompareExchange(ref list[i], connector, null) == null)
                            break;
                    connStringToConnectorsMap[originalConnString] = list;
                }
            }

        }

        return connector;
    }

    internal override bool TryGetIdleConnector(NpgsqlConnectionStringBuilder originalConnString, [NotNullWhen(true)] out NpgsqlConnector? connector)

    {
        connector = null;

        // Try to get the array for this conn string
        if (!connStringToIdleConnectorsMap.TryGetValue(originalConnString, out var connectors))
            return false;

        // Fast scan for the first non-null, *idle* connector
        // (assuming "idle" means connector.State == Idle or similar)
        lock (lockObject)
        {
            for (var i = 0; i < connectors.Length; i++)
            {
                var c = connectors[i];
                if (c is null)
                    continue;  // skip nulls quickly

                if (CheckIdleConnector(c))
                {
                    connector = c;
                    for (var j = 0; j < MaxConnections; j++)
                        if (Interlocked.CompareExchange(ref connectors[j], null, connector) == connector)
                            break;
                    connStringToIdleConnectorsMap[originalConnString] = connectors;
                    if (connStringToConnectorsMap.TryGetValue(originalConnString, out  var list))
                    {
                        for (var j = 0; j < MaxConnections; j++)
                            if (Interlocked.CompareExchange(ref list[j], connector, null) == null)
                                break;
                        connStringToConnectorsMap[originalConnString] = list;
                    }
                    return true;
                }
            }
        }

        return false;
    }

    internal override void Return(NpgsqlConnector connector)
    {
        var flag = 0;
        lock (lockObject)
        {
            foreach (var connStringToConnectors in connStringToConnectorsMap)
            {
                for (var i = 0; i < connStringToConnectors.Value.Length; i++)
                {
                    if (connStringToConnectors.Value[i] == null)
                        continue;
                    if (ReferenceEquals(connStringToConnectors.Value[i], connector))
                    {

                        if (connStringToConnectorsMap.TryGetValue(connStringToConnectors.Key, out var connectorslist))
                        {
                            for (var j = 0; j < MaxConnections; j++)
                                if (Interlocked.CompareExchange(ref connectorslist[j], null, connector) == connector)
                                    break;
                            connStringToConnectorsMap[connStringToConnectors.Key] = connectorslist;

                        }
                        if (connStringToIdleConnectorsMap.TryGetValue(connStringToConnectors.Key, out  var list))
                        {
                            for (var j = 0; j < MaxConnections; j++)
                                if (Interlocked.CompareExchange(ref list[j], connector, null) == null)
                                    break;
                            connStringToIdleConnectorsMap[connStringToConnectors.Key] = list;
                        }
                        else
                        {
                            list = new NpgsqlConnector[MaxConnections];
                            for (var j = 0; j < MaxConnections; j++)
                                if (Interlocked.CompareExchange(ref list[j], connector, null) == null)
                                    break;
                            connStringToIdleConnectorsMap[connStringToConnectors.Key] = list;
                        }

                        flag = 1;
                        break;
                    }
                }
                if (flag == 1)
                    break;
            }
        }

        base.Return(connector);

    }


}
