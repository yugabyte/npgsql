using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
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
    static ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>  connStringToConnectorsMap = null!;
    internal YBPoolingWrapperDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfiguration) :
        base(settings, dataSourceConfiguration)
    {
        connStringToConnectorsMap = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, NpgsqlConnector?[]>();
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
            lock (connStringToConnectorsMap)
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

    internal new bool TryGetIdleConnector([NotNullWhen(true)] out NpgsqlConnector? connector)

    {
        return base.TryGetIdleConnector(out connector);
    }

    internal new void Return(NpgsqlConnector connector)
    {
       base.Return(connector);
    }


}
