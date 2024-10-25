using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using YBNpgsql.Internal;
using YBNpgsql.Util;

namespace YBNpgsql;

/// <summary>
/// For Uniform Load Balancing
/// </summary>
public class ClusterAwareDataSource: NpgsqlDataSource
{
    private static ClusterAwareDataSource? instance;
    /// <summary>
    /// Contains the connection pool
    /// </summary>
    protected static List<NpgsqlDataSource> _pools = new List<NpgsqlDataSource>();
    internal List<NpgsqlDataSource> Pools => _pools;
    /// <summary>
    /// Map of yb_server hosts
    /// </summary>
    protected Dictionary<string, string>? _hostsNodeTypeMap = null;
    // volatile int _roundRobinIndex = -1;
    /// <summary>
    /// Stores the last time yb_servers() was called
    /// </summary>
    protected DateTime _lastServerFetchTime = new DateTime(0);
    readonly double REFRESH_LIST_SECONDS;
    readonly int MAX_REFRESH_INTERVAL = 600;
    /// <summary>
    /// List of unreachable hosts
    /// </summary>
    protected static List<int> unreachableHostsIndices = new List<int>();

    /// <summary>
    /// List of unreachable hosts
    /// </summary>
    protected static List<string> unreachableHosts = new List<string>();
    /// <summary>
    /// Stores a boolean value for which IP Address is to be used - Public or Private
    /// True = Private IPs
    /// False = Public IPs
    /// </summary>
    protected bool? UseHostColumn = null;

    /// <summary>
    /// Stores a map of pool to number of connections made to the pool for the Primary nodes
    /// Key = Pool
    /// Value = Number of Connections to that pool
    /// </summary>
    protected static Dictionary<NpgsqlDataSource, int> poolToNumConnMapPrimary = new Dictionary<NpgsqlDataSource, int>();

    /// <summary>
    /// Stores a map of pool to number of connections for the Read Replica nodes
    /// Key = Pool
    /// Value = Number of Connections to that pool
    /// </summary>
    protected static Dictionary<NpgsqlDataSource, int> poolToNumConnMapRR = new Dictionary<NpgsqlDataSource, int>();

    /// <summary>
    /// Stores a map of host to their priority
    /// </summary>
    protected static Dictionary<string, int> hostToPriorityMap = new Dictionary<string, int>();

    /// <summary>
    /// Connection settings
    /// </summary>
    protected NpgsqlConnectionStringBuilder settings;
    internal NpgsqlDataSourceConfiguration dataSourceConfig;

    /// <summary>
    /// List of initial hosts for control connections
    /// </summary>
    protected List<string>? initialHosts;

    /// <summary>
    /// Lock object
    /// </summary>
    protected static readonly object lockObject = new object();

    /// <summary>
    /// Key of primary placement in the allowedPlacementMap
    /// </summary>
    protected readonly int PRIMARY_PLACEMENTS = 1;
    /// <summary>
    /// Key of first fallback placement in the allowedPlacementMap
    /// </summary>
    protected readonly int FIRST_FALLBACK = 2;
    /// <summary>
    /// Key for placements not mentioned in the Toplology Keys in the allowedPlacementMap
    /// </summary>
    protected readonly int REST_OF_CLUSTER = 11;
    /// <summary>
    /// Maximum value of priority of fallback placements possible
    /// </summary>
    protected readonly int MAX_PREFERENCE_VALUE = 10;
    /// <summary>
    /// list of nodes' public IPs
    /// </summary>
    protected Dictionary<string, string> currentPublicIps = new Dictionary<string, string>();

    /// <summary>
    /// Contains a dictionary of private IPs for fallback
    /// Key = Fallback priority level
    /// Value = Dictionary (IP , nodeType)
    /// </summary>
    protected ConcurrentDictionary<int, Dictionary<string, string>> fallbackPrivateIPs;
    /// <summary>
    /// Contains a dictionary of public IPs for fallback
    /// Key = Fallback priority leve
    /// Value = Dictionary (IP , nodeType)
    /// </summary>
    protected ConcurrentDictionary<int, Dictionary<string, string>> fallbackPublicIPs;

    /// <summary>
    /// To set refresh value explicitly
    /// </summary>
    protected internal static bool forceRefresh = false;

    /// <summary>
    /// Logger instance
    /// </summary>
    protected readonly ILogger _connectionLogger;
    internal ClusterAwareDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfig, bool useClusterAwareDataSource)
        : base(settings, dataSourceConfig)
    {
        fallbackPrivateIPs = new ConcurrentDictionary<int, Dictionary<string, string>>();
        fallbackPublicIPs = new ConcurrentDictionary<int, Dictionary<string, string>>();
        this.settings = settings;
        this.dataSourceConfig = dataSourceConfig;
        REFRESH_LIST_SECONDS = settings.YBServersRefreshInterval > 0 && settings.YBServersRefreshInterval < MAX_REFRESH_INTERVAL
            ? this.settings.YBServersRefreshInterval
            : 300;
        initialHosts = Settings.Host?.Split(',').ToList();
        _connectionLogger = LoggingConfiguration.ConnectionLogger;
        Debug.Assert(initialHosts != null, nameof(initialHosts) + " != null");
        if(useClusterAwareDataSource)
        {
            instance = this;
            foreach (var host in initialHosts.ToList())
            {
                try
                {
                    var controlSettings = settings;
                    controlSettings.Host = host.ToString();
                    NpgsqlDataSource control = new UnpooledDataSource(controlSettings, dataSourceConfig);
                    NpgsqlConnection controlConnection = NpgsqlConnection.FromDataSource(control);
                    controlConnection.Open();
                    lock (lockObject)
                    {
                        _hostsNodeTypeMap = GetCurrentServers(controlConnection);
                    }
                    CreatePool(_hostsNodeTypeMap);
                    controlConnection.Close();
                    break;
                }
                catch (Exception)
                {
                    _connectionLogger.LogDebug("Could not connect to host: {host}", host);
                    initialHosts.Remove(host);
                    if (initialHosts.Count == 0)
                    {
                        _connectionLogger.LogError("Failed to make Control Connection. No suitable host found");
                        throw;
                    }
                }

            }
        }

    }


    /// <summary>
    /// Returns the current instance
    /// </summary>
    /// <returns></returns>
    internal static ClusterAwareDataSource? GetInstance()
    {
        return instance;
    }

    /// <summary>
    /// Create a new pool
    /// </summary>
    internal void CreatePool(Dictionary<string, string> hostsmap)
    {
        lock (lockObject)
        {
            _hostsNodeTypeMap = hostsmap;
            foreach(var host in _hostsNodeTypeMap)
            {
                var flag = 0;
                foreach (var pool in _pools)
                {
                    if (host.Key.Equals(pool.Settings.Host, StringComparison.OrdinalIgnoreCase))
                    {
                        flag = 1;
                        break;
                    }
                }

                if (flag == 1)
                    continue;
                var poolSettings = settings.Clone();
                poolSettings.Host = host.Key;
                _connectionLogger.LogDebug("Adding {host} to connection pool", poolSettings.Host);
                NpgsqlDataSource poolnew = settings.Pooling? new PoolingDataSource(poolSettings, dataSourceConfig): new UnpooledDataSource(poolSettings, dataSourceConfig);
                _pools.Add(poolnew);
                if (host.Value.Equals("primary", StringComparison.OrdinalIgnoreCase))
                {
                    poolToNumConnMapPrimary[poolnew] = 0;
                }
                else if (host.Value.Equals("read_replica", StringComparison.OrdinalIgnoreCase))
                {
                    poolToNumConnMapRR[poolnew] = 0;
                }
            }
        }
    }

    /// <summary>
    /// Checks if a higher priority node is available
    /// </summary>
    /// <param name="poolindex"></param>
    /// <returns></returns>
    protected bool HasBetterNodeAvailable(int poolindex)
    {
        return false;
    }

    /// <summary>
    /// Returns the connection count of a server
    /// </summary>
    /// <param name="server"></param>
    public static int GetLoad(string server)
    {
        foreach (var pool in poolToNumConnMapPrimary)
        {
            Debug.Assert(pool.Key.Settings.Host != null, "pool.Key.Settings.Host != null");
            if (pool.Key.Settings.Host.Equals(server, StringComparison.OrdinalIgnoreCase))
            {
                return pool.Value;
            }
        }
        foreach (var pool in poolToNumConnMapRR)
        {
            Debug.Assert(pool.Key.Settings.Host != null, "pool.Key.Settings.Host != null");
            if (pool.Key.Settings.Host.Equals(server, StringComparison.OrdinalIgnoreCase))
            {
                return pool.Value;
            }
        }

        return -1;
    }

    /// <summary>
    /// gets the list of hosts
    /// </summary>
    /// <param name="conn"></param>
    protected Dictionary<string, string> GetCurrentServers(NpgsqlConnection conn)
    {
        NpgsqlCommand QUERY_SERVER = new NpgsqlCommand("Select * from yb_servers()",conn);
        NpgsqlDataReader reader = QUERY_SERVER.ExecuteReader();
        _lastServerFetchTime = DateTime.Now;
        Dictionary<string, string> currentPrivateIps = new Dictionary<string, string>();
        Dictionary<string, string> currentPublicIps = new Dictionary<string, string>();
        var hostConnectedTo = conn.Host;

        Debug.Assert(hostConnectedTo != null, nameof(hostConnectedTo) + " != null");
        var isIpv6Addresses = hostConnectedTo.Contains(":");
        if (isIpv6Addresses) {
            hostConnectedTo = hostConnectedTo.Replace("[", "").Replace("]", "");
        }
        while (reader.Read())
        {
            var host = reader.GetString(reader.GetOrdinal("host"));
            var publicHost = reader.GetString(reader.GetOrdinal("public_ip"));
            var nodeType = reader.GetString(reader.GetOrdinal("node_type"));
            currentPrivateIps[host] = nodeType;
            currentPublicIps[publicHost] = nodeType;
            if (hostConnectedTo.Equals(host))
            {
                UseHostColumn = true;
            } else if (hostConnectedTo.Equals(publicHost)) {
                UseHostColumn = false;
            }
        }
        return GetPrivateOrPublicServers(currentPrivateIps, currentPublicIps);
    }

    /// <summary>
    /// Returns a list of private or public IPs based on the value of useHostColumn variable
    /// </summary>
    /// <param name="privateHosts"></param>
    /// <param name="publicHosts"></param>
    /// <returns></returns>
    protected Dictionary<string, string> GetPrivateOrPublicServers(Dictionary<string, string> privateHosts, Dictionary<string, string> publicHosts) {
        if (UseHostColumn == null) {
            if (!publicHosts.Any())
            {
                UseHostColumn = true;
            }

            return privateHosts;
        }
        var currentHosts = (bool)UseHostColumn ? privateHosts : publicHosts;
        return currentHosts;
    }
    void UpdateConnectionMap(int poolIndex, int incDec)
    {
        NpgsqlDataSource currPool = null!;
        if (poolIndex >= 0 && poolIndex <= _pools.Count)
        {
            currPool = _pools[poolIndex];
        }
        else
        {
            _connectionLogger.LogWarning("Poolindex not found in pools map");
            return;
        }
        lock (lockObject)
        {
            int currentCount;

            if(poolToNumConnMapPrimary.ContainsKey(currPool))
            {
                currentCount = poolToNumConnMapPrimary[currPool];
                poolToNumConnMapPrimary[currPool] += incDec;
                _connectionLogger.LogTrace("Updated the current count for {host} from {currentCount} to {newCount}", _pools[poolIndex].Settings.Host, currentCount, poolToNumConnMapPrimary[currPool]);
            }
            else if (poolToNumConnMapRR.ContainsKey(currPool))
            {
                currentCount = poolToNumConnMapRR[currPool];
                poolToNumConnMapRR[currPool] += incDec;
                _connectionLogger.LogTrace("Updated the current count for {host} from {currentCount} to {newCount}", _pools[poolIndex].Settings.Host, currentCount, poolToNumConnMapRR[currPool]);
            }
        }
    }

    internal override (int Total, int Idle, int Busy) Statistics { get; }

    internal override bool Refresh()
    {
        _connectionLogger.LogDebug("Refreshing connection");
        Debug.Assert(initialHosts != null, nameof(initialHosts) + " != null");
        if (_hostsNodeTypeMap != null && _hostsNodeTypeMap.Count != 0)
        {
            initialHosts.AddRange(_hostsNodeTypeMap.Keys);
        }

        initialHosts = initialHosts.Distinct().ToList();
        foreach (var host in initialHosts.ToList())
        {
            try
            {
                var controlSettings = settings;
                controlSettings.Host = host.ToString();
                NpgsqlDataSource control = new UnpooledDataSource(controlSettings, dataSourceConfig);
                NpgsqlConnection controlConnection = NpgsqlConnection.FromDataSource(control);
                controlConnection.Open();
                _hostsNodeTypeMap = GetCurrentServers(controlConnection);
                CreatePool(_hostsNodeTypeMap);
                controlConnection.Close();
                break;
            }
            catch (Exception)
            {
                _connectionLogger.LogDebug("Failed to connect to {host}", host);
                initialHosts.Remove(host);
                if (initialHosts.Count == 0)
                {
                    _connectionLogger.LogError("Failed to create control connection. No suitable host found");
                    throw;
                }

            }

        }
        unreachableHostsIndices.Clear();
        unreachableHosts.Clear();
        return true;
    }

    internal override async ValueTask<NpgsqlConnector> Get(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
        CancellationToken cancellationToken)
    {
        NpgsqlConnector? connector = null;
        var exceptions = new List<Exception>();
        connector = await getConnector(conn, timeout,async, cancellationToken, exceptions).ConfigureAwait(false);

        if (this is TopologyAwareDataSource)
        {
            exceptions.Clear();
            Dictionary<string, string>? hosts;
            Dictionary<string, string>? public_ip;
            if (connector == null)
            {
                for (var i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++)
                {
                    _connectionLogger.LogDebug("Attempting Fallback: {fallback}", i);
                    fallbackPrivateIPs.TryGetValue(i, out hosts);
                    fallbackPublicIPs.TryGetValue(i, out public_ip);
                    if (hosts != null)
                    {
                        exceptions.Clear();
                        CreatePool(fallbackPrivateIPs[i]);
                        connector = await getConnector(conn, timeout,async, cancellationToken, exceptions).ConfigureAwait(false);
                        if (connector != null)
                            break;
                    }
                    else if (public_ip != null)
                    {
                        exceptions.Clear();
                        CreatePool(fallbackPublicIPs[i]);
                        connector = await getConnector(conn, timeout,async, cancellationToken, exceptions).ConfigureAwait(false);
                        if (connector != null)
                            break;
                    }
                }
            }

            if (connector == null)
            {
                if (settings.FallBackToTopologyKeysOnly)
                    throw NoSuitableHostsException(exceptions);
                fallbackPrivateIPs.TryGetValue(REST_OF_CLUSTER, out hosts);
                fallbackPublicIPs.TryGetValue(REST_OF_CLUSTER, out public_ip);
                if (hosts != null)
                {
                    exceptions.Clear();
                    CreatePool(fallbackPrivateIPs[REST_OF_CLUSTER]);
                    connector = await getConnector(conn, timeout,async, cancellationToken, exceptions).ConfigureAwait(false);
                }
                else if (public_ip != null)
                {
                    exceptions.Clear();
                    CreatePool(fallbackPublicIPs[REST_OF_CLUSTER]);
                    connector = await getConnector(conn, timeout,async, cancellationToken, exceptions).ConfigureAwait(false);
                }
            }
        }

        return connector ?? throw NoSuitableHostsException(exceptions);
    }

    async Task<NpgsqlConnector?> getConnector(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
        CancellationToken cancellationToken, List<Exception> exceptions)
    {
        NpgsqlConnector? connector = null;
        for (var i = 0; i < _pools.Count; i++)
        {
            CheckDisposed();

            var poolIndex = conn.Settings.LoadBalanceHosts != LoadBalanceHosts.False ? GetRoundRobinIndex() : 0;
            if (poolIndex == -1)
                return null;
            var chosenHost = _pools[poolIndex].Settings.Host;
            _connectionLogger.LogDebug("Chosen Host: {host}", chosenHost);
            var HasBetterNode = HasBetterNodeAvailable(poolIndex);
            if (HasBetterNode)
            {
                _connectionLogger.LogDebug("A better node is available");
                UpdateConnectionMap(poolIndex, -1);
                HasBetterNode = false;
                await getConnector(conn, timeout, async, cancellationToken, exceptions).ConfigureAwait(false);
            }

            if (poolIndex == -1)
                break;
            var timeoutPerHost = timeout.IsSet ? timeout.CheckAndGetTimeLeft() : TimeSpan.Zero;
            var preferredType = GetTargetSessionAttributes(conn);
            var checkUnpreferred = preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby;

            connector = await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken).ConfigureAwait(false) ??
                        (checkUnpreferred ?
                            await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken).ConfigureAwait(false)
                            : null) ??
                        await TryGet(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken).ConfigureAwait(false) ??
                        (checkUnpreferred ?
                            await TryGet(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken).ConfigureAwait(false)
                            : null);
            if (connector != null)
            {
                break;
            }
            unreachableHostsIndices.Add(poolIndex);
            var settingsHost = _pools[poolIndex].Settings.Host;
            if (settingsHost != null) unreachableHosts.Add(settingsHost);
            // poolToNumConnMap.Remove(poolIndex);
            var pool = _pools[poolIndex];
            if (poolToNumConnMapPrimary.ContainsKey(pool))
                poolToNumConnMapPrimary.Remove(pool);
            else if (poolToNumConnMapRR.ContainsKey(pool))
                poolToNumConnMapRR.Remove(pool);
            UpdateConnectionMap(poolIndex, -1);
        }

        return connector;

    }



    internal override bool TryGetIdleConnector([NotNullWhen(true)] out NpgsqlConnector? connector)
        => throw new NpgsqlException("Npgsql bug: trying to get an idle connector from " + nameof(ClusterAwareDataSource));

    internal override ValueTask<NpgsqlConnector?> OpenNewConnector(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken) => throw new NotImplementedException();

    internal override void Return(NpgsqlConnector connector)
    {
        var host = connector.Host;
        var poolIndex = -1;
        for (var i = 0; i < _pools.Count; i++)
        {
            if (host.Equals(_pools[i].Settings.Host, StringComparison.OrdinalIgnoreCase))
            {
                poolIndex = i;
                break;
            }
        }
        UpdateConnectionMap(poolIndex, -1);
    }

    internal override void Clear()
    {
        foreach (var pool in _pools)
            pool.Clear();
    }

    internal override bool OwnsConnectors { get; }

    static bool IsPreferred(DatabaseState state, TargetSessionAttributes preferredType)
        => state switch
        {
            DatabaseState.Offline => false,
            DatabaseState.Unknown => true, // We will check compatibility again after refreshing the database state

            DatabaseState.PrimaryReadWrite when preferredType is
                    TargetSessionAttributes.Primary or
                    TargetSessionAttributes.PreferPrimary or
                    TargetSessionAttributes.ReadWrite
                => true,

            DatabaseState.PrimaryReadOnly when preferredType is
                    TargetSessionAttributes.Primary or
                    TargetSessionAttributes.PreferPrimary or
                    TargetSessionAttributes.ReadOnly
                => true,

            DatabaseState.Standby when preferredType is
                    TargetSessionAttributes.Standby or
                    TargetSessionAttributes.PreferStandby or
                    TargetSessionAttributes.ReadOnly
                => true,

            _ => preferredType == TargetSessionAttributes.Any
        };

    static bool IsOnline(DatabaseState state, TargetSessionAttributes preferredType)
    {
        Debug.Assert(preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby);
        return state != DatabaseState.Offline;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="exceptions"></param>
    /// <returns></returns>
    protected static NpgsqlException NoSuitableHostsException(IList<Exception> exceptions)
        => exceptions.Count == 0
            ? new NpgsqlException("No suitable host was found.")
            : exceptions[0] is PostgresException firstException &&
              exceptions.All(x => x is PostgresException ex && ex.SqlState == firstException.SqlState)
                ? firstException
                : new NpgsqlException("Unable to connect to a suitable host. Check inner exception for more details.",
                    new AggregateException(exceptions));

    internal override bool NeedsRefresh()
    {
        if (forceRefresh)
            return true;
        var currentTime = DateTime.Now;
        var diff = (currentTime - _lastServerFetchTime).TotalSeconds;
        return (diff > REFRESH_LIST_SECONDS);
    }

    int GetRoundRobinIndex()
    {
        Dictionary<NpgsqlDataSource, int>? poolToNumConnMap = null;
        if (settings.LoadBalanceHosts == LoadBalanceHosts.OnlyPrimary || settings.LoadBalanceHosts == LoadBalanceHosts.PreferPrimary)
        {
            poolToNumConnMap = poolToNumConnMapPrimary;
        }
        else if (settings.LoadBalanceHosts == LoadBalanceHosts.OnlyRR || settings.LoadBalanceHosts == LoadBalanceHosts.PreferRR)
        {
            poolToNumConnMap = poolToNumConnMapRR;
        }
        else if (settings.LoadBalanceHosts == LoadBalanceHosts.Any || settings.LoadBalanceHosts == LoadBalanceHosts.True)
        {
            poolToNumConnMap = poolToNumConnMapPrimary.Concat(poolToNumConnMapRR).ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        var PoolIndex = -1;
        lock(lockObject)
        {
            Debug.Assert(poolToNumConnMap != null, nameof(poolToNumConnMap) + " != null");
            PoolIndex = getHosts(poolToNumConnMap);
            if (PoolIndex != -1)
            {
                return PoolIndex;
            }

            if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferPrimary)
            {
                PoolIndex = getHosts(poolToNumConnMapRR);
                return PoolIndex;
            }

            if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferRR)
            {
                PoolIndex = getHosts(poolToNumConnMapPrimary);
                return PoolIndex;
            }

            return -1;
        }
    }

    int getHosts(Dictionary<NpgsqlDataSource, int> poolToNumConnMap)
    {
        var PoolIndex = -1;
        Debug.Assert(poolToNumConnMap != null, nameof(poolToNumConnMap) + " != null");
        for (var i = 0; i < poolToNumConnMap.Count; i++)
        {
            var Pool = poolToNumConnMap.Aggregate((l, r) => l.Value < r.Value ? l : r).Key;
            PoolIndex = _pools.IndexOf(Pool);
            if (!unreachableHostsIndices.Contains(PoolIndex))
            {
                UpdateConnectionMap(PoolIndex, 1);
                return PoolIndex;
            }

        }

        return PoolIndex;
    }

    static TargetSessionAttributes GetTargetSessionAttributes(NpgsqlConnection connection)
        => connection.Settings.TargetSessionAttributesParsed ??
           (PostgresEnvironment.TargetSessionAttributes is { } s
               ? NpgsqlConnectionStringBuilder.ParseTargetSessionAttributes(s)
               : TargetSessionAttributes.Any);

    async ValueTask<NpgsqlConnector?> TryGetIdleOrNew(
        NpgsqlConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType, Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        int poolIndex,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        var pools = _pools;
        var pool = pools[poolIndex];
        var databaseState = pool.GetDatabaseState();
        NpgsqlConnector? connector = null;
        try
        {
            if (pool.TryGetIdleConnector(out connector))
            {
                if (databaseState == DatabaseState.Unknown)
                {
                    databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                    Debug.Assert(databaseState != DatabaseState.Unknown);
                    if (!stateValidator(databaseState, preferredType))
                    {
                        pool.Return(connector);
                        return null;
                    }
                }
                return connector;
            }
            else
            {
                connector = await pool.OpenNewConnector(conn, new NpgsqlTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                if (connector is not null)
                {
                    if (databaseState == DatabaseState.Unknown)
                    {
                        // While opening a new connector we might have refreshed the database state, check again
                        databaseState = pool.GetDatabaseState();
                        if (databaseState == DatabaseState.Unknown)
                            databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
                        Debug.Assert(databaseState != DatabaseState.Unknown);
                        if (!stateValidator(databaseState, preferredType))
                        {
                            pool.Return(connector);
                            return null;
                        }
                    }
                    return connector;
                }
            }
        }
        catch (Exception ex)
        {
            exceptions.Add(ex);
            if (connector is not null)
                pool.Return(connector);
        }

        return null;
    }

    async ValueTask<NpgsqlConnector?> TryGet(
        NpgsqlConnection conn,
        TimeSpan timeoutPerHost,
        bool async,
        TargetSessionAttributes preferredType,
        Func<DatabaseState, TargetSessionAttributes, bool> stateValidator,
        int poolIndex,
        IList<Exception> exceptions,
        CancellationToken cancellationToken)
    {
        var pools = _pools;
        var pool = pools[poolIndex];
        var databaseState = pool.GetDatabaseState();
        NpgsqlConnector? connector = null;

        try
        {
            connector = await pool.Get(conn, new NpgsqlTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);
            if (databaseState == DatabaseState.Unknown)
            {
                // Get might have opened a new physical connection and refreshed the database state, check again
                databaseState = pool.GetDatabaseState();
                if (databaseState == DatabaseState.Unknown)
                    databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken).ConfigureAwait(false);

                Debug.Assert(databaseState != DatabaseState.Unknown);
                if (!stateValidator(databaseState, preferredType))
                {
                    pool.Return(connector);
                    return null;
                }
            }

            return connector;
        }
        catch (Exception ex)
        {
            exceptions.Add(ex);
            if (connector is not null)
                pool.Return(connector);
        }

        return null;
    }

}
