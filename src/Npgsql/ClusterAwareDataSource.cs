using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;
using Npgsql.Util;

namespace Npgsql;

/// <summary>
/// For Uniform Load Balancing
/// </summary>
public class ClusterAwareDataSource: NpgsqlDataSource
{
    private static ClusterAwareDataSource? instance;
    /// <summary>
    /// Contains the connection pool
    /// </summary>
    protected List<NpgsqlDataSource> _pools = new List<NpgsqlDataSource>();
    internal List<NpgsqlDataSource> Pools => _pools;
    /// <summary>
    /// list of yb_server hosts
    /// </summary>
    protected List<string>? _hosts = null;
    // volatile int _roundRobinIndex = -1;
    DateTime _lastServerFetchTime = new DateTime(0);
    readonly double REFRESH_LIST_SECONDS;
    readonly int MAX_REFRESH_INTERVAL = 600;
    List<int> unreachableHostsIndices = new List<int>();
    /// <summary>
    /// Stores a boolean value for which IP Address is to be used - Public or Private
    /// True = Private IPs
    /// False = Public IPs
    /// </summary>
    protected bool? UseHostColumn = null;
    static int index = 0;

    /// <summary>
    /// Stores a map of pool index to number of connections made to the pool
    /// Key = Pool Index
    /// Value = Number of Connections to that pool
    /// </summary>
    protected Dictionary<int, int> poolToNumConnMap = new Dictionary<int, int>();
    /// <summary>
    /// Connection settings
    /// </summary>
    protected NpgsqlConnectionStringBuilder settings;
    internal NpgsqlDataSourceConfiguration dataSourceConfig;

    /// <summary>
    /// List of initial hosts for control connections
    /// </summary>
    protected List<string>? initialHosts;

    internal ClusterAwareDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfig, bool useClusterAwareDataSource)
        : base(settings, dataSourceConfig)
    {
        this.settings = settings;
        this.dataSourceConfig = dataSourceConfig;
        REFRESH_LIST_SECONDS = settings.YBServersRefreshInterval > 0 && settings.YBServersRefreshInterval < MAX_REFRESH_INTERVAL
            ? this.settings.YBServersRefreshInterval
            : 300;
        initialHosts = Settings.Host?.Split(',').ToList();
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
                    CreatePool(controlConnection);
                    controlConnection.Close();
                }
                catch (Exception)
                {
                    if (initialHosts.Count == 0)
                        throw;
                    initialHosts.Remove(host);
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
    internal void CreatePool(NpgsqlConnection conn)
    {
        _hosts = GetCurrentServers(conn);
        foreach(var host in _hosts)
        {
            var flag = 0;
            foreach (var pool in _pools)
            {
                if (host.Equals(pool.Settings.Host, StringComparison.OrdinalIgnoreCase))
                {
                    flag = 1;
                    break;
                }
            }

            if (flag == 1)
                continue;
            var poolSettings = settings.Clone();
            poolSettings.Host = host.ToString();

            _pools.Add(settings.Pooling
                ? new PoolingDataSource(poolSettings, dataSourceConfig)
                : new UnpooledDataSource(poolSettings, dataSourceConfig));

            poolToNumConnMap[index] = 0;
            index++;

        }
    }

    /// <summary>
    /// gets the list of hosts
    /// </summary>
    /// <param name="conn"></param>
    protected List<string> GetCurrentServers(NpgsqlConnection conn)
    {
        NpgsqlCommand QUERY_SERVER = new NpgsqlCommand("Select * from yb_servers()",conn);
        NpgsqlDataReader reader = QUERY_SERVER.ExecuteReader();
        List<string> currentPrivateIps = new List<string>();
        List<string> currentPublicIps = new List<string>();
        var hostConnectedTo = conn.Host;
        
        Debug.Assert(hostConnectedTo != null, nameof(hostConnectedTo) + " != null");
        var isIpv6Addresses = hostConnectedTo.Contains(":");
        if (isIpv6Addresses) {
            hostConnectedTo = hostConnectedTo.Replace("[", "").Replace("]", "");
        }
        while (reader.Read())
        {
            var host = reader.GetString(0);
            var publicHost = reader.GetString(7);
            currentPrivateIps.Add(host);
            currentPublicIps.Add(publicHost);
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
    protected List<string> GetPrivateOrPublicServers(List<string> privateHosts, List<string> publicHosts) {
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

    internal override bool Refresh(NpgsqlConnection conn)
    {
        if (!NeedsRefresh())
            return true;
        DateTime currTime = DateTime.Now;
        CreatePool(conn);
        if (_hosts == null) return false;
        _lastServerFetchTime = currTime;
        unreachableHostsIndices.Clear();
        return true;
    }

    void UpdateConnectionMap(int poolIndex, int incDec)
    {
        int currentCount;
        if (!poolToNumConnMap.TryGetValue(poolIndex, out currentCount))
        {
            if (incDec < 0)
                return;
            else
            {
                poolToNumConnMap.Add(poolIndex, incDec);
            }
        }

        poolToNumConnMap[poolIndex] =  currentCount + incDec;
    }

    internal override (int Total, int Idle, int Busy) Statistics { get; }

    internal override async ValueTask<NpgsqlConnector> Get(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
        CancellationToken cancellationToken)
    {
        NpgsqlConnector? connector = null;
        var exceptions = new List<Exception>();
        for (var i = 0; i < _pools.Count; i++)
        {
            CheckDisposed();

            var poolIndex = conn.Settings.LoadBalanceHosts ? GetRoundRobinIndex() : 0;
            if (poolIndex == -1)
                break;
            var timeoutPerHost = timeout.IsSet ? timeout.CheckAndGetTimeLeft() : TimeSpan.Zero;
            var preferredType = GetTargetSessionAttributes(conn);
            var checkUnpreferred = preferredType is TargetSessionAttributes.PreferPrimary or TargetSessionAttributes.PreferStandby;

            connector = await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken) ??
                            (checkUnpreferred ?
                                await TryGetIdleOrNew(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken)
                                : null) ??
                            await TryGet(conn, timeoutPerHost, async, preferredType, IsPreferred, poolIndex, exceptions, cancellationToken) ??
                            (checkUnpreferred ?
                                await TryGet(conn, timeoutPerHost, async, preferredType, IsOnline, poolIndex, exceptions, cancellationToken)
                                : null);
            if (connector != null)
            {
                break;
            }
            unreachableHostsIndices.Add(poolIndex);
            poolToNumConnMap.Remove(poolIndex);
        }
        
        return connector ?? throw NoSuitableHostsException(exceptions);
    }
    
    

    internal override bool TryGetIdleConnector([NotNullWhen(true)] out NpgsqlConnector? connector)
        => throw new NpgsqlException("Npgsql bug: trying to get an idle connector from " + nameof(ClusterAwareDataSource));

    internal override ValueTask<NpgsqlConnector?> OpenNewConnector(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken) => throw new NotImplementedException();

    internal override void Return(NpgsqlConnector connector) => throw new NotImplementedException();

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
    
    static NpgsqlException NoSuitableHostsException(IList<Exception> exceptions)
        => exceptions.Count == 0
            ? new NpgsqlException("No suitable host was found.")
            : exceptions[0] is PostgresException firstException &&
              exceptions.All(x => x is PostgresException ex && ex.SqlState == firstException.SqlState)
                ? firstException
                : new NpgsqlException("Unable to connect to a suitable host. Check inner exception for more details.",
                    new AggregateException(exceptions));

    internal override bool NeedsRefresh()
    {
        var currentTime = DateTime.Now;
        var diff = (currentTime - _lastServerFetchTime).TotalMilliseconds;
        return (diff > REFRESH_LIST_SECONDS);
    }
    
    int GetRoundRobinIndex()
    {
        // Randomize when two indexes have the same number of connections
        for (var i = 0; i < poolToNumConnMap.Count; i++)
        {
            var PoolIndex = poolToNumConnMap.Aggregate((l, r) => l.Value < r.Value ? l : r).Key;
            if (!unreachableHostsIndices.Contains(PoolIndex))
                return PoolIndex;

        }

        return -1;
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
                    databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken);
                    Debug.Assert(databaseState != DatabaseState.Unknown);
                    if (!stateValidator(databaseState, preferredType))
                    {
                        pool.Return(connector);
                        return null;
                    }
                }
                UpdateConnectionMap(poolIndex, 1);
                return connector;
            }
            else
            {
                connector = await pool.OpenNewConnector(conn, new NpgsqlTimeout(timeoutPerHost), async, cancellationToken);
                if (connector is not null)
                {
                    if (databaseState == DatabaseState.Unknown)
                    {
                        // While opening a new connector we might have refreshed the database state, check again
                        databaseState = pool.GetDatabaseState();
                        if (databaseState == DatabaseState.Unknown)
                            databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken);
                        Debug.Assert(databaseState != DatabaseState.Unknown);
                        if (!stateValidator(databaseState, preferredType))
                        {
                            pool.Return(connector);
                            return null;
                        }
                    }
                    UpdateConnectionMap(poolIndex, 1);
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
            connector = await pool.Get(conn, new NpgsqlTimeout(timeoutPerHost), async, cancellationToken);
            if (databaseState == DatabaseState.Unknown)
            {
                // Get might have opened a new physical connection and refreshed the database state, check again
                databaseState = pool.GetDatabaseState();
                if (databaseState == DatabaseState.Unknown)
                    databaseState = await connector.QueryDatabaseState(new NpgsqlTimeout(timeoutPerHost), async, cancellationToken);
    
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
