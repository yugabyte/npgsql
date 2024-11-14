using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace YBNpgsql;

/// <summary>
///
/// </summary>
public sealed class TopologyAwareDataSource: ClusterAwareDataSource
{
    ConcurrentDictionary<int, HashSet<CloudPlacement>?> allowedPlacements;
    Dictionary<string, string> AllRRIps = new Dictionary<string, string>();
    Dictionary<string, string> AllPrimaryIps = new Dictionary<string, string>();

    internal TopologyAwareDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfig) : base(settings,dataSourceConfig,false)
    {
        allowedPlacements = new ConcurrentDictionary<int, HashSet<CloudPlacement>?>();
        ParseGeoLocations();
        _connectionLogger.LogDebug("Allowed Placements: {allowedPlacements}", allowedPlacements);
        Debug.Assert(initialHosts != null, nameof(initialHosts) + " != null");
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
                    _hostsToNodeTypeMap = GetCurrentServers(controlConnection);
                }
                CreatePool(_hostsToNodeTypeMap);
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

    void ParseGeoLocations()
    {
        var values = settings.TopologyKeys?.Split(',');
        Debug.Assert(values != null, nameof(values) + " != null");
        foreach (var value in values)
        {
            var v = value.Split(':');
            if (v.Length > 2 || value.EndsWith(":", StringComparison.Ordinal))
            {
                throw new InvalidExpressionException("Invalid value part for property" + settings.TopologyKeys + ":" + value);
            }

            if (v.Length == 1 )
            {
                HashSet<CloudPlacement>? primary = allowedPlacements.GetOrAdd(PRIMARY_PLACEMENTS, k => new HashSet<CloudPlacement>());
                PopulatePlacementSet(v[0], primary);
            }
            else
            {
                var pref = Convert.ToInt32(v[1]);
                if (pref == 1)
                {
                    HashSet<CloudPlacement>? primary;
                    if (!allowedPlacements.TryGetValue(PRIMARY_PLACEMENTS, out primary))
                    {
                        primary = new HashSet<CloudPlacement>();
                        allowedPlacements[PRIMARY_PLACEMENTS] = primary;
                    }
                    PopulatePlacementSet(v[0], primary);
                }
                else if (pref > 1 && pref <= MAX_PREFERENCE_VALUE)
                {
                    HashSet<CloudPlacement>? fallbackPlacements;
                    if (!allowedPlacements.TryGetValue(pref, out fallbackPlacements))
                    {
                        fallbackPlacements = new HashSet<CloudPlacement>();
                        allowedPlacements[pref] = fallbackPlacements;
                    }
                    PopulatePlacementSet(v[0], fallbackPlacements);
                }
                else
                {
                    throw new InvalidExpressionException("Invalid value part for property" + settings.TopologyKeys + ":" + value);
                }
            }
        }
    }

    void PopulatePlacementSet(string placements, HashSet<CloudPlacement>? allowedPlacements)
    {
        var pStrings = placements.Split(',');
        foreach (var pl in pStrings)
        {
            var placementParts = pl.Split('.');
            if (placementParts.Length != 3 || placementParts[0].Equals("*") || placementParts[1].Equals("*"))
            {
                throw new InvalidExpressionException("Malformed " + settings.TopologyKeys + " property value:" + pl);
            }

            CloudPlacement cp = new CloudPlacement(placementParts[0], placementParts[1], placementParts[2]);

            allowedPlacements?.Add(cp);

        }
    }


    /// <summary>
    /// Create a new pool
    /// </summary>
    internal new  void CreatePool(Dictionary<string,string> hostsmap)
    {
        lock (lockObject)
        {
            _hostsToNodeTypeMap = hostsmap;
            foreach(var host in _hostsToNodeTypeMap)
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
            unreachableHostsIndices.Clear();
        }
    }

    new bool HasBetterNodeAvailable(int poolindex)
    {
        var chosenHost = _pools[poolindex].Settings.Host;
        if (chosenHost != null && hostToPriorityMap.ContainsKey(chosenHost)) {
            var chosenHostPriority = hostToPriorityMap[chosenHost];
            for (var i = 1; i < chosenHostPriority; i++) {
                if (hostToPriorityMap.Values.Contains(i)) {
                    return true;
                }
            }
        }
        return false;
    }
    void UpdatePriorityMap(string host, string cloud, string region, string zone)
    {
        if (!unreachableHosts.Contains(host))
        {
            var priority = getPriority(cloud, region, zone);
            hostToPriorityMap[host] = priority;
        }
    }

    private int getPriority(string cloud, string region, string zone) {
        CloudPlacement cp = new CloudPlacement(cloud, region, zone);
        return getKeysByValue(cp);
    }

    private int getKeysByValue(CloudPlacement cp) {
        int i;
        HashSet<CloudPlacement>? placement;
        for (i = 1; i <= MAX_PREFERENCE_VALUE; i++)
        {
            allowedPlacements.TryGetValue(i, out placement);
            if (placement != null && allowedPlacements.Any()) {
                if (cp.IsContainedIn(placement)){
                    return i;
                }
            }
        }
        return MAX_PREFERENCE_VALUE + 1;
    }

    new Dictionary<string, string> GetCurrentServers(NpgsqlConnection conn)
    {
        NpgsqlCommand QUERY_SERVER = new NpgsqlCommand("Select * from yb_servers()",conn);
        NpgsqlDataReader reader = QUERY_SERVER.ExecuteReader();
        _lastServerFetchTime = DateTime.Now;
        Dictionary<string, string> currentPrivateIps = new Dictionary<string, string>();

        var hostConnectedTo = conn.Host;
        hostToPriorityMap.Clear();

        Debug.Assert(hostConnectedTo != null, nameof(hostConnectedTo) + " != null");
        while (reader.Read())
        {
            var host = reader.GetString(reader.GetOrdinal("host"));
            var publicHost = reader.GetString(reader.GetOrdinal("public_ip"));
            var cloud = reader.GetString(reader.GetOrdinal("cloud"));
            var region = reader.GetString(reader.GetOrdinal("region"));
            var zone = reader.GetString(reader.GetOrdinal("zone"));
            var nodeType = reader.GetString(reader.GetOrdinal("node_type"));
            if (nodeType.Equals("read_replica", StringComparison.OrdinalIgnoreCase))
            {
                AllRRIps[host] = nodeType;
            }
            if (nodeType.Equals("primary", StringComparison.OrdinalIgnoreCase))
            {
                AllPrimaryIps[host] = nodeType;
            }

            UpdatePriorityMap(host, cloud, region, zone);

            UpdateCurrentHostList(currentPrivateIps, host, publicHost, cloud, region, zone, nodeType);

            if (hostConnectedTo.Equals(host))
            {
                UseHostColumn = true;
            } else if (hostConnectedTo.Equals(publicHost)) {
                UseHostColumn = false;
            }

        }
        return GetPrivateOrPublicServers(currentPrivateIps, currentPublicIps);
    }

    Dictionary<string, string> GetRelevantServerToNodeTypeMap(Dictionary<string, string> serverToNodeTypeMap)
    {
        Dictionary<string,string> serversNodeTypeMapCopy = serverToNodeTypeMap;
        foreach (var serverNodeType in serverToNodeTypeMap)
        {
            if (!unreachableHosts.Contains(serverNodeType.Key))
            {
                if (settings.LoadBalanceHosts == LoadBalanceHosts.OnlyPrimary || settings.LoadBalanceHosts == LoadBalanceHosts.PreferPrimary)
                {
                    if (serverNodeType.Value.Equals("read_replica", StringComparison.OrdinalIgnoreCase))
                    {
                        serversNodeTypeMapCopy.Remove(serverNodeType.Key);
                    }
                }
                if (settings.LoadBalanceHosts == LoadBalanceHosts.OnlyRR || settings.LoadBalanceHosts == LoadBalanceHosts.PreferRR)
                {
                    if (serverNodeType.Value.Equals("primary", StringComparison.OrdinalIgnoreCase))
                    {
                        serversNodeTypeMapCopy.Remove(serverNodeType.Key);
                    }
                }
            }
        }

        return serversNodeTypeMapCopy;
    }
    new Dictionary<string, string> GetPrivateOrPublicServers(Dictionary<string, string> privateHosts, Dictionary<string,string> publicHosts)
    {
        var exceptions = new List<Exception>();
        Dictionary<string,string> serverToNodeTypeMap = base.GetPrivateOrPublicServers(privateHosts, publicHosts);

        if (serverToNodeTypeMap != null && serverToNodeTypeMap.Any())
        {
            serverToNodeTypeMap = GetRelevantServerToNodeTypeMap(serverToNodeTypeMap);

            if (serverToNodeTypeMap.Any())
                return serverToNodeTypeMap;
        }

        for (var i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++)
        {
            fallbackPrivateIPs.TryGetValue(i, out var privateIp);
            fallbackPublicIPs.TryGetValue(i, out var  publicIp);
            if (privateIp == null)
                privateIp = new Dictionary<string, string>();
            if (publicIp == null)
                publicIp = new Dictionary<string, string>();
            if (privateIp.Any() ||  publicIp.Any())
            {
                serverToNodeTypeMap = base.GetPrivateOrPublicServers(privateIp, publicIp);
                serverToNodeTypeMap = GetRelevantServerToNodeTypeMap(serverToNodeTypeMap);

                if (serverToNodeTypeMap.Any())
                    return serverToNodeTypeMap;
            }
        }

        if (settings.FallBackToTopologyKeysOnly)
        {
            if (settings.LoadBalanceHosts != LoadBalanceHosts.PreferPrimary || settings.LoadBalanceHosts != LoadBalanceHosts.PreferRR)
            {
                throw NoSuitableHostsException(exceptions);
            }
            if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferPrimary)
            {
                return AllRRIps;
            }
            else if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferRR)
            {
                return AllPrimaryIps;
            }
        }

        fallbackPrivateIPs.TryGetValue(REST_OF_CLUSTER, out var privateIPRest);
        fallbackPublicIPs.TryGetValue(REST_OF_CLUSTER, out var publicIPRest);

        if (privateIPRest == null)
            privateIPRest = new Dictionary<string, string>();
        if (publicIPRest == null)
            publicIPRest = new Dictionary<string, string>();

        serverToNodeTypeMap = base.GetPrivateOrPublicServers(privateIPRest, publicIPRest);
        serverToNodeTypeMap = GetRelevantServerToNodeTypeMap(serverToNodeTypeMap);

        if (serverToNodeTypeMap.Any())
            return serverToNodeTypeMap;

        if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferPrimary)
        {
            return AllRRIps;
        }
        else if (settings.LoadBalanceHosts == LoadBalanceHosts.PreferRR)
        {
            return AllPrimaryIps;
        }

        throw NoSuitableHostsException(exceptions);
    }

    void UpdateCurrentHostList(Dictionary<string, string> currentPrivateIPs, string host, string publicIP, string cloud, string region, string zone, string nodetype)
    {
        CloudPlacement cp = new CloudPlacement(cloud, region, zone);
        Console.WriteLine("Adding host:" + host);
        if (cp.IsContainedIn(allowedPlacements[PRIMARY_PLACEMENTS]))
        {
            currentPrivateIPs[host] = nodetype;
            if (!string.IsNullOrEmpty(publicIP.Trim()))
            {
                currentPublicIps[publicIP] = nodetype;
            }
        }
        else
        {
            foreach (var allowedCPs in allowedPlacements)
            {
                if (cp.IsContainedIn(allowedCPs.Value))
                {
                    Dictionary<string,string> hostsmap = fallbackPrivateIPs.GetOrAdd(allowedCPs.Key, k => new Dictionary<string, string>());
                    hostsmap.Add(host, nodetype);

                    if (!string.IsNullOrEmpty(publicIP.Trim()))
                    {
                        Dictionary<string,string> publicIPsMap = fallbackPublicIPs.GetOrAdd(allowedCPs.Key, k => new Dictionary<string, string>());
                        publicIPsMap.Add(publicIP, nodetype);
                    }

                    return;
                }
            }

            Dictionary<string,string> remainingHosts = fallbackPrivateIPs.GetOrAdd(REST_OF_CLUSTER, k => new Dictionary<string, string>());
            remainingHosts.Add(host, nodetype);
            if (!string.IsNullOrEmpty(publicIP.Trim()))
            {
                Dictionary<string, string> remainingPublicIPs =
                    fallbackPublicIPs.GetOrAdd(REST_OF_CLUSTER, k => new Dictionary<string, string>());
                remainingPublicIPs.Add(publicIP, nodetype);
            }
        }
    }

    class CloudPlacement
    {
        private string cloud;
        private string region;
        private string zone;

        internal CloudPlacement(string cloud, string region, string zone) {
            this.cloud = cloud;
            this.region = region;
            this.zone = zone;
        }

        public bool EqualPlacements(CloudPlacement obj)
        {
            var equal = false;
            equal = cloud.Equals(obj.cloud, StringComparison.OrdinalIgnoreCase) &&
                    region.Equals(obj.region, StringComparison.OrdinalIgnoreCase) &&
                    zone.Equals(obj.zone, StringComparison.OrdinalIgnoreCase);
            return equal;
        }

        public bool IsContainedIn(HashSet<CloudPlacement>? set)
        {
            Debug.Assert(set != null, nameof(set) + " != null");
            foreach (var cp in set)
            {
               if (cp.zone.Equals("*"))
               {
                   if (cp.cloud.Equals(cloud, StringComparison.OrdinalIgnoreCase) &&
                       cp.region.Equals(region, StringComparison.OrdinalIgnoreCase))
                   {
                       return true;
                   }
               }
               else
               {
                   if (cp.cloud.Equals(cloud, StringComparison.OrdinalIgnoreCase) &&
                       cp.region.Equals(region, StringComparison.OrdinalIgnoreCase) &&
                       cp.zone.Equals(zone, StringComparison.OrdinalIgnoreCase))
                   {
                       return true;
                   }
               }
            }

            return false;
        }
    }
}
