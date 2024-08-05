// using System;
// using System.Collections.Concurrent;
// using System.Collections.Generic;
// using System.Collections.Specialized;
// using System.Data;
// using System.Diagnostics;
// using System.Diagnostics.CodeAnalysis;
// using System.Linq;
// using Microsoft.Extensions.Logging;
//
// namespace YBNpgsql;
//
// /// <summary>
// ///
// /// </summary>
// public sealed class TopologyAwareDataSource: ClusterAwareDataSource
// {
//     ConcurrentDictionary<int, HashSet<CloudPlacement>?> allowedPlacements;
//
//     internal TopologyAwareDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfig) : base(settings,dataSourceConfig,false)
//     {
//         allowedPlacements = new ConcurrentDictionary<int, HashSet<CloudPlacement>?>();
//         ParseGeoLocations();
//         _connectionLogger.LogDebug("Allowed Placements: {allowedPlacements}", allowedPlacements);
//         Debug.Assert(initialHosts != null, nameof(initialHosts) + " != null");
//         foreach (var host in initialHosts.ToList())
//         {
//             try
//             {
//                 var controlSettings = settings;
//                 controlSettings.Host = host.ToString();
//                 NpgsqlDataSource control = new UnpooledDataSource(controlSettings, dataSourceConfig);
//                 NpgsqlConnection controlConnection = NpgsqlConnection.FromDataSource(control);
//                 controlConnection.Open();
//                 lock (lockObject)
//                 {
//                     _hosts = GetCurrentServers(controlConnection);
//                 }
//                 CreatePool(_hosts);
//                 controlConnection.Close();
//                 break;
//             }
//             catch (Exception)
//             {
//                 _connectionLogger.LogDebug("Could not connect to host: {host}", host);
//                 initialHosts.Remove(host);
//                 if (initialHosts.Count == 0)
//                 {
//                     _connectionLogger.LogError("Failed to make Control Connection. No suitable host found");
//                     throw;
//                 }
//             }
//
//         }
//     }
//
//     void ParseGeoLocations()
//     {
//         var values = settings.TopologyKeys?.Split(',');
//         Debug.Assert(values != null, nameof(values) + " != null");
//         foreach (var value in values)
//         {
//             var v = value.Split(':');
//             if (v.Length > 2 || value.EndsWith(":", StringComparison.Ordinal))
//             {
//                 throw new InvalidExpressionException("Invalid value part for property" + settings.TopologyKeys + ":" + value);
//             }
//
//             if (v.Length == 1 )
//             {
//                 HashSet<CloudPlacement>? primary = allowedPlacements.GetOrAdd(PRIMARY_PLACEMENTS, k => new HashSet<CloudPlacement>());
//                 PopulatePlacementSet(v[0], primary);
//             }
//             else
//             {
//                 var pref = Convert.ToInt32(v[1]);
//                 if (pref == 1)
//                 {
//                     HashSet<CloudPlacement>? primary;
//                     if (!allowedPlacements.TryGetValue(PRIMARY_PLACEMENTS, out primary))
//                     {
//                         primary = new HashSet<CloudPlacement>();
//                         allowedPlacements[PRIMARY_PLACEMENTS] = primary;
//                     }
//                     PopulatePlacementSet(v[0], primary);
//                 }
//                 else if (pref > 1 && pref <= MAX_PREFERENCE_VALUE)
//                 {
//                     HashSet<CloudPlacement>? fallbackPlacements;
//                     if (!allowedPlacements.TryGetValue(pref, out fallbackPlacements))
//                     {
//                         fallbackPlacements = new HashSet<CloudPlacement>();
//                         allowedPlacements[pref] = fallbackPlacements;
//                     }
//                     PopulatePlacementSet(v[0], fallbackPlacements);
//                 }
//                 else
//                 {
//                     throw new InvalidExpressionException("Invalid value part for property" + settings.TopologyKeys + ":" + value);
//                 }
//             }
//         }
//     }
//
//     void PopulatePlacementSet(string placements, HashSet<CloudPlacement>? allowedPlacements)
//     {
//         var pStrings = placements.Split(',');
//         foreach (var pl in pStrings)
//         {
//             var placementParts = pl.Split('.');
//             if (placementParts.Length != 3 || placementParts[0].Equals("*") || placementParts[1].Equals("*"))
//             {
//                 throw new InvalidExpressionException("Malformed " + settings.TopologyKeys + " property value:" + pl);
//             }
//
//             CloudPlacement cp = new CloudPlacement(placementParts[0], placementParts[1], placementParts[2]);
//
//             allowedPlacements?.Add(cp);
//
//         }
//     }
//
//
//     /// <summary>
//     /// Create a new pool
//     /// </summary>
//     internal new  void CreatePool(List<string> hosts)
//     {
//         lock (lockObject)
//         {
//             _hosts = hosts;
//             foreach(var host in _hosts)
//             {
//                 var flag = 0;
//                 foreach (var pool in _pools)
//                 {
//                     if (host.Equals(pool.Settings.Host, StringComparison.OrdinalIgnoreCase))
//                     {
//                         flag = 1;
//                         break;
//                     }
//                 }
//
//                 if (flag == 1)
//                     continue;
//                 var poolSettings = settings.Clone();
//                 poolSettings.Host = host.ToString();
//                 _connectionLogger.LogDebug("Adding {host} to connection pool", poolSettings.Host);
//
//                 _pools.Add(settings.Pooling
//                     ? new PoolingDataSource(poolSettings, dataSourceConfig)
//                     : new UnpooledDataSource(poolSettings, dataSourceConfig));
//
//                 poolToNumConnMap[index] = 0;
//                 index++;
//
//             }
//             unreachableHostsIndices.Clear();
//         }
//     }
//
//     new bool HasBetterNodeAvailable(int poolindex)
//     {
//         var chosenHost = _pools[poolindex].Settings.Host;
//         if (chosenHost != null && hostToPriorityMap.ContainsKey(chosenHost)) {
//             var chosenHostPriority = hostToPriorityMap[chosenHost];
//             for (var i = 1; i < chosenHostPriority; i++) {
//                 if (hostToPriorityMap.Values.Contains(i)) {
//                     return true;
//                 }
//             }
//         }
//         return false;
//     }
//     void UpdatePriorityMap(string host, string cloud, string region, string zone)
//     {
//         if (!unreachableHosts.Contains(host))
//         {
//             var priority = getPriority(cloud, region, zone);
//             hostToPriorityMap[host] = priority;
//         }
//     }
//
//     private int getPriority(string cloud, string region, string zone) {
//         CloudPlacement cp = new CloudPlacement(cloud, region, zone);
//         return getKeysByValue(cp);
//     }
//
//     private int getKeysByValue(CloudPlacement cp) {
//         int i;
//         HashSet<CloudPlacement>? placement;
//         for (i = 1; i <= MAX_PREFERENCE_VALUE; i++)
//         {
//             allowedPlacements.TryGetValue(i, out placement);
//             if (placement != null && allowedPlacements.Any()) {
//                 if (cp.IsContainedIn(placement)){
//                     return i;
//                 }
//             }
//         }
//         return MAX_PREFERENCE_VALUE + 1;
//     }
//
//     new List<string> GetCurrentServers(NpgsqlConnection conn)
//     {
//         NpgsqlCommand QUERY_SERVER = new NpgsqlCommand("Select * from yb_servers()",conn);
//         NpgsqlDataReader reader = QUERY_SERVER.ExecuteReader();
//         _lastServerFetchTime = DateTime.Now;
//         List<string> currentPrivateIps = new List<string>();
//         var hostConnectedTo = conn.Host;
//         hostToPriorityMap.Clear();
//
//         Debug.Assert(hostConnectedTo != null, nameof(hostConnectedTo) + " != null");
//         while (reader.Read())
//         {
//             var host = reader.GetString(reader.GetOrdinal("host"));
//             var publicHost = reader.GetString(reader.GetOrdinal("public_ip"));
//             var cloud = reader.GetString(reader.GetOrdinal("cloud"));
//             var region = reader.GetString(reader.GetOrdinal("region"));
//             var zone = reader.GetString(reader.GetOrdinal("zone"));
//
//             UpdatePriorityMap(host, cloud, region, zone);
//
//             UpdateCurrentHostList(currentPrivateIps, host, publicHost, cloud, region, zone);
//
//             if (hostConnectedTo.Equals(host))
//             {
//                 UseHostColumn = true;
//             } else if (hostConnectedTo.Equals(publicHost)) {
//                 UseHostColumn = false;
//             }
//
//         }
//         return GetPrivateOrPublicServers(currentPrivateIps, currentPublicIps);
//     }
//
//     new List<string> GetPrivateOrPublicServers(List<string> privateHosts, List<string> publicHosts)
//     {
//         List<string> servers = base.GetPrivateOrPublicServers(privateHosts, publicHosts);
//         var flag = 0;
//
//         if (servers != null && servers.Any())
//         {
//             foreach (var server in servers)
//             {
//                 if (!unreachableHosts.Contains(server))
//                 {
//                     flag = 1;
//                     break;
//                 }
//             }
//
//             if (flag == 1)
//                 return servers;
//         }
//
//         for (var i = FIRST_FALLBACK; i <= MAX_PREFERENCE_VALUE; i++)
//         {
//             if (fallbackPrivateIPs[i] != null && !fallbackPrivateIPs[i].Any())
//             {
//                 return base.GetPrivateOrPublicServers(fallbackPrivateIPs[i], fallbackPublicIPs[i]);
//             }
//         }
//
//         return base.GetPrivateOrPublicServers(fallbackPrivateIPs[REST_OF_CLUSTER], fallbackPublicIPs[REST_OF_CLUSTER]);
//     }
//
//     void UpdateCurrentHostList(List<string> currentPrivateIPs, string host, string publicIP, string cloud, string region, string zone)
//     {
//         CloudPlacement cp = new CloudPlacement(cloud, region, zone);
//         if (cp.IsContainedIn(allowedPlacements[PRIMARY_PLACEMENTS]))
//         {
//             currentPrivateIPs.Add(host);
//             if (!string.IsNullOrEmpty(publicIP.Trim()))
//             {
//                 currentPublicIps.Add(publicIP);
//             }
//         }
//         else
//         {
//             foreach (var allowedCPs in allowedPlacements)
//             {
//                 if (cp.IsContainedIn(allowedCPs.Value))
//                 {
//                     List<string> hosts = fallbackPrivateIPs.GetOrAdd(allowedCPs.Key, k => new List<string>());
//                     hosts.Add(host);
//
//                     if (!string.IsNullOrEmpty(publicIP.Trim()))
//                     {
//                         List<string> publicIPs = fallbackPublicIPs.GetOrAdd(allowedCPs.Key, k => new List<string>());
//                         publicIPs.Add(publicIP);
//                     }
//
//                     return;
//                 }
//             }
//
//             List<string> remainingHosts = fallbackPrivateIPs.GetOrAdd(REST_OF_CLUSTER, k => new List<string>());
//             remainingHosts.Add(host);
//             List<string> remainingPublicIPs = fallbackPublicIPs.GetOrAdd(REST_OF_CLUSTER, k => new List<string>());
//             remainingPublicIPs.Add(publicIP);
//         }
//     }
//
//     class CloudPlacement
//     {
//         private string cloud;
//         private string region;
//         private string zone;
//
//         internal CloudPlacement(string cloud, string region, string zone) {
//             this.cloud = cloud;
//             this.region = region;
//             this.zone = zone;
//         }
//
//         public bool EqualPlacements(CloudPlacement obj)
//         {
//             var equal = false;
//             equal = cloud.Equals(obj.cloud, StringComparison.OrdinalIgnoreCase) &&
//                     region.Equals(obj.region, StringComparison.OrdinalIgnoreCase) &&
//                     zone.Equals(obj.zone, StringComparison.OrdinalIgnoreCase);
//             return equal;
//         }
//
//         public bool IsContainedIn(HashSet<CloudPlacement>? set)
//         {
//             Debug.Assert(set != null, nameof(set) + " != null");
//             foreach (var cp in set)
//             {
//                if (cp.zone.Equals("*"))
//                {
//                    if (cp.cloud.Equals(cloud, StringComparison.OrdinalIgnoreCase) &&
//                        cp.region.Equals(region, StringComparison.OrdinalIgnoreCase))
//                    {
//                        return true;
//                    }
//                }
//                else
//                {
//                    if (cp.cloud.Equals(cloud, StringComparison.OrdinalIgnoreCase) &&
//                        cp.region.Equals(region, StringComparison.OrdinalIgnoreCase) &&
//                        cp.zone.Equals(zone, StringComparison.OrdinalIgnoreCase))
//                    {
//                        return true;
//                    }
//                }
//             }
//
//             return false;
//         }
//     }
// }
