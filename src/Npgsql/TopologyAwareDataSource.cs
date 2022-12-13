using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;
using Npgsql.Util;

namespace Npgsql;

/// <summary>
/// 
/// </summary>
public sealed class TopologyAwareDataSource: ClusterAwareDataSource
{
    HashSet<CloudPlacement> allowedPlacements;
    internal TopologyAwareDataSource(NpgsqlConnectionStringBuilder settings, NpgsqlDataSourceConfiguration dataSourceConfig) : base(settings,dataSourceConfig,false)
    {
        allowedPlacements = new HashSet<CloudPlacement>();
        PopulatePlacementMap();
        Console.WriteLine("Inside TopologyAwareDatasource");
        try
        {
            NpgsqlDataSource control = new UnpooledDataSource(settings, dataSourceConfig);
            NpgsqlConnection controlConnection = NpgsqlConnection.FromDataSource(control);
            controlConnection.Open();
            CreatePool(controlConnection);
            controlConnection.Close();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }

    void PopulatePlacementMap()
    {
        var placementstrings = settings.TopologyKeys?.Split(',');
        Debug.Assert(placementstrings != null, nameof(placementstrings) + " != null");
        foreach (var pl in placementstrings)
        {
            var placementParts = pl.Split('.');
            if (placementParts.Length != 3)
            {
                Console.WriteLine("Ignoring malformed Topology-Key Property");
                continue;
            }

            var cp = GetPlacementMap(placementParts[0], placementParts[1], placementParts[2]);
            allowedPlacements.Add(cp);
        }
        
    }

    CloudPlacement GetPlacementMap(string cloud, string region, string zone)
    {
        var cp = new CloudPlacement(cloud, region, zone);
        return cp;
    }

    /// <summary>
    /// Create a new pool
    /// </summary>
    internal new void CreatePool(NpgsqlConnection conn)
    {
        _hosts = GetCurrentServers(conn);
        foreach(var host in _hosts)
        {
            var poolSettings = settings.Clone();
            poolSettings.Host = host.ToString();

            _pools.Add(settings.Pooling
                ? new PoolingDataSource(poolSettings, dataSourceConfig)
                : new UnpooledDataSource(poolSettings, dataSourceConfig));
        }
    }

    new List<string> GetCurrentServers(NpgsqlConnection conn)
    {
        NpgsqlCommand QUERY_SERVER = new NpgsqlCommand("Select * from yb_servers()",conn);
        NpgsqlDataReader reader = QUERY_SERVER.ExecuteReader();
        List<string> currentPrivateIps = new List<string>();
        List<string> currentPublicIps = new List<string>();
        var hostConnectedTo = conn.Host;
        
        Debug.Assert(hostConnectedTo != null, nameof(hostConnectedTo) + " != null");
        while (reader.Read())
        {
            var host = reader.GetString(0);
            var publicHost = reader.GetString(7);
            var cloud = reader.GetString(4);
            var region = reader.GetString(5);
            var zone = reader.GetString(6);

            var cp = GetPlacementMap(cloud, region, zone);
            
            if (hostConnectedTo.Equals(host))
            {
                UseHostColumn = true;
            } else if (hostConnectedTo.Equals(publicHost)) {
                UseHostColumn = false;
            }

            foreach (var allowedPlacement in allowedPlacements)
            {
                if (allowedPlacement.EqualPlacements(cp))
                {
                    currentPrivateIps.Add(host);
                    currentPublicIps.Add(publicHost);
                }
            }
        }
        return GetPrivateOrPublicServers(currentPrivateIps, currentPublicIps);
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

    }
}
