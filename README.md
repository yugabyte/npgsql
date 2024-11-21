# Npgsql - the .NET data provider for PostgreSQL

[![stable](https://img.shields.io/nuget/v/Npgsql.svg?label=stable)](https://www.nuget.org/packages/Npgsql/)
[![next patch](https://img.shields.io/myget/npgsql/v/npgsql.svg?label=next%20patch)](https://www.myget.org/feed/npgsql/package/nuget/Npgsql)
[![daily builds (vnext)](https://img.shields.io/myget/npgsql-vnext/v/npgsql.svg?label=vnext)](https://www.myget.org/feed/npgsql-vnext/package/nuget/Npgsql)
[![build](https://github.com/npgsql/npgsql/actions/workflows/build.yml/badge.svg)](https://github.com/npgsql/npgsql/actions/workflows/build.yml)
[![gitter](https://img.shields.io/badge/gitter-join%20chat-brightgreen.svg)](https://gitter.im/npgsql/npgsql)

## What is Npgsql?

Npgsql is the open source .NET data provider for PostgreSQL. It allows you to connect and interact with PostgreSQL server using .NET.

For the full documentation, please visit [the Npgsql website](https://www.npgsql.org). For the Entity Framework Core provider that works with this provider, see [Npgsql.EntityFrameworkCore.PostgreSQL](https://github.com/npgsql/efcore.pg).

## Quickstart

Here's a basic code snippet to get you started:

```csharp
using Npgsql;

var connString = "Host=myserver;Username=mylogin;Password=mypass;Database=mydatabase";

var dataSourceBuilder = new NpgsqlDataSourceBuilder(connString);
var dataSource = dataSourceBuilder.Build();

var conn = await dataSource.OpenConnectionAsync();

// Insert some data
await using (var cmd = new NpgsqlCommand("INSERT INTO data (some_field) VALUES (@p)", conn))
{
    cmd.Parameters.AddWithValue("p", "Hello world");
    await cmd.ExecuteNonQueryAsync();
}

// Retrieve all rows
await using (var cmd = new NpgsqlCommand("SELECT some_field FROM data", conn))
await using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
        Console.WriteLine(reader.GetString(0));
}
```

## Key features

* High-performance PostgreSQL driver. Regularly figures in the top contenders on the [TechEmpower Web Framework Benchmarks](https://www.techempower.com/benchmarks/).
* Full support of most PostgreSQL types, including advanced ones such as arrays, enums, ranges, multiranges, composites, JSON, PostGIS and others.
* Highly-efficient bulk import/export API.
* Failover, load balancing and general multi-host support.
* Great integration with Entity Framework Core via [Npgsql.EntityFrameworkCore.PostgreSQL](https://www.nuget.org/packages/Npgsql.EntityFrameworkCore.PostgreSQL).

For the full documentation, please visit the Npgsql website at [https://www.npgsql.org](https://www.npgsql.org).

## YugabyteDB Npgsql Features


Yugabyte Npgsql driver is a distributed .NET driver for YSQL built on the PostgreSQL Npgsql driver. Although the upstream PostgreSQL driver works with YugabyteDB, the Yugabyte driver enhances YugabyteDB by eliminating the need for external load balancers.

* It is cluster-aware, which eliminates the need for an external load balancer.
* It is topology-aware, which is essential for geographically-distributed applications. The driver uses servers that are part of a set of geo-locations specified by topology keys.

### Load balancing

The Yugabyte Npgsql driver has the following load balancing features:

* Uniform load balancing

In this mode, the driver makes the best effort to uniformly distribute the connections to each YugabyteDB server. For example, if a client application creates 100 connections to a YugabyteDB cluster consisting of 10 servers, then the driver creates 10 connections to each server. If the number of connections are not exactly divisible by the number of servers, then a few may have 1 less or 1 more connection than the others. This is the client view of the load, so the servers may not be well balanced if other client applications are not using the Yugabyte JDBC driver.

* Topology-aware load balancing

Because YugabyteDB clusters can have servers in different regions and availability zones, the YugabyteDB JDBC driver is topology-aware, and can be configured to create connections only on servers that are in specific regions and zones. This is useful for client applications that need to connect to the geographically nearest regions and availability zone for lower latency; the driver tries to uniformly load only those servers that belong to the specified regions and zone.

### Usage

Load balancing connection properties:

The following connection properties are added to enable load balancing:

* Load Balance Hosts - Starting with version 8.0.3.2, it expects one of False, Any (same as true), OnlyPrimary, OnlyRR, PreferPrimary and PreferRR as its possible values.
    * False - No connection load balancing. Behaviour is similar to vanilla Npgsql driver
    * Any - Same as value true. Distribute connections equally across all nodes in the cluster, irrespective of its type (primary or read-replica)
    * OnlyPrimary - Create connections equally across only the primary nodes of the cluster
    * OnlyRR - Create connections equally across only the read-replica nodes of the cluster
    * PreferPrimary - Create connections equally across primary cluster nodes. If none available, on any available read replica node in the cluster
    * PreferRR - Create connections equally across read replica nodes of the cluster. If none available, on any available primary cluster node
* Topology Keys - provide comma-separated geo-location values to enable topology-aware load balancing. Geo-locations can be provided as cloud.region.zone.
* YB Servers Refresh Interval - The list of servers, to balance the connection load on, are refreshed periodically every 5 minutes by default. This time can be regulated by this property.
* Fallback To Topology Keys Only - Decides if the driver can fall back to nodes outside of the given placements for new connections, if the nodes in the given placements are not available. Value true means stick to explicitly given placements for fallback, else fail. Value false means fall back to entire cluster nodes when nodes in the given placements are unavailable. Default is false. It is ignored if topology-keys is not specified or load-balance is set to either prefer-primary or prefer-rr.
* Failed Host Reconnect Delay Sec - When the driver cannot connect to a server, it marks it as failed with a timestamp. Later, whenever it refreshes the server list via yb_servers(), if it sees the failed server in the response, it marks the server as UP only if the time specified via this property has elapsed since the time it was last marked as a failed host. Default is 5 seconds.
* Enable Discard Sequence - Flag to enable/disable running `Discard sequences` on connection Reset
* Enable Discard Temp - Flag to enable/disable running `Discard Temp` on connection Reset
* Enable Close All - Flag to enable/disable running `Close All` on connection Reset
* Enable Discard Al - Flag to enable/disable running `Discard All` on connection Reset

Pass new connection properties for load balancing in the connection string. To enable uniform load balancing across all servers, you set the `Load Balance Hosts` property to True in the URL, as per the following example.

Connection String::

```csharp
var connString = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";
```

#### Note: The behaviour of `Load Balance Hosts` is different in YugabyteDB Npgsql Driver as compared to the upstream driver. The upstream driver balances connections on the list of hosts provided in the `host` property while the YugabyteDB Npgsql Driver balances the connections on the list of servers returned by the `yb_servers()` function.

To specify topology keys, you set the `Topology Keys` property to comma separated values, as per the following example.

Connection String::

```csharp
var connString = "host=127.0.0.3;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;Topology Keys=cloud.region.zone";
```

Multiple topologies can also be passed to the Topology Keys property, and each of them can also be given a preference value, as per the following example.

```csharp
var connString = "host=127.0.0.3;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;Topology Keys=cloud1.region1.zone1:1,cloud2.region2.zone2:2";
```