using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using YBNpgsql;

namespace YBNpgsql.Tests;

public class YBFallBackTopologyExtended : YBFallbackTopolgyTests
{
    string connStringBuilder = "host=127.0.0.1,127.0.0.5,127.0.0.7;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;YB Servers Refresh Interval=10;Topology Keys=";
    int numConnections = 18;
    new void CreateCluster()
    {
        ExecuteShellCommand("/bin/yb-ctl destroy", "destroy cluster");
        var cmd = "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-1a,aws.us-west.us-west-1a,aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, "start cluster");
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"";
        ExecuteShellCommand(cmd, "add node (us-east-2a)");
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2b\"";
        ExecuteShellCommand(cmd, "add node (us-east-2b)");
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2c\"";
        ExecuteShellCommand(cmd, "add node (us-east-2c)");
    }
    void startYBDBClusterWithNineNodes() {

        ExecuteShellCommand( "/bin/yb-ctl destroy", "destroy cluster");

        ExecuteShellCommand( "/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" ",
            "start cluster");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"",
            "add node (us-east-2a)");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"",
            "add node (us-east-2a)");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-north.eu-north-2a\"",
            "add node (eu-north-2a)");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-west.eu-west-2a\"",
            "add node (eu-west-2a)");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-west.eu-west-2a\"",
            "add node (eu-west-2a)");
        ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-north.eu-north-2a\"",
            "add node (eu-north-2a)");
        Thread.Sleep(5000);
    }

    async Task createConnectionsWithoutCloseAndVerify(string tkValue, int[] counts) {
        List<NpgsqlConnection> connections = new List<NpgsqlConnection>();
        for (var i = 0; i < numConnections; i++) {
            NpgsqlConnection conn = new NpgsqlConnection(connStringBuilder + tkValue);
            conn.Open();
            connections.Add(conn);
        }
        Console.WriteLine("Created " + numConnections + " connections");

        var j = 1;
        Console.WriteLine("Client backend processes on ");
        foreach (var expectedCount in counts)
        {
            if (expectedCount != -1) {
                await VerifyOn("127.0.0." + j, expectedCount);
                Console.WriteLine(", ");
            }
            j++;
        }
    }
     [Test, Timeout(240000)]
    public async Task TestFallback()
    {

        CreateCluster();
        int[] count = { 4, 4, 4, 0, 0, 0 };
        var conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        var cmd = "/bin/yb-ctl stop_node 1";
        ExecuteShellCommand(cmd, "stop node 1");

        cmd = "/bin/yb-ctl stop_node 2";
        ExecuteShellCommand(cmd, "stop node 2");

        cmd = "/bin/yb-ctl stop_node 3";
        ExecuteShellCommand(cmd, "stop node 3");

        cmd = "/bin/yb-ctl stop_node 4";
        ExecuteShellCommand(cmd, "stop node 4");

        Thread.Sleep(15000);

        count = new[] { -1, -1, -1, -1, 12, 0 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        cmd = "/bin/yb-ctl start_node 4 --placement_info \"aws.us-east.us-east-2a\"";
        ExecuteShellCommand(cmd, "start node 4 (aws.us-east.us-east-2a)");
        Thread.Sleep(15000);

        count = new[] { -1, -1, -1, 12, 0, 0 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        cmd = "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, "start node 1 (aws.us-west.us-west-1a)");

        cmd = "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, "start node 2 (aws.us-west.us-west-1a)");

        Thread.Sleep(15000);

        count = new[] { 6, 6, -1, 0, 0, 0 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        DestroyCluster();
    }

    [Test, Timeout(240000)]
    public async Task CheckMultiNodeDown(){
    // Start RF=3 cluster with 9 nodes and with placements (127.0.0.1, 127.0.0.2, 127.0.0.3) -> us-west-1a,
    // and 127.0.0.4 -> us-east-2a, 127.0.0.5 -> us-east-2a and 127.0.0.6 -> eu-north-2a, 127.0.0.9 -> eu-north-2a,
    // and 127.0.0.7 -> eu-west-2a, 127.0.0.8 -> eu-west-2a.
    startYBDBClusterWithNineNodes();

    try
    {
        await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                     ".*:3,aws.eu-north.*:4", new[] { 6, 6, 6, 0, 0, 0, 0, 0, 0 });

        ExecuteShellCommand("/bin/yb-ctl stop_node 1", "stop node 1");
        ExecuteShellCommand("/bin/yb-ctl stop_node 2", "stop node 2");
        ExecuteShellCommand("/bin/yb-ctl stop_node 3", "stop node 3");
        ExecuteShellCommand("/bin/yb-ctl stop_node 4", "stop node 4");
        ExecuteShellCommand("/bin/yb-ctl stop_node 5", "stop node 5");
        ExecuteShellCommand("/bin/yb-ctl stop_node 7", "stop node 7");
        ExecuteShellCommand("/bin/yb-ctl stop_node 8", "stop node 8");
        await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                     ".*:3,aws.eu-north.*:4", new[] { -1, -1, -1, -1, -1, 9, -1, -1, 9 });

        ExecuteShellCommand("/bin/yb-ctl stop_node 9", "stop node 9");
        await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                     ".*:3,aws.eu-north.*:4", new[] { -1, -1, -1, -1, -1, 27, -1, -1, -1 });

        ExecuteShellCommand("/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\"",
            "start node 2 (aws.us-west.us-west-1a)");

        Thread.Sleep(15000);

        await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                     ".*:3,aws.eu-north.*:4", new[] { -1, 18, -1, -1, -1, 27, -1, -1, -1 });

        ExecuteShellCommand("/bin/yb-ctl stop_node 2", "stop node 2");
        await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                     ".*:3,aws.eu-north.*:4", new[]{-1, -1, -1, -1, -1, 45, -1, -1, -1 });

      ExecuteShellCommand("/bin/yb-ctl start_node 5 --placement_info \"aws.us-east.us-east-2a\"", "start node 5 (aws.us-east.us-east-2a)");

      Thread.Sleep(15000);

      await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                       ".*:3,aws.eu-north.*:4", new[]{-1, -1, -1, -1, 18, 45, -1, -1, -1});

      ExecuteShellCommand("/bin/yb-ctl stop_node 5", "stop node 5");
      await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
                                                       ".*:3,aws.eu-north.*:4", new[]{-1, -1, -1, -1, -1, 63, -1, -1, -1});

    } finally {
        ExecuteShellCommand("/bin/yb-ctl destroy", "destroy cluster");
    }
    }

    [Test, Timeout(240000)]
    public async Task checkNodeDownPrimary() {

      var savedConnString = connStringBuilder;
      connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;YB Servers Refresh Interval=10;Topology Keys=";

      ExecuteShellCommand("/bin/yb-ctl destroy", "destroy cluster");

      ExecuteShellCommand("/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" ",
          "start cluster");

      Thread.Sleep(15000);
      try {
          await createConnectionsWithoutCloseAndVerify( "aws.us-west.*:1", new[]{6, 6, 6});
          ExecuteShellCommand("/bin/yb-ctl stop_node 1", "stop node 1");
          await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1", new[]{-1, 15, 15});
          ExecuteShellCommand("/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"",
          "start node 1 (aws.us-west.us-west-1a)");
          ClusterAwareDataSource.forceRefresh = true;
          Thread.Sleep(5000);
          await createConnectionsWithoutCloseAndVerify("aws.us-west.*:1", new[]{16, 16, 16});

      } finally {
          connStringBuilder = savedConnString;
          ExecuteShellCommand("/bin/yb-ctl status", "cluster status");
      }
  }
}
