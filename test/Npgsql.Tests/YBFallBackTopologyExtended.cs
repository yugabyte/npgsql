// using System;
// using System.Collections.Generic;
// using System.Threading;
// using System.Threading.Tasks;
// using NUnit.Framework;
// using YBNpgsql;
//
// namespace YBNpgsql.Tests;
//
// public class YBFallBackTopologyExtended : YBFallbackTopolgyTests
// {
//
//     static int mlock = 0;
//     string connStringBuilder = "host=127.0.0.1,127.0.0.5;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;YB Servers Refresh Interval=10;Topology Keys=";
//
//     new void CreateCluster()
//     {
//         string? _Output = null;
//         string? _Error = null;
//         var cmd = "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-1a,aws.us-west.us-west-1a,aws.us-west.us-west-1a\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//         cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//         cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2b\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//         cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2c\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//     }
//     void startYBDBClusterWithNineNodes() {
//
//         string? _Output = null;
//         string? _Error = null;
//         ExecuteShellCommand( "/bin/yb-ctl destroy",  ref _Output, ref _Error );
//
//         ExecuteShellCommand( "/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" ",
//             ref _Output, ref _Error );
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"",
//             ref _Output, ref _Error );
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"",
//             ref _Output, ref _Error );
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-north.eu-north-2a\"",
//             ref _Output, ref _Error );
//
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-west.eu-west-2a\"",
//             ref _Output, ref _Error );
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-west.eu-west-2a\"",
//             ref _Output, ref _Error );
//         ExecuteShellCommand( "/bin/yb-ctl add_node --placement_info \"aws.eu-north.eu-north-2a\"",
//             ref _Output, ref _Error );
//         Thread.Sleep(5000);
//     }
//
//     void createConnectionsWithoutCloseAndVerify(String url, String tkValue, ArrayList<Integer> counts) throws SQLException {
//         Connection[] connections = new Connection[numConnections];
//         for (int i = 0; i < numConnections; i++) {
//         try {
//             connections[i] = DriverManager.getConnection(url + tkValue, "yugabyte", "yugabyte");
//         } catch (PSQLException e) {
//             if (counts.get(0) != -1) {
//                 throw new RuntimeException("Did not expect an exception! ", e);
//             }
//             System.out.println(e.getCause());
//             if (!(e.getCause() instanceof IllegalArgumentException)) {
//                 throw new RuntimeException("Did not expect this exception! ", e);
//             }
//             return;
//         }
//     }
//     System.out.println("Created " + numConnections + " connections");
//
//     int j = 1;
//     System.out.print("Client backend processes on ");
//         for (int expectedCount : counts) {
//         if (expectedCount != -1) {
//             verifyOn("127.0.0." + j, expectedCount, (j > 3 && j < 7) ? "skip" : tkValue);
//             System.out.print(", ");
//         }
//         j++;
//     }
//     System.out.println("");
// }
//     [Test]
//     public async Task TestFallback()
//     {
//
//         string? _Output = null;
//         string? _Error = null;
//
//         CreateCluster();
//         int[] count = { 4, 4, 4, -1, -1, -1 };
//         var conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);
//
//         var cmd = "/bin/yb-ctl stop_node 1";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         cmd = "/bin/yb-ctl stop_node 2";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         cmd = "/bin/yb-ctl stop_node 3";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         cmd = "/bin/yb-ctl stop_node 4";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         count = new[] { -1, -1, -1, -1, 12, 0 };
//         conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);
//
//         cmd = "/bin/yb-ctl start_node 4 --placement_info \"aws.us-east.us-east-2a\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//         Thread.Sleep(15000);
//
//         count = new[] { -1, -1, -1, 12, 0, 0 };
//         conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);
//
//         cmd = "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         cmd = "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\"";
//         ExecuteShellCommand(cmd, ref _Output, ref _Error );
//         Console.WriteLine("Output:" + _Output);
//
//         Thread.Sleep(15000);
//
//         count = new[] { 6, 6, -1, -1, -1, -1 };
//         conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);
//
//         DestroyCluster();
//     }
//
//     [Test]
//     public async Task CheckMultiNodeDown(){
//     // Start RF=3 cluster with 9 nodes and with placements (127.0.0.1, 127.0.0.2, 127.0.0.3) -> us-west-1a,
//     // and 127.0.0.4 -> us-east-2a, 127.0.0.5 -> us-east-2a and 127.0.0.6 -> eu-north-2a, 127.0.0.9 -> eu-north-2a,
//     // and 127.0.0.7 -> eu-west-2a, 127.0.0.8 -> eu-west-2a.
//     startYBDBClusterWithNineNodes();
//     var url = "jdbc:yugabytedb://127.0.0.1:5433,127.0.0.4:5433,127.0.0" +
//         ".7:5433/yugabyte?load-balance=true&yb-servers-refresh-interval=10&topology-keys=";
//
//     try {
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(6, 6, 6, 0, 0, 0, 0, 0, 0));
//
//       executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 3", "Stop node 3", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 4", "Stop node 4", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 5", "Stop node 5", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 7", "Stop node 7", 10);
//       executeCmd(path + "/bin/yb-ctl stop_node 8", "Stop node 8", 10);
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, -1, -1, -1, -1, 9, -1, -1, 9));
//
//       executeCmd(path + "/bin/yb-ctl stop_node 9", "Stop node 9", 10);
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, -1, -1, -1, -1, 27, -1, -1, -1));
//
//       executeCmd(path + "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\"",
//           "Start node 2", 10);
//       try {
//         Thread.sleep(15000);
//       } catch (InterruptedException ie) {
//       }
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, 18, -1, -1, -1, 27, -1, -1, -1));
//
//       executeCmd(path + "/bin/yb-ctl stop_node 2", "Stop node 2", 10);
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, -1, -1, -1, -1, 45, -1, -1, -1));
//
//       executeCmd(path + "/bin/yb-ctl start_node 5 --placement_info \"aws.us-east.us-east-2a\"",
//           "Start node 5", 10);
//       try {
//         Thread.sleep(15000);
//       } catch (InterruptedException ie) {
//       }
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, -1, -1, -1, 18, 45, -1, -1, -1));
//
//       executeCmd(path + "/bin/yb-ctl stop_node 5", "Stop node 5", 10);
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1,aws.us-east.*:2,aws.eu-west" +
//           ".*:3,aws.eu-north.*:4", expectedInput(-1, -1, -1, -1, -1, 63, -1, -1, -1));
//
//     } finally {
//       executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
//     }
//   }
//
//   private static void checkNodeDownPrimary() throws SQLException {
//
//     executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
//
//     executeCmd(path + "/bin/yb-ctl --rf 3 start --placement_info \"aws.us-west.us-west-1a\" ",
//         "Start YugabyteDB rf=3 cluster", 15);
//
//     String url = "jdbc:yugabytedb://127.0.0.1:5433/yugabyte?load-balance=true&topology-keys=";
//
//     try {
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1", expectedInput(6, 6, 6));
//
//       executeCmd(path + "/bin/yb-ctl stop_node 1", "Stop node 1", 10);
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1", expectedInput(-1, 15, 15));
//
//       executeCmd(path + "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"",
//           "Start node 1", 10);
//       ClusterAwareLoadBalancer.forceRefresh = true;
//       try {
//         Thread.sleep(5000);
//       } catch (InterruptedException ie) {
//       }
//       createConnectionsWithoutCloseAndVerify(url, "aws.us-west.*:1", expectedInput(16, 16, 16));
//
//     } finally {
//       executeCmd(path + "/bin/yb-ctl destroy", "Stop YugabyteDB cluster", 10);
//     }
//   }
// }
