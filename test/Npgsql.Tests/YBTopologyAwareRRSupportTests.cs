using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBTopologyAwareRRSupportTests : YBTestUtils
{
     int numConns = 6;
    [Test]
    public async Task TestOnlyPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", numConns);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestOnlyPrimaryAllNodesDownAllPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.2, 127.0.0.3

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", -1);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public void TestOnlyPrimaryAllNodesDownInAllPlacementsFallBackToTopologyOnly()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;FallBack To Topology Keys Only=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.2, 127.0.0.3

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);

        }
        catch (NpgsqlException ex)
        {
            if (ex.Message.Equals("No suitable host was found", StringComparison.OrdinalIgnoreCase))
                Console.WriteLine("Expected Failure:" + ex.Message);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestOnlyPrimaryAllNodesDownPrimarylacement()
    {
        var connStringBuilder = "host=127.0.0.3;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.2

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", numConns);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", numConns);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferPrimaryAllNodesDownPrimaryPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.2

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", numConns);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }
    [Test]
    public async Task? TestPreferPrimaryAllPrimaryNodesDown()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.1, 127.0.0.2, 127.0.0.3

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", -1);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", -1);
            await VerifyOn("127.0.0.4", numConns / 3 );
            await VerifyOn("127.0.0.5", numConns / 3);
            await VerifyOn("127.0.0.6", numConns / 3);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferPrimaryAllNodesDownAllPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary; Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.2, 127.0.0.3

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", -1);
            await VerifyOn("127.0.0.4", 0);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }
    [Test]
    public async Task TestOnlyRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestOnlyRRAllNodesDownInPrimaryPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", numConns);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestOnlyRRAllNodesDownInAllPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4, 127.0.0.5

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", -1);
            await VerifyOn("127.0.0.6", numConns);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public void TestOnlyRRAllNodesDownInAllPlacementsFallBackToTopologyOnly()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;FallBack To Topology Keys Only=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4, 127.0.0.5

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);

        }
        catch (NpgsqlException ex)
        {
            if (ex.Message.Equals("No suitable host was found", StringComparison.OrdinalIgnoreCase))
                Console.WriteLine("Expected Failure:" + ex.Message);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferRRAllNodesDownInPrimaryPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", numConns);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferRRAllNodesDownInAllPlacements()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4, 127.0.0.5

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", -1);
            await VerifyOn("127.0.0.6", numConns);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task? TestPreferRRAllRRNodesDown()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.4, 127.0.0.5, 127.0.0.6

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns / 3 );
            await VerifyOn("127.0.0.2", numConns / 3);
            await VerifyOn("127.0.0.3", numConns / 3);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", -1);
            await VerifyOn("127.0.0.6", -1);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }
    [Test]
    public async Task TestAny()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", numConns / 2);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns / 2);
            await VerifyOn("127.0.0.5", 0);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestAnyAllNodesDownPrimaryPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.2, 127.0.0.4

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", numConns /2);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", numConns / 2);
            await VerifyOn("127.0.0.6", 0);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public void TestAnyAllNodesDownAllPlacementFallBackToTopologyOnly()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2; FallBack To Topology Keys Only=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.2, 127.0.0.3, 127.0.0.4, 127.0.0.5

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);

        }
        catch (NpgsqlException ex)
        {
            if (ex.Message.Equals("No suitable host was found", StringComparison.OrdinalIgnoreCase))
                Console.WriteLine("Expected Failure:" + ex.Message);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestAnyAllNodesDownAllPlacement()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Topology Keys=cloud1.datacenter2.rack1:1,cloud1.datacenter3.rack1:2;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop Node: 127.0.0.2, 127.0.0.3, 127.0.0.4, 127.0.0.5

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns /2);
            await VerifyOn("127.0.0.2", -1);
            await VerifyOn("127.0.0.3", -1);
            await VerifyOn("127.0.0.4", -1);
            await VerifyOn("127.0.0.5", -1);
            await VerifyOn("127.0.0.6", numConns /2);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            foreach (var conn in conns)
            {
                conn.Close();
            }
            // DestroyCluster();
        }
    }

    static List<NpgsqlConnection> CreateConnections(string connString, int numConns)
    {
        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        try
        {
            for (var i = 1; i <= numConns; i++)
            {
                NpgsqlConnection conn = new NpgsqlConnection(connString);
                conn.Open();
                conns.Add(conn);
            }

            Console.WriteLine("Connections Created");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
            return conns;
        }

        return conns;

    }
}
