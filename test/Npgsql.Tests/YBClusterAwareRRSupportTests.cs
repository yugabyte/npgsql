using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBClusterAwareRRSupportTests : YBTestUtils{
    int numConns = 6;
    [Test, Timeout(60000)]
    public async Task TestOnlyPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();

        Thread.Sleep(15000);

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns / 3);
            await VerifyOn("127.0.0.2", numConns / 3);
            await VerifyOn("127.0.0.3", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }

    [Test, Timeout(60000)]
    public async Task TestPreferPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns / 3);
            await VerifyOn("127.0.0.2", numConns / 3);
            await VerifyOn("127.0.0.3", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }
    [Test, Timeout(60000)]
    public async Task TestPreferPrimaryAllNodesDown()
    {
        var connStringBuilder = "host=127.0.0.4;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();
        // Stop node: 127.0.0.1, 127.0.0.2, 127.0.0.3

        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl stop_node 1";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);
        cmd = "/bin/yb-ctl stop_node 2";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);
        cmd = "/bin/yb-ctl stop_node 3";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }
    [Test, Timeout(60000)]
    public async Task TestOnlyRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }

    [Test, Timeout(60000)]
    public async Task TestPreferRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", 0);
            await VerifyOn("127.0.0.2", 0);
            await VerifyOn("127.0.0.3", 0);
            await VerifyOn("127.0.0.4", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }

    [Test, Timeout(60000)]
    public async Task TestPreferRRAllNodesDown()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();
        // Stop node : 127.0.0.4, 127.0.0.5, 127.0.0.6

        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl stop_node 4";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);
        cmd = "/bin/yb-ctl stop_node 5";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);
        cmd = "/bin/yb-ctl stop_node 6";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);
        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns / 3);
            await VerifyOn("127.0.0.2", numConns / 3);
            await VerifyOn("127.0.0.3", numConns / 3);
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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
        }
    }
    [Test, Timeout(60000)]
    public async Task TestAny()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateRRCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns / 6);
            await VerifyOn("127.0.0.2", numConns / 6);
            await VerifyOn("127.0.0.3", numConns / 6);
            await VerifyOn("127.0.0.4", numConns / 6);
            await VerifyOn("127.0.0.5", numConns / 6);
            await VerifyOn("127.0.0.6", numConns / 6);

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
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
            VerifyLocal("127.0.0.4", 0);
            VerifyLocal("127.0.0.5", 0);
            VerifyLocal("127.0.0.6", 0);
            DestroyCluster();
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

    void CreateRRCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl create --rf 3 --placement_info cloud1.datacenter1.rack1,cloud1.datacenter2.rack1,cloud1.datacenter3.rack1 --tserver_flags \"placement_uuid=live,max_stale_read_bound_time_ms=60000000\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/build/latest/bin/yb-admin --master_addresses 127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100 modify_placement_info cloud1.datacenter1.rack1,cloud1.datacenter2.rack1,cloud1.datacenter3.rack1 3 live";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node --placement_info cloud1.datacenter2.rack1 --tserver_flags placement_uuid=rr";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node--placement_info cloud1.datacenter3.rack1 --tserver_flags placement_uuid=rr";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node --placement_info cloud1.datacenter4.rack1 --tserver_flags placement_uuid=rr";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
    }

    protected void DestroyCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl destroy";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
    }
}
