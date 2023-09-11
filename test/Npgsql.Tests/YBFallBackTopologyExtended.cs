using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests;

public class YBFallBackTopologyExtended : YBFallbackTopolgyTests
{

    static int mlock = 0;
    string connStringBuilder = "host=127.0.0.1,127.0.0.5;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;YB Servers Refresh Interval=10;Topology Keys=";

    [Test]
    public async Task TestFallback()
    {

        string? _Output = null;
        string? _Error = null;

        CreateCluster();
        int[] count = { 4, 4, 4, -1, -1, -1 };
        var conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        var cmd = "/bin/yb-ctl stop_node 1";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        cmd = "/bin/yb-ctl stop_node 2";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        cmd = "/bin/yb-ctl stop_node 3";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        cmd = "/bin/yb-ctl stop_node 4";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        count = new[] { -1, -1, -1, -1, 12, 0 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        cmd = "/bin/yb-ctl start_node 4 --placement_info \"aws.us-east.us-east-2a\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        Thread.Sleep(15000);

        count = new[] { -1, -1, -1, 12, 0, 0 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        cmd = "/bin/yb-ctl start_node 1 --placement_info \"aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        cmd = "/bin/yb-ctl start_node 2 --placement_info \"aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);

        Thread.Sleep(15000);

        count = new[] { 6, 6, -1, -1, -1, -1 };
        conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-1a:1,aws.us-east.us-east-2a:2,aws.us-east.us-east-2b:3,aws.us-east.us-east-2c:4", count);

        DestroyCluster();
    }
    static async Task<List<NpgsqlConnection>> CreateConnections(string connString, int[] counts)
    {
        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        if (mlock == 0)
        {
            mlock = 1;
            try
            {
                for (var i = 1; i <= 12; i++)
                {
                    NpgsqlConnection conn = new NpgsqlConnection(connString);
                    conn.Open();
                    conns.Add(conn);
                }

                Console.WriteLine("Connections Created");

                var j = 1;
                foreach (var count in counts)
                {
                    if (count != -1)
                    {
                        var host = "127.0.0." + j;
                        await VerifyOn(host, count);
                    }

                    j++;
                }
                CloseConnections(conns);
                mlock = 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failure:" + ex.Message);
                Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
                return conns;
            }
        }

        return conns;

    }

    static void CloseConnections(List<NpgsqlConnection>conns)
    {
        foreach (var conn in conns)
        {
            if (conn.State != System.Data.ConnectionState.Closed)
            {
                conn.Close();
            }
        }
    }

    void CreateCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-1a,aws.us-west.us-west-1a,aws.us-west.us-west-1a\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2a\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2b\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "/bin/yb-ctl add_node --placement_info \"aws.us-east.us-east-2c\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
    }

    void DestroyCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl destroy";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
    }
}
