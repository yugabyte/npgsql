using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests;

public class YBFallbackTopolgyTests : YBTestUtils
{
    static int mlock = 0;
    string connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;Topology Keys=";

    [Test]
    public async Task TestFallback1()
    {
        CreateCluster();
        var conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-2a,aws.us-west.us-west-2c", 6, 0, 6);
        CloseConnections(conns);
        DestroyCluster();
    }
    [Test]
    public async Task TestFallback2()
    {
        CreateCluster();
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", 6, 6, 0);
        CloseConnections(conns);
        DestroyCluster();
    }

    [Test]
    public async Task TestFallback3()
    {
        CreateCluster();
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", 12, 0, 0);
        CloseConnections(conns);
        DestroyCluster();
    }

    [Test]
    public async Task TestFallback4()
    {
        CreateCluster();
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", 4, 4, 4);
        CloseConnections(conns);
        DestroyCluster();
    }

    [Test]
    public async Task TestFallback5()
    {
        CreateCluster();
        var connString = connStringBuilder + "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3";

        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();

        await VerifyOn("127.0.0.1", 1);

        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl stop_node 1";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);

        var conns =await CreateConnections(connString, 0, 12, 0);

        CloseConnections(conns);
        DestroyCluster();
    }

    [Test]
    public async Task TestFallback6()
    {
        CreateCluster();
        var connString = connStringBuilder + "aws.us-west.us-west-2a:1";

        NpgsqlConnection conn = new NpgsqlConnection(connString);
        conn.Open();

        await VerifyOn("127.0.0.1", 1);

        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl stop_node 1";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine(_Output);

        var conns = await CreateConnections(connString, 0, 6, 6);
        CloseConnections(conns);
        DestroyCluster();
    }

    static async Task<List<NpgsqlConnection>> CreateConnections(string connString, int cnt1, int cnt2, int cnt3)
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

                await VerifyOn("127.0.0.1", cnt1);
                await VerifyOn("127.0.0.2", cnt2);
                await VerifyOn("127.0.0.3", cnt3);
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
        var cmd = "/bin/yb-ctl start --rf 3 --placement_info \"aws.us-west.us-west-2a,aws.us-west.us-west-2b,aws.us-west.us-west-2c\"";
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
