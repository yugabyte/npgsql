using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests;

public class YBFallbackTopolgyTests
{
    static int mlock = 0;
    string connStringBuilder = "host=127.0.0.3;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0;Topology Keys=";

    [Test]
    public async Task TestFallback1()
    {
        var conns = await CreateConnections(connStringBuilder+"aws.us-west.us-west-2a,aws.us-west.us-west-2c", 6, 0, 6);
        CloseConnections(conns);
    }
    [Test]
    public async Task TestFallback2()
    {
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.us-west-2a,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", 6, 6, 0);
        CloseConnections(conns);
    }

    [Test]
    public async Task TestFallback3()
    {
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.us-west-2a:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", 12, 0, 0);
        CloseConnections(conns);
    }

    [Test]
    public async Task TestFallback4()
    {
        var conns = await CreateConnections(connStringBuilder + "aws.us-west.*,aws.us-west.us-west-2b:1,aws.us-west.us-west-2c:2", 4, 4, 4);
        CloseConnections(conns);
    }

    [Test]
    public async Task TestFallback5()
    {
        var conns =await CreateConnections(connStringBuilder + "aws.us-west.*:1,aws.us-west.us-west-2b:2,aws.us-west.us-west-2c:3", 4, 4, 4);
        CloseConnections(conns);
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

    static async Task VerifyOn(string server, int ExpectedCount)
    {
        var url = string.Format("http://{0}:{1}/rpcz", server, 13000);
        var client = new HttpClient();
        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();
        var responseBody = await response.Content.ReadAsStringAsync();
        var count = responseBody.Split("client backend");
        Assert.AreEqual(ExpectedCount, count.Length - 1);
    }
}
