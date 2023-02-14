using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests;

public class LoadBalancerTests
{
    [Test]
    public async Task TestLoadBalance1()
        {
            var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

            try
            {
                await CreateConnections(connStringBuilder, 2, 2, 2);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failure:" + ex.Message);
                Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
            }
        }

    [Test]
    public async Task TestLoadBalance2()
        {
                var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";
                List<NpgsqlConnection> conns = new List<NpgsqlConnection>();

                try
                {
                    for (var i = 1; i <= 6; i++)
                    {
                        NpgsqlConnection conn = new NpgsqlConnection(connStringBuilder);
                        Console.WriteLine("Connection created");
                        conn.Open();
                        Console.WriteLine("Connection open");
                        conns.Add(conn);
                    }

                    Console.WriteLine("Stop node 1");

                    System.Threading.Thread.Sleep(300000);

                    for (var i = 1; i <= 6; i++)
                    {
                        NpgsqlConnection conn = new NpgsqlConnection(connStringBuilder);
                        Console.WriteLine("Connection created");
                        conn.Open();
                        Console.WriteLine("Connection open");
                        conns.Add(conn);
                    }

                    await VerifyOn("127.0.0.2", 5);
                    await VerifyOn("127.0.0.3", 5);
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
                        if (conn.State != System.Data.ConnectionState.Closed)
                        {
                            conn.Close();
                        }
                    }
                }
        }

    static async Task<List<NpgsqlConnection>> CreateConnections(string connString, int cnt1, int cnt2, int cnt3)
    {
        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        try
        {
            for (var i = 1; i <= 6; i++)
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
        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
            return conns;
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


