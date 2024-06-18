using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class LoadBalancerTests : YBTestUtils
{
    int numConns = 6;

    [Test]
    public async Task TestLoadBalance1()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns/3);
            await VerifyOn("127.0.0.2", numConns/3);
            await VerifyOn("127.0.0.3", numConns / 3);
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
            DestroyCluster();
        }
    }

    [Test]
    public async Task TestLoadBalance2()
        {
            var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;YB Servers Refresh Interval=30;Timeout=0";
            List<NpgsqlConnection> conns = new List<NpgsqlConnection>();

            CreateCluster();
            try
            {
                var conn1 = CreateConnections(connStringBuilder, numConns);
                conns.AddRange(conn1);

                string? _Output = null;
                string? _Error = null;
                var cmd = "/bin/yb-ctl stop_node 1";
                ExecuteShellCommand(cmd, ref _Output, ref _Error );
                Console.WriteLine(_Output);

                System.Threading.Thread.Sleep(30000);

                var conn2 = CreateConnections(connStringBuilder, numConns);
                conns.AddRange(conn2);

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
                DestroyCluster();
            }
        }

    [Test]
    public async Task TestLoadBalance3()
    {
        var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        try
        {
            List<Thread> threads = new List<Thread>();
            List<NpgsqlConnection> conn = new List<NpgsqlConnection>();
            var numThreads = 15;
            for (var i = 0; i < numThreads; i++)
            {
                Thread thread = new Thread(() => { conn = CreateConnections(connStringBuilder, numConns); });
                threads.Add(thread);
            }

            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
                conns.AddRange(conn);
            }
            await VerifyOn("127.0.0.1", numThreads * numConns/3);
            await VerifyOn("127.0.0.2", numThreads * numConns/3);
            await VerifyOn("127.0.0.3", numThreads * numConns / 3);
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
            DestroyCluster();
        }
    }

    void CreateCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl create --rf 3";
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


