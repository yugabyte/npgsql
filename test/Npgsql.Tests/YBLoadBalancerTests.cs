using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBLoadBalancerTests : YBTestUtils
{
    int numConns = 6;

    [Test]
    public void TestLoadBalance1()
    {
        var connStringBuilder1 = "host=127.0.0.1;database=yugabyte;userid=postgres;password=postgres;Load Balance Hosts=any;Timeout=0;";
        var connStringBuilder2 = "host=127.0.0.1;database=yugabyte;userid=tester;password=abc123;Load Balance Hosts=any;Timeout=0;";


        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

        try
        {
            // for (var i = 1; i <= numConns; i++)
            // {
            NpgsqlConnection conn = new NpgsqlConnection(connStringBuilder1);
            NpgsqlConnection conn1 = new NpgsqlConnection(connStringBuilder1);
            NpgsqlConnection conn2 = new NpgsqlConnection(connStringBuilder2);
            conn.Open();
            conn.Close();
            conn1.Open();
            for (var i = 0; i < 6; i++)
            {
                NpgsqlConnection conn3 = new NpgsqlConnection(connStringBuilder1);
                conn3.Open();
                conns.Add(conn3);
            }
            conn2.Open();

            // NpgsqlCommand cmd = new NpgsqlCommand("SELECT current_user;", conn);
            // NpgsqlDataReader reader = cmd.ExecuteReader();
            // Console.WriteLine("User 1: ");
            // while (reader.Read())
            // {
            //     Console.WriteLine("{0}", reader.GetString(0));
            // }

            NpgsqlCommand cmd1 = new NpgsqlCommand("SELECT current_user;", conn1);
            NpgsqlDataReader reader1 = cmd1.ExecuteReader();
            Console.WriteLine("User 1: ");
            while (reader1.Read())
            {
                Console.WriteLine("{0}", reader1.GetString(0));
            }
            // }

            NpgsqlCommand cmd2 = new NpgsqlCommand("SELECT current_user;", conn2);
            NpgsqlDataReader reader2 = cmd2.ExecuteReader();
            Console.WriteLine("User 2: ");
            while (reader2.Read())
            {
                Console.WriteLine("{0}", reader2.GetString(0));
            }

            Console.WriteLine("Connections Created");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        // finally
        // {
        //     foreach (var conn in conns)
        //     {
        //         conn.Close();
        //     }
        //     Console.WriteLine("Verifying if all connections are closed...");
        //     // VerifyLocal("127.0.0.1", 0);
        //     // VerifyLocal("127.0.0.2", 0);
        //     // VerifyLocal("127.0.0.3", 0);
        //     // DestroyCluster();
        // }
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
                VerifyLocal("127.0.0.2", 0);
                VerifyLocal("127.0.0.3", 0);
                DestroyCluster();
            }
        }

    [Test]
    public async Task TestLoadBalance3()
    {
        var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

        List<NpgsqlConnection> allConns = new List<NpgsqlConnection>();
        CreateCluster();
        try
        {
            List<Thread> threads = new List<Thread>();
            List<NpgsqlConnection> conn = new List<NpgsqlConnection>();
            var numThreads = 15;
            for (var i = 0; i < numThreads; i++)
            {
                Thread thread = new Thread(() => {
                    var threadConns = CreateConnections(connStringBuilder, numConns); // Each thread uses its own list
                    lock (allConns)
                    {
                        allConns.AddRange(threadConns); // Safely add to the shared list
                    }
                });
                threads.Add(thread);
            }

            foreach (var thread in threads)
            {
                thread.Start();
            }

            foreach (var thread in threads)
            {
                thread.Join();
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
            Console.WriteLine("Conns count" + allConns.Count);
            foreach (var conn in allConns)
            {
                conn.Close();
            }
            VerifyLocal("127.0.0.1", 0);
            VerifyLocal("127.0.0.2", 0);
            VerifyLocal("127.0.0.3", 0);
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


