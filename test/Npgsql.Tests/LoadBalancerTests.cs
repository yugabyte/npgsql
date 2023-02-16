using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests;

public class LoadBalancerTests : YBTestUtils
{
    int numConns = 6;

    [Test]
    public async Task TestLoadBalance1()
    {
        var connStringBuilder = "host=127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        createCluster();

        try
        {
            conns = CreateConnections(connStringBuilder, numConns);
            await VerifyOn("127.0.0.1", numConns/3, 0);
            await VerifyOn("127.0.0.2", numConns/3, 0);
            await VerifyOn("127.0.0.3", numConns / 3, 0);
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

            createCluster();
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

                await VerifyOn("127.0.0.2", 5, 0);
                await VerifyOn("127.0.0.3", 5, 0);
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
        List<NpgsqlConnection> conn1 = new List<NpgsqlConnection>();
        List<NpgsqlConnection> conn2 = new List<NpgsqlConnection>();

        createCluster();
        Console.WriteLine("Cluster created");

        try
        {
            Thread thr1 = new Thread(() => { conn1 = CreateConnections(connStringBuilder, numConns); });
            Thread thr2 = new Thread(() => { conn2 = CreateConnections(connStringBuilder, numConns); });
            thr1.Start();
            thr2.Start();
            thr1.Join();
            thr2.Join();
            conns.AddRange(conn1);
            conns.AddRange(conn2);
            await VerifyOn("127.0.0.1", 2 * numConns/3, 1);
            await VerifyOn("127.0.0.2", 2 * numConns/3, 1);
            await VerifyOn("127.0.0.3", 2 * numConns / 3, 1);
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

    void createCluster()
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


