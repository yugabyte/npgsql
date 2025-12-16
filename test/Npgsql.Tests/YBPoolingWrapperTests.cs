using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBPoolingWrapperTests : YBTestUtils
{
    [Test]
    public void TestPoolingForMultileConnStrings()
    {
        var connStringBuilder1 = "host=127.0.0.1;database=yugabyte;userid=postgres;password=postgres;Load Balance Hosts=any;Timeout=0;";
        var connStringBuilder2 = "host=127.0.0.1;database=yugabyte;userid=tester;password=abc123;Load Balance Hosts=any;Timeout=0;";


        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateCluster();

        try
        {
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

            NpgsqlCommand cmd1 = new NpgsqlCommand("SELECT current_user;", conn1);
            NpgsqlDataReader reader1 = cmd1.ExecuteReader();
            while (reader1.Read())
            {
                Assert.AreEqual(reader1.GetString(0), "postgres");
            }

            NpgsqlCommand cmd2 = new NpgsqlCommand("SELECT current_user;", conn2);
            NpgsqlDataReader reader2 = cmd2.ExecuteReader();
            while (reader2.Read())
            {
                Assert.AreEqual(reader2.GetString(0), "tester");
            }

            Console.WriteLine("Connections Created");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            DestroyCluster();
        }
    }

    [Test]
    public void TestPoolingForMultileConnStringsWithTopologyKeys()
    {
        var connStringBuilder1 = "host=127.0.0.1;database=yugabyte;userid=postgres;password=postgres;Load Balance Hosts=any;Topology Keys=cloud1.datacenter1.rack1:1;Timeout=0;";
        var connStringBuilder2 = "host=127.0.0.1;database=yugabyte;userid=tester;password=abc123;Load Balance Hosts=any;Topology Keys=cloud1.datacenter1.rack1:1;Timeout=0;";


        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        CreateCluster();

        try
        {
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

            NpgsqlCommand cmd1 = new NpgsqlCommand("SELECT current_user;", conn1);
            NpgsqlDataReader reader1 = cmd1.ExecuteReader();
            while (reader1.Read())
            {
                Assert.AreEqual(reader1.GetString(0), "postgres");
            }

            NpgsqlCommand cmd2 = new NpgsqlCommand("SELECT current_user;", conn2);
            NpgsqlDataReader reader2 = cmd2.ExecuteReader();
            while (reader2.Read())
            {
                Assert.AreEqual(reader2.GetString(0), "tester");
            }

            Console.WriteLine("Connections Created");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
            DestroyCluster();
        }
    }

    [Test]
    public void TestPoolingForMultileConnStringsMultiThread()
    {
        var connStringBuilder1 = "host=127.0.0.1;database=yugabyte;userid=postgres;password=postgres;Load Balance Hosts=any;Timeout=0;";
        var connStringBuilder2 = "host=127.0.0.1;database=yugabyte;userid=tester;password=abc123;Load Balance Hosts=any;Timeout=0;";

        List<NpgsqlConnection> conns1 = new List<NpgsqlConnection>();
        List<NpgsqlConnection> conns2 = new List<NpgsqlConnection>();

        CreateCluster();
        try
        {
            List<Thread> threads = new List<Thread>();
            List<NpgsqlConnection> conn = new List<NpgsqlConnection>();
            var numThreads = 5;
            for (var i = 0; i < numThreads; i++)
            {
                Thread thread = new Thread(() => {
                    var threadConns = CreateConnections(connStringBuilder1, 6); // Each thread uses its own list
                    lock (conns1)
                    {
                        conns1.AddRange(threadConns); // Safely add to the shared list
                    }
                });
                threads.Add(thread);
            }
            for (var i = 0; i < numThreads; i++)
            {
                Thread thread = new Thread(() => {
                    var threadConns = CreateConnections(connStringBuilder2, 6); // Each thread uses its own list
                    lock (conns2)
                    {
                        conns2.AddRange(threadConns); // Safely add to the shared list
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
            NpgsqlCommand cmd1 = new NpgsqlCommand("SELECT current_user;", conns1[0]);
            NpgsqlDataReader reader1 = cmd1.ExecuteReader();
            while (reader1.Read())
            {
                Assert.AreEqual(reader1.GetString(0), "postgres");
            }

            NpgsqlCommand cmd2 = new NpgsqlCommand("SELECT current_user;", conns2[0]);
            NpgsqlDataReader reader2 = cmd2.ExecuteReader();
            while (reader2.Read())
            {
                Assert.AreEqual(reader2.GetString(0), "tester");
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine("Failure:" + ex.Message);
            Console.WriteLine("Failure stacktrace: " + ex.StackTrace);
        }
        finally
        {
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
    void CreateCluster()
    {
        string? _Output = null;
        string? _Error = null;
        var cmd = "/bin/yb-ctl create --rf 3";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "bin/ysqlsh -c \"CREATE USER tester WITH PASSWORD 'abc123'\"";
        ExecuteShellCommand(cmd, ref _Output, ref _Error );
        Console.WriteLine("Output:" + _Output);
        cmd = "bin/ysqlsh -c \"GRANT ALL PRIVILEGES ON DATABASE \"yugabyte\" to tester;\"";
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
