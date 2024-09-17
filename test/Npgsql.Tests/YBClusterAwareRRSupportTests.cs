using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBClusterAwareRRSupportTests : YBTestUtils{
    int numConns = 6;
    [Test]
    public async Task TestOnlyPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyprimary;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

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
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferPrimary()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferprimary;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

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
            // DestroyCluster();
        }
    }
    [Test]
    public async Task TestPreferPrimaryAllNodesDown()
    {
        var connStringBuilder = "host=127.0.0.4;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node: 127.0.0.1, 127.0.0.2, 127.0.0.3

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
            // DestroyCluster();
        }
    }
    [Test]
    public async Task TestOnlyRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=onlyrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

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
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferRR()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

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
            // DestroyCluster();
        }
    }

    [Test]
    public async Task TestPreferRRAllNodesDown()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=preferrr;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();
        // Stop node : 127.0.0.4, 127.0.0.5, 127.0.0.6

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
            // DestroyCluster();
        }
    }
    [Test]
    public async Task TestAny()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugabyte;Load Balance Hosts=any;Timeout=0";

        List<NpgsqlConnection> conns = new List<NpgsqlConnection>();
        // CreateCluster();

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
