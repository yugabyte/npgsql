using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace Npgsql.Tests;

public class LoadBalancerTests
{
    [Test]
    public void TestLoadBalance()
        {
            var connStringBuilder = "host=127.0.0.2,127.0.0.1;port=5433;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";
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

                foreach (var conn1 in conns )
                {
                    Console.WriteLine("Connection host:{0}",conn1.Host );
                }
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
}
