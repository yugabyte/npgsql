using System;
using System.Threading.Tasks;
using YBNpgsqlTypes;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBPreparedStatementsTest
{
    [Test]

    public async Task PreparedStatementsTestWithFlagsEnabled()
    {
        var connStringBuilder = "host=localhost;port=5433;database=yugabyte;userid=yugabyte;password=yugabyte;Enable Discard Sequences=false;Enable Discard Temp= false;Enable Close All=false;Load Balance Hosts=true;";
        NpgsqlConnection conn = new NpgsqlConnection(connStringBuilder);
        try
        {
            conn.Open();
            NpgsqlCommand empCreateCmd = new NpgsqlCommand("CREATE TABLE employee (id int PRIMARY KEY,age int);", conn);
            empCreateCmd.ExecuteNonQuery();
            Console.WriteLine("Created table Employee");
            AssertNumPreparedStatements(conn, 0);

            using (NpgsqlCommand empInsertCommand = new NpgsqlCommand("Insert into employee values (@c1 ,@c2)", conn))
            {
                empInsertCommand.Parameters.Add("c1", NpgsqlDbType.Integer);
                empInsertCommand.Parameters.Add("c2", NpgsqlDbType.Integer);

                await empInsertCommand.PrepareAsync();

                empInsertCommand.Parameters[0].Value = 3;
                empInsertCommand.Parameters[1].Value = 5;

                await empInsertCommand.ExecuteNonQueryAsync();
            }

            conn.Close();
        }
        catch (PostgresException e)
        {
            Console.WriteLine(e);
        }
    }

    [Test]

    public void TypeLoadingTimeTest()
    {
        var connStringBuilderWithNoTypeLoading = "host=localhost;port=5433;database=northwind;userid=yugabyte;Enable Discard Sequences=false;Enable Discard Temp= false;Load Balance Hosts=true; Server Compatibility Mode=NoTypeLoading";
        var connStringBuilder = "host=localhost;port=5433;database=northwind;userid=yugabyte;Enable Discard Sequences=false;Enable Discard Temp= false;Load Balance Hosts=true;";

        DateTime timeBeforeConnection1 = DateTime.Now;
        NpgsqlConnection conn1 = new NpgsqlConnection(connStringBuilderWithNoTypeLoading);
        conn1.Open();
        DateTime timeAfterConnection1 = DateTime.Now;

        DateTime timeBeforeConnection2 = DateTime.Now;
        NpgsqlConnection conn2 = new NpgsqlConnection(connStringBuilder);
        conn2.Open();
        DateTime timeAfterConnection2 = DateTime.Now;

        Console.WriteLine("Time taken to create connection with no type loading:" + (timeAfterConnection1-timeBeforeConnection1).TotalSeconds);
        Console.WriteLine("Time taken to create connection with type loading:" + (timeAfterConnection2-timeBeforeConnection2).TotalSeconds);

    }

    void AssertNumPreparedStatements(NpgsqlConnection conn, int expected)
        => Assert.That(conn.ExecuteScalar("SELECT COUNT(*) FROM pg_prepared_statements WHERE statement NOT LIKE '%FROM pg_prepared_statements%'"), Is.EqualTo(expected));

}
