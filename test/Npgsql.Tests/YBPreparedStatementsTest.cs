using System.Threading.Tasks;
using NUnit.Framework;

namespace YBNpgsql.Tests;

public class YBPreparedStatementsTest
{
    [Test]
    public async Task TestLoadBalance1()
    {
        var connStringBuilder = "host=127.0.0.1;database=yugabyte;userid=yugabyte;password=yugsbyte;Load Balance Hosts=true;Timeout=0";

        NpgsqlConnection connection = new NpgsqlConnection();
        connection.Open();

        // var createTable = new NpgsqlCommand("Create table test(c1 int primary key, c2 int)", connection);

        await using (var cmd = new NpgsqlCommand("", connection))
        {
            await cmd.PrepareAsync();    // First time on this physical connection, Npgsql prepares with PostgreSQL
            await cmd.ExecuteNonQueryAsync();
        }

    }
}
