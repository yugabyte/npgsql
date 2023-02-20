using AdoNet.Specification.Tests;

namespace YBNpgsql.Specification.Tests;

public sealed class NpgsqlConnectionTests : ConnectionTestBase<NpgsqlDbFactoryFixture>
{
    public NpgsqlConnectionTests(NpgsqlDbFactoryFixture fixture)
        : base(fixture)
    {
    }
}