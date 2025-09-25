using AdoNet.Specification.Tests;

namespace YBNpgsql.Specification.Tests;

public sealed class NpgsqlConnectionTests(NpgsqlDbFactoryFixture fixture) : ConnectionTestBase<NpgsqlDbFactoryFixture>(fixture);