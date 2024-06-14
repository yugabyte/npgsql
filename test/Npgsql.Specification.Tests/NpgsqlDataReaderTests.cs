using AdoNet.Specification.Tests;

namespace YBNpgsql.Specification.Tests;

public sealed class NpgsqlDataReaderTests(NpgsqlSelectValueFixture fixture) : DataReaderTestBase<NpgsqlSelectValueFixture>(fixture);