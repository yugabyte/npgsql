using System;
using BenchmarkDotNet.Attributes;
using YBNpgsql.Internal.Converters;

namespace YBNpgsql.Benchmarks.TypeHandlers;

[Config(typeof(Config))]
public class Uuid : TypeHandlerBenchmarks<Guid>
{
    public Uuid() : base(new GuidUuidConverter()) { }
}
