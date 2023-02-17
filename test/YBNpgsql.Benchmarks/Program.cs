using BenchmarkDotNet.Running;
using System.Reflection;

namespace YBNpgsql.Benchmarks;

class Program
{
    static void Main(string[] args) => new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
}