using System;
using YBNpgsql.Internal.TypeHandlers.NumericHandlers;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.PostgresTypes;
using YBNpgsql.TypeMapping;
using YBNpgsqlTypes;

namespace YBNpgsql.Internal.TypeHandlers.InternalTypeHandlers;

/// <summary>
/// An int2vector is simply a regular array of shorts, with the sole exception that its lower bound must
/// be 0 (we send 1 for regular arrays).
/// </summary>
sealed class Int2VectorHandler : ArrayHandler<short>
{
    public Int2VectorHandler(PostgresType arrayPostgresType, PostgresType postgresShortType)
        : base(arrayPostgresType, new Int16Handler(postgresShortType), ArrayNullabilityMode.Never, 0) { }

    public override NpgsqlTypeHandler CreateArrayHandler(PostgresArrayType pgArrayType, ArrayNullabilityMode arrayNullabilityMode)
        => new ArrayHandler<ArrayHandler<short>>(pgArrayType, this, arrayNullabilityMode);
}