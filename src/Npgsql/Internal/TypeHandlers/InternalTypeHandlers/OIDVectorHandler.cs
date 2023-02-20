using System;
using YBNpgsql.Internal.TypeHandlers.NumericHandlers;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.PostgresTypes;
using YBNpgsql.TypeMapping;
using YBNpgsqlTypes;

namespace YBNpgsql.Internal.TypeHandlers.InternalTypeHandlers;

/// <summary>
/// An OIDVector is simply a regular array of uints, with the sole exception that its lower bound must
/// be 0 (we send 1 for regular arrays).
/// </summary>
sealed class OIDVectorHandler : ArrayHandler<uint>
{
    public OIDVectorHandler(PostgresType oidvectorType, PostgresType oidType)
        : base(oidvectorType, new UInt32Handler(oidType), ArrayNullabilityMode.Never, 0) { }

    public override NpgsqlTypeHandler CreateArrayHandler(PostgresArrayType pgArrayType, ArrayNullabilityMode arrayNullabilityMode)
        => new ArrayHandler<ArrayHandler<uint>>(pgArrayType, this, arrayNullabilityMode);
}