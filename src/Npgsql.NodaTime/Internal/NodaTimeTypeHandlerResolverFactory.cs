using System;
using YBNpgsql.Internal;
using YBNpgsql.Internal.TypeHandling;

namespace YBNpgsql.NodaTime.Internal;

public class NodaTimeTypeHandlerResolverFactory : TypeHandlerResolverFactory
{
    public override TypeHandlerResolver Create(NpgsqlConnector connector)
        => new NodaTimeTypeHandlerResolver(connector);

    public override string? GetDataTypeNameByClrType(Type type)
        => NodaTimeTypeHandlerResolver.ClrTypeToDataTypeName(type);

    public override TypeMappingInfo? GetMappingByDataTypeName(string dataTypeName)
        => NodaTimeTypeHandlerResolver.DoGetMappingByDataTypeName(dataTypeName);
}