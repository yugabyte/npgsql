using System;
using YBNpgsql.Internal;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.TypeMapping;

namespace YBNpgsql.GeoJSON.Internal;

public class GeoJSONTypeHandlerResolverFactory : TypeHandlerResolverFactory
{
    readonly GeoJSONOptions _options;
    readonly bool _geographyAsDefault;

    public GeoJSONTypeHandlerResolverFactory(GeoJSONOptions options, bool geographyAsDefault)
        => (_options, _geographyAsDefault) = (options, geographyAsDefault);

    public override TypeHandlerResolver Create(NpgsqlConnector connector)
        => new GeoJSONTypeHandlerResolver(connector, _options, _geographyAsDefault);

    public override string? GetDataTypeNameByClrType(Type type)
        => GeoJSONTypeHandlerResolver.ClrTypeToDataTypeName(type, _geographyAsDefault);

    public override TypeMappingInfo? GetMappingByDataTypeName(string dataTypeName)
        => GeoJSONTypeHandlerResolver.DoGetMappingByDataTypeName(dataTypeName);
}