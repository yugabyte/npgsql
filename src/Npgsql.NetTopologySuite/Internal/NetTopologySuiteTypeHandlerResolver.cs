using System;
using System.Data;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using YBNpgsql.Internal;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.PostgresTypes;
using YBNpgsql.TypeMapping;
using YBNpgsqlTypes;

namespace YBNpgsql.NetTopologySuite.Internal;

public class NetTopologySuiteTypeHandlerResolver : TypeHandlerResolver
{
    readonly NpgsqlDatabaseInfo _databaseInfo;
    readonly bool _geographyAsDefault;

    readonly NetTopologySuiteHandler? _geometryHandler, _geographyHandler;

    internal NetTopologySuiteTypeHandlerResolver(
        NpgsqlConnector connector,
        CoordinateSequenceFactory coordinateSequenceFactory,
        PrecisionModel precisionModel,
        Ordinates handleOrdinates,
        bool geographyAsDefault)
    {
        _databaseInfo = connector.DatabaseInfo;
        _geographyAsDefault = geographyAsDefault;

        var (pgGeometryType, pgGeographyType) = (PgType("geometry"), PgType("geography"));

        var reader = new PostGisReader(coordinateSequenceFactory, precisionModel, handleOrdinates);
        var writer = new PostGisWriter();

        if (pgGeometryType is not null)
            _geometryHandler = new NetTopologySuiteHandler(pgGeometryType, reader, writer);
        if (pgGeographyType is not null)
            _geographyHandler = new NetTopologySuiteHandler(pgGeographyType, reader, writer);
    }

    public override NpgsqlTypeHandler? ResolveByDataTypeName(string typeName)
        => typeName switch
        {
            "geometry" => _geometryHandler,
            "geography" => _geographyHandler,
            _ => null
        };

    public override NpgsqlTypeHandler? ResolveByClrType(Type type)
        => ClrTypeToDataTypeName(type, _geographyAsDefault) is { } dataTypeName && ResolveByDataTypeName(dataTypeName) is { } handler
            ? handler
            : null;

    internal static string? ClrTypeToDataTypeName(Type type, bool geographyAsDefault)
        => type != typeof(Geometry) && type.BaseType != typeof(Geometry) && type.BaseType != typeof(GeometryCollection)
            ? null
            : geographyAsDefault
                ? "geography"
                : "geometry";

    public override TypeMappingInfo? GetMappingByDataTypeName(string dataTypeName)
        => DoGetMappingByDataTypeName(dataTypeName);

    internal static TypeMappingInfo? DoGetMappingByDataTypeName(string dataTypeName)
        => dataTypeName switch
        {
            "geometry"  => new(NpgsqlDbType.Geometry,  "geometry"),
            "geography" => new(NpgsqlDbType.Geography, "geography"),
            _ => null
        };

    PostgresType? PgType(string pgTypeName) => _databaseInfo.TryGetPostgresTypeByName(pgTypeName, out var pgType) ? pgType : null;
}