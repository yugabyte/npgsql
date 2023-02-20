using System;
using YBNpgsql.TypeMapping;
using YBNpgsqlTypes;
using Newtonsoft.Json;
using YBNpgsql.Json.NET.Internal;

// ReSharper disable once CheckNamespace
namespace YBNpgsql;

/// <summary>
/// Extension allowing adding the Json.NET plugin to an Npgsql type mapper.
/// </summary>
public static class NpgsqlJsonNetExtensions
{
    /// <summary>
    /// Sets up JSON.NET mappings for the PostgreSQL json and jsonb types.
    /// </summary>
    /// <param name="mapper">The type mapper to set up.</param>
    /// <param name="settings">Optional settings to customize JSON serialization.</param>
    /// <param name="jsonbClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>jsonb</c> (no need to specify <see cref="NpgsqlDbType.Jsonb" />).
    /// </param>
    /// <param name="jsonClrTypes">
    /// A list of CLR types to map to PostgreSQL <c>json</c> (no need to specify <see cref="NpgsqlDbType.Json" />).
    /// </param>
    public static INpgsqlTypeMapper UseJsonNet(
        this INpgsqlTypeMapper mapper,
        JsonSerializerSettings? settings = null,
        Type[]? jsonbClrTypes = null,
        Type[]? jsonClrTypes = null)
    {
        mapper.AddTypeResolverFactory(new JsonNetTypeHandlerResolverFactory(jsonbClrTypes, jsonClrTypes, settings));
        return mapper;
    }
}