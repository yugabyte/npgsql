using System;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.PostgresTypes;

namespace YBNpgsql.Internal.TypeMapping;

public interface IUserTypeMapping
{
    public string PgTypeName { get; }
    public Type ClrType { get; }

    public NpgsqlTypeHandler CreateHandler(PostgresType pgType, NpgsqlConnector connector);
}