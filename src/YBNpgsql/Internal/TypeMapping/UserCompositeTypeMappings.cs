using System;
using YBNpgsql.Internal.TypeHandlers.CompositeHandlers;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.PostgresTypes;

namespace YBNpgsql.Internal.TypeMapping;

public interface IUserCompositeTypeMapping : IUserTypeMapping
{
    INpgsqlNameTranslator NameTranslator { get; }
}

sealed class UserCompositeTypeMapping<T> : IUserCompositeTypeMapping
{
    public string PgTypeName { get; }
    public Type ClrType => typeof(T);
    public INpgsqlNameTranslator NameTranslator { get; }

    public UserCompositeTypeMapping(string pgTypeName, INpgsqlNameTranslator nameTranslator)
        => (PgTypeName, NameTranslator) = (pgTypeName, nameTranslator);

    public NpgsqlTypeHandler CreateHandler(PostgresType pgType, NpgsqlConnector connector)
        => new CompositeHandler<T>((PostgresCompositeType)pgType, connector.TypeMapper, NameTranslator);
}