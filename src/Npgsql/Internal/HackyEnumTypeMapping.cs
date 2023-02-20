using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using YBNpgsql.Internal;
using YBNpgsql.PostgresTypes;
using YBNpgsqlTypes;

namespace YBNpgsql.Internal;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

/// <summary>
/// Hacky temporary measure used by EFCore.PG to extract user-configured enum mappings. Accessed via reflection only.
/// </summary>
[Experimental(NpgsqlDiagnostics.ConvertersExperimental)]
public sealed class HackyEnumTypeMapping
{
    public HackyEnumTypeMapping(Type enumClrType, string pgTypeName, INpgsqlNameTranslator nameTranslator)
    {
        EnumClrType = enumClrType;
        PgTypeName = pgTypeName;
        NameTranslator = nameTranslator;
    }

    public string PgTypeName { get; }
    public Type EnumClrType { get; }
    public INpgsqlNameTranslator NameTranslator { get; }
}
