using System;
using System.Threading;
using System.Threading.Tasks;
using NodaTime;
using YBNpgsql.BackendMessages;
using YBNpgsql.Internal;
using YBNpgsql.Internal.TypeHandlers;
using YBNpgsql.Internal.TypeHandling;
using YBNpgsql.NodaTime.Properties;
using YBNpgsql.PostgresTypes;
using YBNpgsqlTypes;
using static YBNpgsql.NodaTime.Internal.NodaTimeUtils;

namespace YBNpgsql.NodaTime.Internal;

public partial class DateRangeHandler : RangeHandler<LocalDate>, INpgsqlTypeHandler<DateInterval>
#if NET6_0_OR_GREATER
    , INpgsqlTypeHandler<NpgsqlRange<DateOnly>>
#endif
{
    public DateRangeHandler(PostgresType rangePostgresType, NpgsqlTypeHandler subtypeHandler)
        : base(rangePostgresType, subtypeHandler)
    {
    }

    public override Type GetFieldType(FieldDescription? fieldDescription = null) => typeof(DateInterval);
    public override Type GetProviderSpecificFieldType(FieldDescription? fieldDescription = null) => typeof(DateInterval);

    public override async ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async,
        FieldDescription? fieldDescription = null)
        => (await Read<DateInterval>(buf, len, async, fieldDescription))!;

    async ValueTask<DateInterval> INpgsqlTypeHandler<DateInterval>.Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription)
    {
        var range = await Read(buf, len, async, fieldDescription);

        var upperBound = range.UpperBound;

        if (DisableDateTimeInfinityConversions || upperBound != LocalDate.MaxIsoValue)
            upperBound -= Period.FromDays(1);

        return new(range.LowerBound, upperBound);
    }

    public int ValidateAndGetLength(DateInterval value, ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
        => ValidateAndGetLengthRange(new NpgsqlRange<LocalDate>(value.Start, value.End), ref lengthCache, parameter);

    public Task Write(
        DateInterval value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter, bool async,
        CancellationToken cancellationToken = default)
        => WriteRange(new NpgsqlRange<LocalDate>(value.Start, value.End), buf, lengthCache, parameter, async, cancellationToken);

#if NET6_0_OR_GREATER
    ValueTask<NpgsqlRange<DateOnly>> INpgsqlTypeHandler<NpgsqlRange<DateOnly>>.Read(
        NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription)
        => ReadRange<DateOnly>(buf, len, async, fieldDescription);

    public int ValidateAndGetLength(NpgsqlRange<DateOnly> value, ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
        => ValidateAndGetLengthRange(value, ref lengthCache, parameter);

    public Task Write(
        NpgsqlRange<DateOnly> value,
        NpgsqlWriteBuffer buf,
        NpgsqlLengthCache? lengthCache,
        NpgsqlParameter? parameter,
        bool async,
        CancellationToken cancellationToken = default)
        => WriteRange(value, buf, lengthCache, parameter, async, cancellationToken);
#endif
}