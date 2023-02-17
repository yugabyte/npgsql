
// Only used for value types, but can't constrain because MappedCompositeHandler isn't constrained
#nullable disable

namespace YBNpgsql.Internal.TypeHandlers.CompositeHandlers;

sealed class ByReference<T>
{
    public T Value;
}