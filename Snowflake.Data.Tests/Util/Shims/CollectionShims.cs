#if NETFRAMEWORK
using System.Collections.Generic;

namespace Snowflake.Data.Tests.Util.Shims;

internal static class CollectionShims
{
    public static bool TryDequeue<T>(this Queue<T> queue, out T element)
    {
        element = default(T);
        if (queue.Count == 0)
            return false;

        element = queue.Dequeue();
        return true;
    }
}
#endif
