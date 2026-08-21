using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests.Util.Shims;

#if !NET8_0_OR_GREATER
internal static class CancellationTokenShims
{
    public static Task CancelAsync(this CancellationTokenSource cts)
    {
        cts.Cancel();
        return Task.CompletedTask;
    }
}
#endif
