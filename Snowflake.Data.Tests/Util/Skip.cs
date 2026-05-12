using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
public static class Skip
{
    public static void If(bool condition, string rationale)
    {
        if (condition)
            throw SkipException.ForSkip(rationale);
    }
}
#endif
