using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

public static class Skip
{
    public static void When(bool condition, string rationale)
    {
        if (condition)
            throw SkipException.ForSkip(rationale);
    }
}
