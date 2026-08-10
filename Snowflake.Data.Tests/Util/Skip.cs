using System;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

public static class Skip
{
    public static void When(bool condition, string rationale)
    {
        if (condition)
            throw SkipException.ForSkip(rationale);
    }

    public static void WhenOnTfm(Tfm tfm, string rationale)
    {
        #if NETFRAMEWORK
        var skip = tfm.HasFlag(Tfm.Framework);
        When(skip, rationale);
        #endif
        #if NET6_0
        var skip = tfm.HasFlag(Tfm.Net6);
        When(skip, rationale);
        #endif
        #if NET7_0
        var skip = tfm.HasFlag(Tfm.Net7);
        When(skip, rationale);
        #endif
        #if NET8_0
        var skip = tfm.HasFlag(Tfm.Net8);
        When(skip, rationale);
        #endif
        #if NET9_0
        var skip = tfm.HasFlag(Tfm.Net9);
        When(skip, rationale);
        #endif
        #if NET10_0
        var skip = tfm.HasFlag(Tfm.Net10);
        When(skip, rationale);
        #endif
    }

    [Flags]
    public enum Tfm
    {
        Framework,
        Net6,
        Net7,
        Net8,
        Net9,
        Net10
    }
}
