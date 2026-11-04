using System;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class HeartBeatBackgroundTest
{
    // Default behavior (no user frequency) — should be master/4

    [SFFact]
    public void TestDefaultIntervalIsMasterOver4()
    {
        // master=14400s → 14400/4 = 3600s
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    [SFFact]
    public void TestDefaultIntervalFromZeroMasterUsesDefaultMasterValidity()
    {
        // Zero master falls back to default 14400s → 14400/4 = 3600s
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.Zero, TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    [SFFact]
    public void TestDefaultIntervalCappedAtOneHour()
    {
        // master=100000s → 100000/4 = 25000s, capped at 3600s
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(100000), TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromHours(1), interval);
    }

    // User frequency within valid range

    [SFFact]
    public void TestUserFrequencyWithinRangeIsUsed()
    {
        // master=14400, range=[900, 3600], user=2000 → stays 2000
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(2000));
        Assert.Equal(TimeSpan.FromSeconds(2000), interval);
    }

    [SFFact]
    public void TestUserFrequencyAtMinBoundaryIsAccepted()
    {
        // Exact boundary: 900 == 14400/16 → accepted
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(900));
        Assert.Equal(TimeSpan.FromSeconds(900), interval);
    }

    [SFFact]
    public void TestUserFrequencyAtMaxBoundaryIsAccepted()
    {
        // Exact boundary: 3600 == 14400/4 → accepted
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(3600));
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    // Clamping — below min

    [SFFact]
    public void TestUserFrequencyBelowMinClampedUp()
    {
        // master=14400, min=900, user=100 → clamped up to 900
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(100));
        Assert.Equal(TimeSpan.FromSeconds(900), interval);
    }

    // Clamping — above max

    [SFFact]
    public void TestUserFrequencyAboveMaxClampedDown()
    {
        // master=14400, max=3600, user=9000 → clamped down to 3600
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(9000));
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    // Hard cap at 1 hour

    [SFFact]
    public void TestUserFrequencyExceedingOneHourCapped()
    {
        // master=100000, max=25000 capped to 3600, user=7200 → capped to 3600
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(100000), TimeSpan.FromSeconds(7200));
        Assert.Equal(TimeSpan.FromHours(1), interval);
    }

    // Negative inputs

    [SFFact]
    public void TestNegativeUserFrequencyTreatedAsDefault()
    {
        // Negative user frequency → falls back to master/4
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(14400), TimeSpan.FromSeconds(-500));
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    [SFFact]
    public void TestNegativeMasterFallsBackToDefault()
    {
        // Negative master → falls back to default master (14400s) → 14400/4 = 3600s
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromSeconds(-1000), TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(3600), interval);
    }

    // Large master token validity

    [SFFact]
    public void TestVeryLargeMasterWithUserFrequencyStillCappedAtOneHour()
    {
        // master=24h, min=1.5h, max=6h capped to 1h, user=2h → capped to 1h
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromHours(24), TimeSpan.FromHours(2));
        Assert.Equal(TimeSpan.FromHours(1), interval);
    }

    [SFFact]
    public void TestVeryLargeMasterWithSmallUserFrequencyClampedToMin()
    {
        // master=24h, min=1.5h, user=30min → clamped up to 1.5h, but 1.5h > 1h cap → 1.5h
        // Wait — the cap is only on maxBound, minBound is not capped. So this returns 1.5h.
        // Actually let me trace: maxBound = 24h/4 = 6h → capped to 1h. minBound = 24h/16 = 1.5h.
        // user=30min < minBound=1.5h → result=1.5h. result > maxBound=1h? No, because we already clamped
        // to minBound and minBound > maxBound here. The clamp logic: if < min → min, if > max → max.
        // Since min > max in this case, if user < min, result = min = 1.5h.
        // Then the > max check: 1.5h > 1h → result = 1h. ✓
        var interval = HeartBeatBackground.ComputeHeartBeatInterval(TimeSpan.FromHours(24), TimeSpan.FromMinutes(30));
        Assert.Equal(TimeSpan.FromHours(1), interval);
    }
}
