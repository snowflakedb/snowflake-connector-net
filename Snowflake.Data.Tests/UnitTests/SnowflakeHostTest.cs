using Moq;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SnowflakeHostTest
{
    // Vectors 1-3: apex accepted for each allowed suffix
    [InlineData("snowflakecomputing.com", true)]
    [InlineData("snowflakecomputing.cn", true)]
    [InlineData("snowflakecomputing.mil", true)]
    // Vectors 4-6: subdomain accepted for each allowed suffix
    [InlineData("myaccount.snowflakecomputing.com", true)]
    [InlineData("myaccount.snowflakecomputing.cn", true)]
    [InlineData("myaccount.snowflakecomputing.mil", true)]
    // Vector 7: multi-level subdomain accepted
    [InlineData("myaccount.us-west-2.snowflakecomputing.com", true)]
    // Vector 8: case-insensitivity
    [InlineData("MyAccount.SNOWFLAKECOMPUTING.COM", true)]
    // Vector 9: leading/trailing whitespace trimmed
    [InlineData("  myaccount.snowflakecomputing.com  ", true)]
    // Vector 10: trailing FQDN dots stripped
    [InlineData("myaccount.snowflakecomputing.com.", true)]
    // Vector 11: port suffix stripped before matching
    [InlineData("myaccount.snowflakecomputing.com:443", true)]
    // Vector 12: apex with port
    [InlineData("snowflakecomputing.com:8080", true)]
    // Vector 13: empty host rejected
    [InlineData("", false)]
    // Vector 14: whitespace-only host rejected
    [InlineData("   ", false)]
    // Vector 15: unrelated host rejected
    [InlineData("evil.com", false)]
    // Vector 16: suffix containment without label boundary must be rejected (not "ends with", just "contains")
    [InlineData("notsnowflakecomputing.com", false)]
    // Vector 17: the listed suffix appearing as a leading label of another domain must be rejected
    [InlineData("snowflakecomputing.com.evil.com", false)]
    // Vector 18: a host embedding the listed suffix as a label of a different domain must be rejected
    [InlineData("snowflakecomputing.com.attacker.net", false)]
    // Vector 19: bare TLD-like suffix without the "snowflakecomputing" label must be rejected
    [InlineData("com", false)]
    // Vector 20: IP literal rejected implicitly (no special-case handling)
    [InlineData("192.168.1.1", false)]
    // Vector 21: IPv6 literal in bracket+port form. Normalization truncates at the first ':',
    // which for "[::1]:443" leaves "[" - matching no suffix, so it is rejected either way.
    [InlineData("[::1]:443", false)]
    // Vector 21b: bare IPv6 literal, likewise rejected
    [InlineData("::1", false)]
    // Vector 22: localhost rejected by default (no allowlist entry, no env override)
    [InlineData("localhost", false)]
    // Vector 23: FQDN trailing dot combined with an explicit port is accepted - the port is
    // stripped before the trailing dot, so "acct.snowflakecomputing.com.:443" still matches.
    [InlineData("acct.snowflakecomputing.com.:443", true)]
    // Vector 11 (spec): extra labels after the suffix must be rejected
    [InlineData("acct.snowflakecomputing.com.attacker.example", false)]
    // Vector 12 (spec): the listed suffix appears in the host but not as a trailing label
    [InlineData("evil.snowflakecomputing.attacker.example", false)]
    // Vector 13 (spec): TLD not in the allowlist
    [InlineData("acct.snowflakecomputing.zip", false)]
    // Vector 14 (spec): unrelated host
    [InlineData("attacker.example", false)]
    // Vector 15 (spec): the listed suffix embedded as a leading label of a different domain
    [InlineData("snowflakecomputing.com.evil.io", false)]
    // Vector 18 (spec): near-miss host missing the label separator before "snowflakecomputing"
    [InlineData("xsnowflakecomputing.mil", false)]
    // Vector 19 (spec): near-miss TLD (.co instead of .com)
    [InlineData("acct.snowflakecomputing.co", false)]
    [SFTheory]
    public void TestIsSnowflakeHostForWorkloadIdentity_CanonicalVectors(string host, bool expected)
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns(string.Empty);

        var actual = SnowflakeHost.IsSnowflakeHostForWorkloadIdentity(host, environmentFacade.Object);

        Assert.Equal(expected, actual);
    }

    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_EscapeHatchAddsSuffixAdditively()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns("localhost, internal.example.com");

        // the extra suffix is now accepted
        Assert.True(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("localhost", environmentFacade.Object));
        Assert.True(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("wif-metadata.internal.example.com", environmentFacade.Object));

        // the built-in suffixes are still accepted (additive, not replacing)
        Assert.True(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("myaccount.snowflakecomputing.com", environmentFacade.Object));

        // unrelated hosts are still rejected
        Assert.False(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("evil.com", environmentFacade.Object));
    }

    // Vector 20 (spec): wiremock.local rejected with the env var unset
    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_EscapeHatchUnset_WiremockLocalRejected()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns(string.Empty);

        Assert.False(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("wiremock.local", environmentFacade.Object));
    }

    // Vector 21 (spec): wiremock.local accepted once explicitly allow-listed via the env var
    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_EscapeHatchSet_WiremockLocalAccepted()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns("wiremock.local");

        Assert.True(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("wiremock.local", environmentFacade.Object));
    }

    // Vector 22 (spec): an unrelated host is still rejected even when the env var is set to something else
    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_EscapeHatchSet_UnrelatedHostStillRejected()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns("wiremock.local");

        Assert.False(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("attacker.example", environmentFacade.Object));
    }

    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_EscapeHatchIgnoresEmptyEntries()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns(",, localhost ,,");

        Assert.True(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("localhost", environmentFacade.Object));
        Assert.False(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity("evil.com", environmentFacade.Object));
    }

    [SFFact]
    public void TestIsSnowflakeHostForWorkloadIdentity_NullHostRejected()
    {
        var environmentFacade = new Mock<IEnvironmentFacade>();
        environmentFacade.Setup(e => e.GetString(EnvVars.WifAllowedHostSuffixes)).Returns(string.Empty);

        Assert.False(SnowflakeHost.IsSnowflakeHostForWorkloadIdentity(null, environmentFacade.Object));
    }

    [SFFact]
    public void TestIsSnowflakeHost_LegacyBehaviorUnchanged()
    {
        // IsSnowflakeHost is a distinct, pre-existing helper (used for OAuth "is Snowflake the IdP"
        // checks) and must not be affected by the new WIF-specific helper. It does not accept the
        // bare apex, unlike IsSnowflakeHostForWorkloadIdentity.
        Assert.False(SnowflakeHost.IsSnowflakeHost("snowflakecomputing.com"));
        Assert.True(SnowflakeHost.IsSnowflakeHost("myaccount.snowflakecomputing.com"));
        Assert.True(SnowflakeHost.IsSnowflakeHost("myaccount.snowflakecomputing.cn"));
        Assert.False(SnowflakeHost.IsSnowflakeHost("myaccount.snowflakecomputing.mil"));
        Assert.False(SnowflakeHost.IsSnowflakeHost("evil.com"));
    }
}
