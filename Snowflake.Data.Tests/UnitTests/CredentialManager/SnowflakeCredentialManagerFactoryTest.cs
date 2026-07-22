using System;
using System.Runtime.InteropServices;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.CredentialManager
{
    public class SnowflakeCredentialManagerFactoryTest : IDisposable
    {
        [SFFact]
        public void TestBuildCacheKeyGoldenHashA_OAuth()
        {
            // Vector A: OAuth flow — 4-field keyData, mixed-case quoted segments preserved verbatim.
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: "DPOP_BUNDLED_ACCESS_TOKEN",
                idp: "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
                snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
                username: "\"First Last\"@long-corporate-domain.example.com",
                role: "\"Analyst Role With Spaces\":north_america:prod:readonly"
            ));
            Assert.Equal("SnowflakeTokenCache.v2.DPOP_BUNDLED_ACCESS_TOKEN.be782aa7c9abf8698adc9e6de61b954ccec7d9202899b44c2eb4e1dfa4313d5f", key);
        }

        [SFFact]
        public void TestBuildCacheKeyGoldenHashB_Mfa()
        {
            // Vector B: MFA flow — 2-field keyData (snowflake + username only), mixed-case quoted segment preserved.
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: "MFA_TOKEN",
                idp: "",
                snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
                username: "\"First Last\"@long-corporate-domain.example.com",
                role: ""
            ));
            Assert.Equal("SnowflakeTokenCache.v2.MFA_TOKEN.a508fa2858a6e22e9fdbc90b4149a3ff666d1acbb286c85ff179499ac92d75c8", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHasCorrectPrefix()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "host.snowflake.com", "user", ""));
            Assert.StartsWith("SnowflakeTokenCache.v2.MFA_TOKEN.", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHashIsLowercaseHex()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "host.snowflake.com", "user", ""));
            var segments = key.Split('.');
            // Format: SnowflakeTokenCache.v2.<TOKEN_TYPE>.<hash>
            Assert.Equal(4, segments.Length);
            var hash = segments[3];
            Assert.Equal(64, hash.Length);
            Assert.Equal(hash, hash.ToLowerInvariant());
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenSnowflakeEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "", "user", "")));
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenUsernameEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "snowflake", "", "")));
        }

        [SFFact]
        public void TestDimensionIsolationMfaVsOAuth()
        {
            // MFA and OAuth for the same user/host must produce different keys (different prefix + field set).
            var mfaKey = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                "MFA_TOKEN", "", "acct.snowflakecomputing.com", "user", ""));
            var oauthKey = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                "OAUTH_ACCESS_TOKEN", "idp.example.com", "acct.snowflakecomputing.com", "user", ""));
            Assert.NotEqual(mfaKey, oauthKey);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentSnowflakeHost()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "idp.example.com", "acct1.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "idp.example.com", "acct2.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentRole()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "idp.example.com", "acct.snowflakecomputing.com", "user", "ANALYST"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "idp.example.com", "acct.snowflakecomputing.com", "user", "ENGINEER"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentIdp()
        {
            // Same Snowflake account and user, different IdP — the multi-account/shared-IdP collision case.
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "login.microsoftonline.com/tenantA", "acct.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "login.microsoftonline.com/tenantB", "acct.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestBuildCacheKeyIsDeterministic()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "host.snowflake.com", "user", ""));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "", "host.snowflake.com", "user", ""));
            Assert.Equal(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentTokenType()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_ACCESS_TOKEN", "idp.example.com", "acct.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OAUTH_REFRESH_TOKEN", "idp.example.com", "acct.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestNormalizeUrlStripsScheme()
        {
            Assert.Equal("EXAMPLE.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://example.com/path"));
            Assert.Equal("EXAMPLE.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("http://example.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlStripsUserinfo()
        {
            Assert.Equal("HOST.COM", SnowflakeCredentialManagerFactory.NormalizeUrl("https://user:pass@host.com"));
            Assert.Equal("HOST.EXAMPLE.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://user:pass@host.example.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlPreservesAtSignInPath()
        {
            // An '@' after the authority is part of the path and must survive; only userinfo is stripped.
            Assert.Equal("HOST.EXAMPLE.COM/OAUTH/@HANDLE/TOKEN",
                SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.example.com/oauth/@handle/token"));
        }

        [SFFact]
        public void TestNormalizeUrlDropsQueryAndFragment()
        {
            Assert.Equal("HOST.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path?q=1#frag"));
            // An '@' inside the dropped query string must not be mistaken for userinfo.
            Assert.Equal("HOST.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path?email=user@domain.com"));
        }

        [SFFact]
        public void TestNormalizeUrlTrimsRootOnlyTrailingSlash()
        {
            Assert.Equal("HOST.COM", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/"));
            Assert.Equal("HOST.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlPreservesPortAndPath()
        {
            Assert.Equal("LOGIN.MICROSOFTONLINE.COM:443/TENANT-ID/OAUTH2/V2.0",
                SnowflakeCredentialManagerFactory.NormalizeUrl("https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0"));
        }

        [SFFact]
        public void TestNormalizeIdentifierUnquotedIsUppercased()
        {
            Assert.Equal("USER@DOMAIN.COM", SnowflakeCredentialManagerFactory.NormalizeIdentifier("user@domain.com"));
        }

        [SFFact]
        public void TestNormalizeIdentifierQuotedSegmentPreservedVerbatim()
        {
            Assert.Equal("\"First Last\"@LONG-CORPORATE-DOMAIN.EXAMPLE.COM",
                SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"First Last\"@long-corporate-domain.example.com"));
        }

        [SFFact]
        public void TestNormalizeIdentifierMixedQuotedAndUnquoted()
        {
            Assert.Equal("\"Analyst Role With Spaces\":NORTH_AMERICA:PROD:READONLY",
                SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"Analyst Role With Spaces\":north_america:prod:readonly"));
        }

        [SFFact]
        public void TestNormalizeIdentifierEmptyReturnsEmpty()
        {
            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.NormalizeIdentifier(string.Empty));
        }

        [SFFact]
        public void TestNormalizeIdentifierUppercasesAsciiOnly()
        {
            // Matches the reference's ASCII-only case-folding: non-ASCII letters are left unchanged
            // so keys stay byte-identical across drivers.
            Assert.Equal("CAF\u00e9", SnowflakeCredentialManagerFactory.NormalizeIdentifier("caf\u00e9"));
            Assert.Equal("STRA\u00dfE", SnowflakeCredentialManagerFactory.NormalizeIdentifier("stra\u00dfe"));
        }

        [SFFact]
        public void TestNormalizeIdentifierPreservesSqlEscapedQuotes()
        {
            Assert.Equal("\"Foo\"\"Bar\":BAZ", SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"Foo\"\"Bar\":baz"));
        }


        private void TearDown()
        {
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
        }

        [SFFact]
        public void TestUsingDefaultCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.IsType<SFCredentialManagerWindowsNativeImpl>(credentialManager);
            }
            else
            {
                Assert.IsType<SFCredentialManagerFileImpl>(credentialManager);
            }
        }

        [SFFact]
        public void TestSettingCustomCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.SetCredentialManager(SFCredentialManagerInMemoryImpl.Instance);

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsType<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestUseMemoryImplCredentialManager()
        {
            // arrange
            SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();

            // act
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();

            // assert
            Assert.IsType<SFCredentialManagerInMemoryImpl>(credentialManager);
        }

        [SFFact]
        public void TestThatThrowsErrorWhenTryingToSetCredentialManagerToNull()
        {
            // act and assert
            var exception = Assert.Throws<SnowflakeDbException>(() => SnowflakeCredentialManagerFactory.SetCredentialManager(null));
            Assert.Contains("Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method.", exception.Message);
        }

        [SFFact(SkipCondition.SkipOnWindows)]
        public void TestUseWindowsCredentialManagerFailsOnUnix()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseWindowsCredentialManager);

            // assert
            Assert.Equal("Windows native credential manager implementation can be used only on Windows", thrown.Message);
        }

        [SFFact(SkipCondition.RunOnlyOnWindows)]
        public void TestUseFileCredentialManagerFailsOnWindows()
        {
            // act
            var thrown = Assert.Throws<Exception>(SnowflakeCredentialManagerFactory.UseFileCredentialManager);

            // assert
            Assert.Equal("File credential manager implementation is not supported on Windows", thrown.Message);
        }

        public void Dispose()
        {
            TearDown();
        }
    }
}
