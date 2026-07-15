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
        public void TestBuildCacheKeyGoldenHash()
        {
            // quoted segments are preserved verbatim by NormalizeIdentifier — pre-uppercase them to match
            // the canonical JSON: {"idp":"LOGIN...","role":"\"ANALYST ROLE WITH SPACES\":...","username":"\"FIRST LAST\"@..."}
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: "DPOP_BUNDLED_ACCESS_TOKEN",
                idp: "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
                snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
                username: "\"FIRST LAST\"@long-corporate-domain.example.com",
                role: "\"ANALYST ROLE WITH SPACES\":north_america:prod:readonly"
            ));
            Assert.Equal("SnowflakeTokenCache.v2.75ff2ad65a68afb402f125f62894697673c5ef3d863aba466d16b7a81053d1f4", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHasCorrectPrefix()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "host.snowflake.com", "host.snowflake.com", "user", ""));
            Assert.StartsWith("SnowflakeTokenCache.v2.", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHashIsLowercaseHex()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "host.snowflake.com", "host.snowflake.com", "user", ""));
            var hash = key.Substring("SnowflakeTokenCache.v2.".Length);
            Assert.Equal(64, hash.Length);
            Assert.Equal(hash, hash.ToLowerInvariant());
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenSnowflakeEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "idp", "", "user", "")));
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenUsernameEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "idp", "snowflake", "", "")));
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
        public void TestDimensionIsolationMfaEmptyRoleIsStable()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "host.snowflake.com", "host.snowflake.com", "user", ""));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MFA_TOKEN", "host.snowflake.com", "host.snowflake.com", "user", ""));
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
        }

        [SFFact]
        public void TestNormalizeUrlDropsQueryAndFragment()
        {
            Assert.Equal("HOST.COM/PATH", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path?q=1#frag"));
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
