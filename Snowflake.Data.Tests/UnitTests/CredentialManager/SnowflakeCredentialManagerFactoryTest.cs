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
            // Vector A: OAuth flow — 4-field keyData, lowercase normalization, quoted values verbatim.
            // "DpopBundledAccessToken" passed directly — no named constant for this type.
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: "DpopBundledAccessToken",
                idp: "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
                snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
                username: "\"First Last\"@long-corporate-domain.example.com",
                role: "\"Analyst Role With Spaces\":north_america:prod:readonly"
            ));
            Assert.Equal("SnowflakeTokenCache.v2.DpopBundledAccessToken.741b6d66d252666d6821bfd19e0151511cf4efdaaeba2b3c87673aa4de6d2c0b", key);
        }

        [SFFact]
        public void TestBuildCacheKeyGoldenHashB_Mfa()
        {
            // Vector B: MFA flow — 2-field keyData, lowercase normalization, quoted username verbatim.
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                tokenType: "MfaToken",
                idp: "",
                snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
                username: "\"First Last\"@long-corporate-domain.example.com",
                role: ""
            ));
            Assert.Equal("SnowflakeTokenCache.v2.MfaToken.10c5dde84bb8f584c0df06ea826d418c4f580e08f9db10187c0cb5e2a732a0d6", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHasCorrectPrefix()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "host.snowflake.com", "user", ""));
            Assert.StartsWith("SnowflakeTokenCache.v2.MfaToken.", key);
        }

        [SFFact]
        public void TestBuildCacheKeyHashIsLowercaseHex()
        {
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "host.snowflake.com", "user", ""));
            var segments = key.Split('.');
            // Format: SnowflakeTokenCache.v2.<TokenType>.<hash>
            Assert.Equal(4, segments.Length);
            var hash = segments[3];
            Assert.Equal(64, hash.Length);
            Assert.Equal(hash, hash.ToLowerInvariant());
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenSnowflakeEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "", "user", "")));
        }

        [SFFact]
        public void TestBuildCacheKeyThrowsWhenUsernameEmpty()
        {
            Assert.Throws<ArgumentException>(() => SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "snowflake", "", "")));
        }

        [SFFact]
        public void TestDimensionIsolationMfaVsOAuth()
        {
            // MFA and OAuth for the same user/host must produce different keys (different prefix + field set).
            var mfaKey = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                "MfaToken", "", "acct.snowflakecomputing.com", "user", ""));
            var oauthKey = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(
                "OauthAccessToken", "idp.example.com", "acct.snowflakecomputing.com", "user", ""));
            Assert.NotEqual(mfaKey, oauthKey);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentSnowflakeHost()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "idp.example.com", "acct1.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "idp.example.com", "acct2.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentRole()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "idp.example.com", "acct.snowflakecomputing.com", "user", "analyst"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "idp.example.com", "acct.snowflakecomputing.com", "user", "engineer"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentIdp()
        {
            // Same Snowflake account and user, different IdP — the multi-account/shared-IdP collision case.
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "login.microsoftonline.com/tenantA", "acct.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "login.microsoftonline.com/tenantB", "acct.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestBuildCacheKeyIsDeterministic()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "host.snowflake.com", "user", ""));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("MfaToken", "", "host.snowflake.com", "user", ""));
            Assert.Equal(key1, key2);
        }

        [SFFact]
        public void TestDimensionIsolationDifferentTokenType()
        {
            var key1 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthAccessToken", "idp.example.com", "acct.snowflakecomputing.com", "user", "role"));
            var key2 = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput("OauthRefreshToken", "idp.example.com", "acct.snowflakecomputing.com", "user", "role"));
            Assert.NotEqual(key1, key2);
        }

        [SFFact]
        public void TestNormalizeUrlStripsScheme()
        {
            Assert.Equal("example.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("https://example.com/path"));
            Assert.Equal("example.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("http://example.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlStripsUserinfo()
        {
            Assert.Equal("host.com", SnowflakeCredentialManagerFactory.NormalizeUrl("https://user:pass@host.com"));
            Assert.Equal("host.example.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("https://user:pass@host.example.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlPreservesAtSignInPath()
        {
            // An '@' after the authority is part of the path and must survive; only userinfo is stripped.
            Assert.Equal("host.example.com/oauth/@handle/token",
                SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.example.com/oauth/@handle/token"));
        }

        [SFFact]
        public void TestNormalizeUrlDropsQueryAndFragment()
        {
            Assert.Equal("host.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path?q=1#frag"));
            // An '@' inside the dropped query string must not be mistaken for userinfo.
            Assert.Equal("host.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path?email=user@domain.com"));
        }

        [SFFact]
        public void TestNormalizeUrlTrimsRootOnlyTrailingSlash()
        {
            Assert.Equal("host.com", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/"));
            Assert.Equal("host.com/path", SnowflakeCredentialManagerFactory.NormalizeUrl("https://host.com/path"));
        }

        [SFFact]
        public void TestNormalizeUrlPreservesPortAndPath()
        {
            Assert.Equal("login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
                SnowflakeCredentialManagerFactory.NormalizeUrl("https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0"));
        }

        [SFFact]
        public void TestNormalizeIdentifierUnquotedIsLowercased()
        {
            Assert.Equal("user@domain.com", SnowflakeCredentialManagerFactory.NormalizeIdentifier("USER@DOMAIN.COM"));
            Assert.Equal("analyst_role", SnowflakeCredentialManagerFactory.NormalizeIdentifier("ANALYST_ROLE"));
        }

        [SFFact]
        public void TestNormalizeIdentifierQuotedValueIsVerbatim()
        {
            // Any double-quote anywhere in the value → return verbatim, unchanged.
            Assert.Equal("\"First Last\"@long-corporate-domain.example.com",
                SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"First Last\"@long-corporate-domain.example.com"));
        }

        [SFFact]
        public void TestNormalizeIdentifierQuotedValueNotAtStart()
        {
            // A quote that is NOT at position 0 still triggers the verbatim path.
            Assert.Equal("prefix-\"seg\"",
                SnowflakeCredentialManagerFactory.NormalizeIdentifier("prefix-\"seg\""));
        }

        [SFFact]
        public void TestNormalizeIdentifierMixedQuotedAndUnquotedIsVerbatim()
        {
            // Contains quotes → entire value returned verbatim (mixed case preserved).
            Assert.Equal("\"Analyst Role With Spaces\":north_america:prod:readonly",
                SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"Analyst Role With Spaces\":north_america:prod:readonly"));
        }

        [SFFact]
        public void TestNormalizeIdentifierEmptyReturnsEmpty()
        {
            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.NormalizeIdentifier(string.Empty));
        }

        [SFFact]
        public void TestNormalizeIdentifierLowercasesNonAscii()
        {
            // No quotes → ToLowerInvariant applied to all characters.
            Assert.Equal("caf\u00e9", SnowflakeCredentialManagerFactory.NormalizeIdentifier("caf\u00e9"));
            Assert.Equal("stra\u00dfe", SnowflakeCredentialManagerFactory.NormalizeIdentifier("stra\u00dfe"));
        }

        [SFFact]
        public void TestNormalizeIdentifierSqlEscapedQuotesAreVerbatim()
        {
            // SQL-escaped double-quote inside the value still triggers verbatim path.
            Assert.Equal("\"Foo\"\"Bar\":baz", SnowflakeCredentialManagerFactory.NormalizeIdentifier("\"Foo\"\"Bar\":baz"));
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
