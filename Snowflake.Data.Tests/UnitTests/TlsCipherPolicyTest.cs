using System;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;
#if NET8_0_OR_GREATER
using System.Net.Http;
using System.Net.Security;
#endif

namespace Snowflake.Data.Tests.UnitTests
{
    public class TlsCipherPolicyTest
    {
        [SFFact]
        public void TestUnsetPolicyLeavesCipherSelectionUnchanged()
        {
            Assert.Null(TlsCipherPolicy.Parse(null));
            Assert.Null(TlsCipherPolicy.Parse("  "));
        }

        [SFFact]
        public void TestPolicyAcceptsPythonCompatibleMixedNames()
        {
            var policy = TlsCipherPolicy.Parse(
                "TLS_AES_128_GCM_SHA256:ECDHE-RSA-AES256-GCM-SHA384");

            Assert.Equal(
                "TLS_AES_128_GCM_SHA256:TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                policy.CanonicalValue);
            Assert.True(policy.HasTls12Cipher);
            Assert.True(policy.HasTls13Cipher);
        }

        [SFTheory]
        [InlineData("NOT_A_CIPHER")]
        [InlineData("TLS_NOPE_SHA999")]
        [InlineData(" : ")]
        public void TestPolicyRejectsUnknownOrEmptyNames(string rawValue)
        {
            var exception = Assert.Throws<SnowflakeDbException>(() => TlsCipherPolicy.Parse(rawValue));

            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.TLS_CONFIGURATION_NOT_SUPPORTED);
            Assert.Contains("SNOWFLAKE_TLS_CIPHERS", exception.Message);
        }

#if NET8_0_OR_GREATER
        [SFFact]
        public void TestPolicyIsAppliedToSessionHandlerOrRejectedByPlatform()
        {
            var policy = TlsCipherPolicy.Parse("TLS_AES_128_GCM_SHA256");
            var config = new HttpClientConfig(
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                7,
                20,
                minTlsProtocol: "tls13",
                maxTlsProtocol: "tls13",
                tlsProtocolsExplicitlyRequested: true,
                tlsCipherPolicy: policy);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var exception = Assert.Throws<SnowflakeDbException>(
                    () => HttpUtil.Instance.SetupCustomHttpHandler(config));
                SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.TLS_CONFIGURATION_NOT_SUPPORTED);
                return;
            }

            using var handler = Assert.IsType<SocketsHttpHandler>(
                HttpUtil.Instance.SetupCustomHttpHandler(config));
            Assert.Equal(SslProtocolsExtensions.Tls13, handler.SslOptions.EnabledSslProtocols);
            Assert.Equal(
                new[] { TlsCipherSuite.TLS_AES_128_GCM_SHA256 },
                handler.SslOptions.CipherSuitesPolicy.AllowedCipherSuites.ToArray());
        }

        [SFFact]
        public void TestCipherOnlyPolicyPreservesStorageProtocolAndProxyDefaultsOrIsRejectedByPlatform()
        {
            var policy = TlsCipherPolicy.Parse("ECDHE-RSA-AES256-GCM-SHA384");

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var exception = Assert.Throws<SnowflakeDbException>(
                    () => HttpUtil.CreateStorageHandler(
                        SslProtocols.None,
                        cipherPolicy: policy));
                SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.TLS_CONFIGURATION_NOT_SUPPORTED);
                return;
            }

            using var handler = Assert.IsType<SocketsHttpHandler>(
                HttpUtil.CreateStorageHandler(
                    SslProtocols.None,
                    cipherPolicy: policy));
            Assert.Equal(SslProtocols.None, handler.SslOptions.EnabledSslProtocols);
            Assert.True(handler.UseProxy);
            Assert.Equal(
                new[] { TlsCipherSuite.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 },
                handler.SslOptions.CipherSuitesPolicy.AllowedCipherSuites.ToArray());
        }

        [SFFact]
        public void TestCustomCrlValidationIsPreservedOnCipherHandler()
        {
            Skip.When(
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows),
                "Windows rejects per-connection TLS cipher configuration.");
            var policy = TlsCipherPolicy.Parse("TLS_AES_128_GCM_SHA256");
            var config = new HttpClientConfig(
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                7,
                20,
                certRevocationCheckMode: "ENABLED",
                tlsCipherPolicy: policy);

            using var handler = Assert.IsType<SocketsHttpHandler>(
                HttpUtil.Instance.SetupCustomHttpHandler(config));

            Assert.NotNull(handler.SslOptions.RemoteCertificateValidationCallback);
        }
#else
        [SFFact]
        public void TestPolicyIsRejectedWhenTargetHasNoCipherSuiteApi()
        {
            var policy = TlsCipherPolicy.Parse("TLS_AES_128_GCM_SHA256");
            var exception = Assert.Throws<SnowflakeDbException>(
                () => HttpUtil.CreateStorageHandler(
                    SslProtocolsExtensions.Tls13,
                    cipherPolicy: policy));

            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.TLS_CONFIGURATION_NOT_SUPPORTED);
        }
#endif
    }
}
