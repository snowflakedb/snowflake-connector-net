using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
#if NET8_0_OR_GREATER
using System.Net.Security;
#endif
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// Process-wide TLS cipher restriction read from SNOWFLAKE_TLS_CIPHERS.
    /// The accepted vocabulary matches the Python driver: IANA TLS 1.3 names and
    /// OpenSSL TLS 1.2 aliases, separated by colons.
    /// </summary>
    internal sealed class TlsCipherPolicy
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<TlsCipherPolicy>();

        private static readonly IReadOnlyDictionary<string, string> s_openSslToIana =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["ECDHE-ECDSA-AES128-GCM-SHA256"] = "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                ["ECDHE-ECDSA-AES256-GCM-SHA384"] = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                ["ECDHE-RSA-AES128-GCM-SHA256"] = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                ["ECDHE-RSA-AES256-GCM-SHA384"] = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                ["ECDHE-ECDSA-CHACHA20-POLY1305"] = "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
                ["ECDHE-RSA-CHACHA20-POLY1305"] = "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
                ["DHE-RSA-AES128-GCM-SHA256"] = "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
                ["DHE-RSA-AES256-GCM-SHA384"] = "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
                ["AES128-GCM-SHA256"] = "TLS_RSA_WITH_AES_128_GCM_SHA256",
                ["AES256-GCM-SHA384"] = "TLS_RSA_WITH_AES_256_GCM_SHA384"
            };

        private TlsCipherPolicy(string rawValue, IReadOnlyList<string> ianaNames)
        {
            RawValue = rawValue;
            IanaNames = ianaNames;
            CanonicalValue = string.Join(":", ianaNames);
            HasTls12Cipher = ianaNames.Any(name => !IsTls13(name));
            HasTls13Cipher = ianaNames.Any(IsTls13);
        }

        internal string RawValue { get; }

        internal string CanonicalValue { get; }

        internal IReadOnlyList<string> IanaNames { get; }

        internal bool HasTls12Cipher { get; }

        internal bool HasTls13Cipher { get; }

        internal static TlsCipherPolicy FromEnvironment() =>
            Parse(EnvironmentFacade.Instance.GetString(EnvVars.TlsCiphers));

        internal static TlsCipherPolicy Parse(string rawValue)
        {
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                return null;
            }

            var names = rawValue
                .Split(':')
                .Select(name => name.Trim())
                .Where(name => name.Length > 0)
                .ToArray();
            if (names.Length == 0)
            {
                throw InvalidPolicy(rawValue, "the value contains no cipher names.");
            }

            var ianaNames = names.Select(name =>
            {
                var isTls13Name = name.StartsWith("TLS_", StringComparison.OrdinalIgnoreCase);
                var ianaName = isTls13Name
                    ? name.Replace('-', '_').ToUpperInvariant()
                    : s_openSslToIana.TryGetValue(name, out var mappedName)
                        ? mappedName
                        : null;
                if (isTls13Name && !IsKnownTls13Cipher(ianaName))
                {
                    throw InvalidPolicy(rawValue, $"TLS 1.3 cipher suite '{name}' is not supported.");
                }
                if (ianaName == null || !IsKnownCipherSuite(ianaName))
                {
                    throw InvalidPolicy(rawValue, $"cipher name '{name}' is not supported.");
                }
                return ianaName;
            }).ToArray();

            return new TlsCipherPolicy(rawValue, ianaNames);
        }

        internal void WarnIfInconsistentWith(SslProtocols minTlsProtocol, SslProtocols maxTlsProtocol)
        {
            if (minTlsProtocol == SslProtocolsExtensions.Tls13 && HasTls12Cipher && !HasTls13Cipher)
            {
                s_logger.Warn($"{EnvVars.TlsCiphers.Name} configures only TLS 1.2 ciphers ({RawValue}) while MINTLS requires TLS 1.3, so none of them can be negotiated");
            }
            else if (maxTlsProtocol != SslProtocols.Tls12 && HasTls12Cipher && !HasTls13Cipher)
            {
                s_logger.Warn($"{EnvVars.TlsCiphers.Name} configures only TLS 1.2 ciphers ({RawValue}) while TLS 1.3 is permitted; TLS 1.3 handshakes have no allowed cipher suite");
            }
            if (maxTlsProtocol == SslProtocols.Tls12 && HasTls13Cipher && !HasTls12Cipher)
            {
                s_logger.Warn($"{EnvVars.TlsCiphers.Name} configures only TLS 1.3 cipher suites ({RawValue}) while MAXTLS permits only TLS 1.2, so none of them can be negotiated");
            }
            else if (minTlsProtocol == SslProtocols.Tls12 &&
                maxTlsProtocol != SslProtocols.Tls12 &&
                HasTls13Cipher && !HasTls12Cipher)
            {
                s_logger.Warn($"{EnvVars.TlsCiphers.Name} configures only TLS 1.3 cipher suites ({RawValue}) while TLS 1.2 is permitted; TLS 1.2 handshakes have no allowed cipher");
            }
        }

#if NET8_0_OR_GREATER
        internal CipherSuitesPolicy CreatePlatformPolicy()
        {
            if (OperatingSystem.IsWindows())
            {
                throw Unsupported(
                    "Windows does not support configuring TLS cipher suites per connection. Use the Windows Schannel policy instead.");
            }
            try
            {
                return new CipherSuitesPolicy(IanaNames.Select(ParseCipherSuite));
            }
            catch (PlatformNotSupportedException cause)
            {
                throw Unsupported(
                    "this operating system does not support configuring TLS cipher suites per connection. Use the operating system TLS policy instead.",
                    cause);
            }
        }

        private static TlsCipherSuite ParseCipherSuite(string ianaName) =>
            Enum.Parse<TlsCipherSuite>(ianaName, ignoreCase: true);
#endif

        internal SnowflakeDbException Unsupported(string reason, Exception cause = null) =>
            Unsupported(RawValue, reason, cause);

        internal static SnowflakeDbException Unsupported(string rawValue, string reason, Exception cause = null)
        {
            var exception = new SnowflakeDbException(
                cause,
                SFError.TLS_CONFIGURATION_NOT_SUPPORTED,
                $"{EnvVars.TlsCiphers.Name}={rawValue}",
                reason);
            s_logger.Error(exception.Message, exception);
            return exception;
        }

        private static SnowflakeDbException InvalidPolicy(string rawValue, string reason) =>
            Unsupported(rawValue, $"{reason} Expected a colon-separated list of OpenSSL TLS 1.2 names and IANA TLS 1.3 names.");

        private static bool IsKnownCipherSuite(string ianaName)
        {
#if NET8_0_OR_GREATER
            return Enum.TryParse<TlsCipherSuite>(ianaName, ignoreCase: true, out var cipherSuite) &&
                Enum.IsDefined(typeof(TlsCipherSuite), cipherSuite);
#else
            // CipherSuitesPolicy does not exist on this target. Keep parsing deterministic so an
            // unsupported runtime reports the platform error rather than accepting arbitrary names.
            return s_openSslToIana.Values.Contains(ianaName, StringComparer.OrdinalIgnoreCase) ||
                IsKnownTls13Cipher(ianaName);
#endif
        }

        private static bool IsKnownTls13Cipher(string ianaName) =>
            ianaName.Equals("TLS_AES_128_GCM_SHA256", StringComparison.OrdinalIgnoreCase) ||
            ianaName.Equals("TLS_AES_256_GCM_SHA384", StringComparison.OrdinalIgnoreCase) ||
            ianaName.Equals("TLS_CHACHA20_POLY1305_SHA256", StringComparison.OrdinalIgnoreCase) ||
            ianaName.Equals("TLS_AES_128_CCM_SHA256", StringComparison.OrdinalIgnoreCase) ||
            ianaName.Equals("TLS_AES_128_CCM_8_SHA256", StringComparison.OrdinalIgnoreCase);

        private static bool IsTls13(string ianaName) =>
            IsKnownTls13Cipher(ianaName);
    }
}
