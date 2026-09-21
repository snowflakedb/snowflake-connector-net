using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Newtonsoft.Json;

namespace Snowflake.Data.Client
{
    /// <summary>Input parameters for building a v2 token cache key.</summary>
    internal readonly record struct CacheKeyInput(TokenType TokenType, string Idp, string SnowflakeUrl, string Username, string Role);

    public class SnowflakeCredentialManagerFactory
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeCredentialManagerFactory>();

        private static readonly object s_credentialManagerLock = new object();
        private static readonly ISnowflakeCredentialManager s_defaultCredentialManager = GetDefaultCredentialManager();

        private static ISnowflakeCredentialManager s_credentialManager = s_defaultCredentialManager;

        /// <summary>
        /// Builds a v2 token cache key:
        /// <c>SnowflakeTokenCache.v2.&lt;TokenType&gt;.&lt;sha256hex(canonical_json(keyData))&gt;</c>.
        /// The PascalCase token type appears in the readable prefix so keystore tooling can
        /// identify token classes without decoding the opaque hash. <c>keyData</c> is
        /// flow-specific and never contains the token type: OAuth flows include <c>idp</c>,
        /// <c>role</c>, <c>snowflake</c>, and <c>username</c> (all lowercased); MFA and ID
        /// token flows include only <c>snowflake</c> and <c>username</c>.
        /// </summary>
        internal static string BuildCacheKey(CacheKeyInput input)
        {
            if (string.IsNullOrEmpty(input.SnowflakeUrl))
                throw new ArgumentException("snowflake URL must not be empty");
            if (string.IsNullOrEmpty(input.Username))
                throw new ArgumentException("username must not be empty");

            var isOAuth = input.TokenType is TokenType.OAuthAccessToken
                or TokenType.OAuthRefreshToken
                or TokenType.DpopBundledAccessToken;

            var keyData = isOAuth
                ? new SortedDictionary<string, string>
                {
                    ["idp"] = NormalizeUrl(input.Idp),
                    ["role"] = NormalizeIdentifier(input.Role),
                    ["snowflake"] = NormalizeUrl(input.SnowflakeUrl),
                    ["username"] = NormalizeIdentifier(input.Username),
                }
                : new SortedDictionary<string, string>
                {
                    ["snowflake"] = NormalizeUrl(input.SnowflakeUrl),
                    ["username"] = NormalizeIdentifier(input.Username),
                };

            var json = JsonConvert.SerializeObject(keyData, Formatting.None);
            var hash = json.ToSha256Hash().ToLowerInvariant();
            return $"SnowflakeTokenCache.v2.{input.TokenType.ToCacheKeyPrefix()}.{hash}";
        }

        /// <summary>
        /// Returns a lowercase host, explicitly stated port, and path, without scheme,
        /// userinfo, query, fragment, or trailing slashes.
        /// </summary>
        internal static string NormalizeUrl(string url)
        {
            if (string.IsNullOrEmpty(url))
                return string.Empty;

            const UriComponents Zero = 0;
            var uri = new Uri(url, UriKind.RelativeOrAbsolute);
            var hasExplicitScheme = uri.IsAbsoluteUri;
            if (!hasExplicitScheme)
                uri = new Uri($"https://{uri}");

            // Non-default ports are always part of the authority. Default ports (e.g. :443)
            // are kept only when they were written explicitly in the original URL.
            var hasExplicitPort = !uri.IsDefaultPort ||
                url.IndexOf($"{uri.Host}:{uri.Port}", StringComparison.Ordinal) >= 0;

            var resultComponents = UriComponents.Host | (hasExplicitPort ? UriComponents.Port : Zero);
            // Slice the path from the original string so percent-encoding is preserved
            // (Uri canonicalizes escaped unreserved characters, e.g. "%7E" → "~").
            var pathStartIndex = uri.GetComponents(
                resultComponents | UriComponents.UserInfo | (hasExplicitScheme ? UriComponents.Scheme : Zero),
                UriFormat.Unescaped).Length;
            var pathEnd = url.IndexOfAny(['?', '#'], pathStartIndex);
            if (pathEnd < 0)
                pathEnd = url.Length;

            var host = uri.GetComponents(resultComponents, UriFormat.Unescaped);
            var path = url.Substring(pathStartIndex, pathEnd - pathStartIndex);
            return (host + path).TrimEnd('/').ToLowerInvariant();
        }

        /// <summary>
        /// Normalizes a Snowflake identifier for use as a cache key field.
        /// If the value contains any double-quote character (<c>"</c>), it is returned
        /// verbatim — the quotes signal case-sensitive SQL semantics that must not be altered.
        /// SQL escaped-quotes (<c>""</c>) contain a <c>"</c> and are therefore also returned verbatim.
        /// Otherwise the entire value is lowercased: unquoted identifiers are case-insensitive
        /// in Snowflake so lowercasing produces a stable canonical form.
        /// </summary>
        internal static string NormalizeIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return string.Empty;
            return identifier.Contains("\"") ? identifier : identifier.ToLowerInvariant();
        }

        public static void UseDefaultCredentialManager()
        {
            SetCredentialManager(GetDefaultCredentialManager());
        }

        public static void UseInMemoryCredentialManager()
        {
            SetCredentialManager(SFCredentialManagerInMemoryImpl.Instance);
        }

        public static void UseFileCredentialManager()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var errorMessage = "File credential manager implementation is not supported on Windows";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            SetCredentialManager(SFCredentialManagerFileImpl.Instance);
        }

        public static void UseWindowsCredentialManager()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var errorMessage = "Windows native credential manager implementation can be used only on Windows";
                s_logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
            SetCredentialManager(SFCredentialManagerWindowsNativeImpl.Instance);
        }

        public static void SetCredentialManager(ISnowflakeCredentialManager customCredentialManager)
        {
            lock (s_credentialManagerLock)
            {
                if (customCredentialManager == null)
                {
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                        "Credential manager cannot be null. If you want to use the default credential manager, please call the UseDefaultCredentialManager method.");
                }

                if (customCredentialManager == s_credentialManager)
                {
                    s_logger.Info($"Credential manager is already set to: {customCredentialManager.GetType().Name}");
                    return;
                }

                s_logger.Info($"Setting the credential manager: {customCredentialManager.GetType().Name}");
                s_credentialManager = customCredentialManager;
            }
        }

        public static ISnowflakeCredentialManager GetCredentialManager()
        {
            var credentialManager = s_credentialManager;
            var typeCredentialText = credentialManager == s_defaultCredentialManager ? "default" : "custom";
            s_logger.Info($"Using {typeCredentialText} credential manager: {credentialManager?.GetType().Name}");
            return credentialManager;
        }

        private static ISnowflakeCredentialManager GetDefaultCredentialManager()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? SFCredentialManagerWindowsNativeImpl.Instance
                : SFCredentialManagerFileImpl.Instance;
        }
    }
}
