using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Newtonsoft.Json;

namespace Snowflake.Data.Client
{
    /// <summary>Input parameters for building a v2 token cache key.</summary>
    internal sealed class CacheKeyInput
    {
        public string TokenType { get; }
        public string Idp { get; }
        public string SnowflakeUrl { get; }
        public string Username { get; }
        public string Role { get; }

        public CacheKeyInput(string tokenType, string idp, string snowflake, string username, string role)
        {
            TokenType = tokenType;
            Idp = idp;
            SnowflakeUrl = snowflake;
            Username = username;
            Role = role;
        }
    }

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

            bool isOAuth = input.TokenType is "OauthAccessToken"
                            or "OauthRefreshToken"
                            or "DpopBundledAccessToken";

            SortedDictionary<string, string> keyData;
            if (isOAuth)
            {
                keyData = new SortedDictionary<string, string>
                {
                    ["idp"]       = NormalizeUrl(input.Idp),
                    ["role"]      = NormalizeIdentifier(input.Role),
                    ["snowflake"] = NormalizeUrl(input.SnowflakeUrl),
                    ["username"]  = NormalizeIdentifier(input.Username),
                };
            }
            else  // MFA_TOKEN, ID_TOKEN
            {
                keyData = new SortedDictionary<string, string>
                {
                    ["snowflake"] = NormalizeUrl(input.SnowflakeUrl),
                    ["username"]  = NormalizeIdentifier(input.Username),
                };
            }

            string json = JsonConvert.SerializeObject(keyData, Formatting.None);
            string hash = ToSha256HashLower(json);
            return $"SnowflakeTokenCache.v2.{input.TokenType}.{hash}";
        }

        /// <summary>
        /// Strips the scheme and any userinfo prefix, drops query and fragment, then lowercases the
        /// remaining authority (host and any explicitly-stated port) and path, trimming trailing slashes.
        /// The raw string is used (not a parsed URL) so an explicit default port such as <c>:443</c> is preserved.
        /// </summary>
        internal static string NormalizeUrl(string url)
        {
            if (string.IsNullOrEmpty(url))
                return string.Empty;

            // Strip the scheme prefix ("scheme://") from the raw string, preserving any explicit port.
            var schemeIdx = url.IndexOf("://", StringComparison.Ordinal);
            var s = schemeIdx >= 0 ? url.Substring(schemeIdx + 3) : url;

            // Drop query string and fragment; they never appear in cache keys.
            s = s.Split('?')[0].Split('#')[0];

            // Strip userinfo ("user:pass@") from the authority only. The authority ends at the first
            // '/', so an '@' before that slash is a userinfo delimiter; an '@' inside the path survives.
            var slashIdx = s.IndexOf('/');
            var authorityEnd = slashIdx >= 0 ? slashIdx : s.Length;
            var authority = s.Substring(0, authorityEnd);
            var path = s.Substring(authorityEnd);
            var atIdx = authority.IndexOf('@');
            if (atIdx >= 0)
                authority = authority.Substring(atIdx + 1);

            return (authority + path).TrimEnd('/').ToLowerInvariant();
        }

        /// <summary>
        /// Normalizes a Snowflake identifier for use as a cache key field.
        /// If the value contains any double-quote character (<c>"</c>), it is returned
        /// verbatim — the quotes signal case-sensitive SQL semantics that must not be altered.
        /// Otherwise the entire value is lowercased: unquoted identifiers are case-insensitive
        /// in Snowflake so lowercasing produces a stable canonical form.
        /// </summary>
        internal static string NormalizeIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return string.Empty;
            return identifier.Contains("\"") ? identifier : identifier.ToLowerInvariant();
        }

        private static string ToSha256HashLower(string text)
        {
            using (var sha = SHA256.Create())
            {
                var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(text));
                return BitConverter.ToString(hash).Replace("-", string.Empty).ToLowerInvariant();
            }
        }

        [Obsolete("Use BuildCacheKey(CacheKeyInput) instead. This method produces a legacy key format.")]
        internal static string GetSecureCredentialKey(string host, string user, TokenType tokenType)
        {
            return $"{host.ToUpper()}:{user.ToUpper()}:{tokenType.ToString().ToUpper()}".ToSha256Hash();
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
