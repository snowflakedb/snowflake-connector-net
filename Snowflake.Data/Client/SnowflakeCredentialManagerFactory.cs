using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Newtonsoft.Json;

namespace Snowflake.Data.Client
{
    /// <summary>Carries the five dimensions for a v2 token cache key.</summary>
    internal sealed class CacheKeyInput
    {
        public string TokenType { get; }
        public string Idp { get; }
        public string Snowflake { get; }
        public string Username { get; }
        public string Role { get; }

        public CacheKeyInput(string tokenType, string idp, string snowflake, string username, string role)
        {
            TokenType = tokenType;
            Idp = idp;
            Snowflake = snowflake;
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
        /// Builds a v2 token cache key: <c>SnowflakeTokenCache.v2.&lt;sha256hex(canonical_json)&gt;</c>.
        /// Normalizes all dimensions before hashing; hashing occurs exactly once here.
        /// </summary>
        internal static string BuildCacheKey(CacheKeyInput input)
        {
            if (string.IsNullOrEmpty(input.Snowflake))
                throw new ArgumentException("snowflake URL must not be empty");
            if (string.IsNullOrEmpty(input.Username))
                throw new ArgumentException("username must not be empty");

            var keyData = new SortedDictionary<string, string>
            {
                ["idp"]        = NormalizeUrl(input.Idp),
                ["role"]       = NormalizeIdentifier(input.Role),
                ["snowflake"]  = NormalizeUrl(input.Snowflake),
                ["token_type"] = input.TokenType,
                ["username"]   = NormalizeIdentifier(input.Username),
            };

            string json = JsonConvert.SerializeObject(keyData, Formatting.None);
            string hash = ToSha256HashLower(json);
            return $"SnowflakeTokenCache.v2.{hash}";
        }

        /// <summary>Strips scheme, userinfo, query, fragment; uppercases the remainder; trims a root-only trailing slash.</summary>
        internal static string NormalizeUrl(string url)
        {
            if (string.IsNullOrEmpty(url))
                return string.Empty;
            var s = Regex.Replace(url, @"^https?://", "");
            var atIdx = s.IndexOf('@');
            if (atIdx >= 0) s = s.Substring(atIdx + 1);
            s = s.Split('?')[0].Split('#')[0];
            s = s.TrimEnd('/');
            return s.ToUpperInvariant();
        }

        /// <summary>Uppercases unquoted segments; preserves the content of double-quoted segments verbatim (including surrounding quotes).</summary>
        internal static string NormalizeIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return string.Empty;
            var sb = new StringBuilder();
            bool inQuotes = false;
            foreach (char c in identifier)
            {
                if (c == '"') { inQuotes = !inQuotes; sb.Append(c); }
                else if (inQuotes) sb.Append(c);
                else sb.Append(char.ToUpperInvariant(c));
            }
            return sb.ToString();
        }

        private static string ToSha256HashLower(string text)
        {
            using (var sha = SHA256.Create())
            {
                var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(text));
                return BitConverter.ToString(hash).Replace("-", string.Empty).ToLowerInvariant();
            }
        }

        [Obsolete("Use BuildCacheKey(CacheKeyInput) instead. This method produces a v1 key that is not cross-driver compatible.")]
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
