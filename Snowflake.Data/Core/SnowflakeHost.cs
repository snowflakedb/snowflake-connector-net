using System;
using System.Collections.Generic;
using System.Linq;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core
{
    internal static class SnowflakeHost
    {
        public const string DefaultHost = "snowflakecomputing.com";
        private const string AlternativeHost = "snowflakecomputing.cn";
        public const string GovHost = "snowflakecomputing.mil";

        // Canonical suffix allowlist for the WORKLOAD_IDENTITY authenticator. Matching is anchored
        // to a label boundary at the end of the host, so only these suffixes and their subdomains
        // are recognized. This is intentionally separate from IsSnowflakeHost below (used for OAuth
        // "is Snowflake the IdP" checks), which has other callers and different semantics (e.g. does
        // not accept the bare apex).
        private static readonly string[] s_wifAllowedHostSuffixes = { DefaultHost, AlternativeHost, GovHost };

        public static bool IsSnowflakeHost(string host) =>
            host.EndsWith($".{DefaultHost}", StringComparison.OrdinalIgnoreCase) ||
            host.EndsWith($".{AlternativeHost}", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Suffix-anchored allowlist that restricts Workload Identity attestation to recognized
        /// Snowflake hosts before any cloud credential is fetched. Accepts the bare apex (e.g.
        /// "snowflakecomputing.com") in addition to any subdomain, and supports an additive escape
        /// hatch via the SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES environment variable.
        /// </summary>
        internal static bool IsSnowflakeHostForWorkloadIdentity(string host, IEnvironmentFacade environmentFacade = null)
        {
            var normalizedHost = NormalizeHost(host);
            if (string.IsNullOrEmpty(normalizedHost))
            {
                return false;
            }

            var allowedSuffixes = GetAllowedSuffixes(environmentFacade);
            foreach (var suffix in allowedSuffixes)
            {
                if (normalizedHost.Equals(suffix, StringComparison.Ordinal) ||
                    normalizedHost.EndsWith("." + suffix, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        private static IEnumerable<string> GetAllowedSuffixes(IEnvironmentFacade environmentFacade)
        {
            foreach (var suffix in s_wifAllowedHostSuffixes)
            {
                yield return suffix;
            }

            foreach (var suffix in GetExtraAllowedHostSuffixes(environmentFacade))
            {
                yield return suffix;
            }
        }

        /// <summary>
        /// Parses and normalizes the additive suffixes configured via the SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES
        /// environment variable. Exposed (in addition to being used by GetAllowedSuffixes above) so
        /// callers can log at INFO naming the extra suffixes without this static class needing its own
        /// logger instance (SnowflakeHost cannot be a generic type argument to
        /// SFLoggerFactory.GetLogger&lt;T&gt;() because it is a static class).
        /// </summary>
        internal static IReadOnlyList<string> GetExtraAllowedHostSuffixes(IEnvironmentFacade environmentFacade)
        {
            var facade = environmentFacade ?? EnvironmentFacade.Instance;
            var extraSuffixesRaw = facade.GetString(EnvVars.WifAllowedHostSuffixes);
            if (string.IsNullOrEmpty(extraSuffixesRaw))
            {
                return Array.Empty<string>();
            }

            return extraSuffixesRaw
                .Split(',')
                .Select(NormalizeHost)
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList();
        }

        private static string NormalizeHost(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return string.Empty;
            }

            var normalized = host.Trim().ToLowerInvariant();

            // Strip the port before FQDN trailing dots, so that a host carrying both
            // (e.g. "acct.snowflakecomputing.com.:443") still normalizes to a matchable form.
            var colonIndex = normalized.IndexOf(':');
            if (colonIndex >= 0)
            {
                normalized = normalized.Substring(0, colonIndex);
            }

            normalized = normalized.TrimEnd('.');

            return normalized;
        }
    }
}
