using System;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Revocation
{
    internal class CrlRepository : ICrlCache
    {
        private static CrlCacheManager s_cacheManager;
        private static readonly object s_lock = new object();

        internal CrlRepository(bool useMemoryCache, bool useFileCache)
        {
            if (!useMemoryCache && !useFileCache) return;

            if (s_cacheManager != null) return;

            lock (s_lock)
            {
                if (s_cacheManager != null) return;

                var parser = new CrlParser(EnvironmentOperations.Instance);
                var cacheValidityTime = parser.GetCacheValidityTime();
                var cleanupInterval = GetCleanupInterval();

                s_cacheManager = CrlCacheManager.Build(useMemoryCache, useFileCache, cleanupInterval, cacheValidityTime);
            }
        }

        private static TimeSpan GetCleanupInterval()
        {
            const string envName = "SF_CRL_CACHE_REMOVAL_DELAY";
            const int defaultDays = 7;

            var cleanupDays = ValuesExtractor.ExtractInt(
                () => EnvironmentOperations.Instance.GetEnvironmentVariable(envName),
                $"environmental variable {envName}",
                defaultDays);
            return TimeSpan.FromDays(cleanupDays);
        }

        public Crl Get(string crlUrl) => s_cacheManager?.Get(crlUrl);

        public void Set(string crlUrl, Crl crl) => s_cacheManager?.Set(crlUrl, crl);
    }
}
