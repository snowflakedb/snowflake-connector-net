using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Revocation
{
    internal class CrlRepository : ICrlCache
    {
        private readonly CrlCacheManager _cacheManager;

        internal CrlRepository(bool useMemoryCache, bool useFileCache) : this(EnvironmentFacade.Instance, useMemoryCache, useFileCache)
        {
        }

        internal CrlRepository(IEnvironmentFacade environmentFacade, bool useMemoryCache, bool useFileCache)
        {
            if (!useMemoryCache && !useFileCache) return;

            var parser = new CrlParser(environmentFacade);
            var cacheValidityTime = parser.GetCacheValidityTime();
            var cleanupInterval = parser.GetCleanupInterval();

            _cacheManager = CrlCacheManager.Build(useMemoryCache, useFileCache, cleanupInterval, cacheValidityTime);
        }

        public Crl Get(string crlUrl) => _cacheManager?.Get(crlUrl);

        public void Set(string crlUrl, Crl crl) => _cacheManager?.Set(crlUrl, crl);
    }
}
