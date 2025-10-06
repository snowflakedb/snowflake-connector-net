using System;
using System.Collections.Concurrent;
using System.Linq;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Revocation
{
    internal class MemoryCrlCache : ICrlCache
    {
        private readonly ConcurrentDictionary<string, Crl> _cache = new ConcurrentDictionary<string, Crl>();

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<MemoryCrlCache>();

        private readonly TimeSpan _cacheValidityTime;

        internal MemoryCrlCache(TimeSpan cacheValidityTime)
        {
            _cacheValidityTime = cacheValidityTime;
        }

        public Crl Get(string crlUrl)
        {
            if (_cache.TryGetValue(crlUrl, out var crl))
            {
                return crl;
            }
            return null;
        }

        public void Set(string crlUrl, Crl crl)
        {
            s_logger.Debug($"Updating in memory crl cache for crl url: {crlUrl}");
            _cache.AddOrUpdate(crlUrl, crl, (key, oldValue) => crl);
        }

        public void Cleanup()
        {
            var now = DateTime.UtcNow;
            var keysToRemove = _cache
                .Where(entry => entry.Value.IsExpiredOrStale(now, _cacheValidityTime))
                .Select(entry => entry.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _cache.TryRemove(key, out _);
            }

            if (keysToRemove.Count > 0)
            {
                s_logger.Info($"Removed {keysToRemove.Count} expired/stale entries from in-memory CRL cache");
            }
        }
    }
}
