using System.Collections.Concurrent;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Revocation
{
    internal class MemoryCrlCache : ICrlCache
    {
        public static readonly MemoryCrlCache Instance = new MemoryCrlCache();

        private readonly ConcurrentDictionary<string, Crl> _cache = new ConcurrentDictionary<string, Crl>();

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<MemoryCrlCache>();

        internal MemoryCrlCache()
        {
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
    }
}
