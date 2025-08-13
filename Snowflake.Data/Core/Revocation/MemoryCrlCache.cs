using System.Collections.Concurrent;

namespace Snowflake.Data.Core.Revocation
{
    internal class MemoryCrlCache : ICrlCache
    {
        public static readonly MemoryCrlCache Instance = new MemoryCrlCache();

        private readonly ConcurrentDictionary<string, Crl> _cache = new ConcurrentDictionary<string, Crl>();

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
            _cache.AddOrUpdate(crlUrl, crl, (key, oldValue) => crl);
        }
    }
}
