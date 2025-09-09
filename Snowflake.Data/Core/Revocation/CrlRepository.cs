namespace Snowflake.Data.Core.Revocation
{
    internal class CrlRepository : ICrlCache
    {
        internal readonly ICrlCache _memoryCrlCache;

        internal readonly ICrlCache _fileCrlCache;

        internal CrlRepository(bool useMemoryCache, bool useFileCache)
        {
            _memoryCrlCache = useMemoryCache ? MemoryCrlCache.Instance : (ICrlCache)null;
            _fileCrlCache = useFileCache ? FileCrlCache.CreateInstance() : (ICrlCache)null;
        }

        internal CrlRepository(ICrlCache memoryCrlCache, ICrlCache fileCrlCache)
        {
            _memoryCrlCache = memoryCrlCache;
            _fileCrlCache = fileCrlCache;
        }

        public Crl Get(string crlUrl)
        {
            var crl = _memoryCrlCache?.Get(crlUrl);
            if (crl != null)
                return crl;
            crl = _fileCrlCache?.Get(crlUrl);
            if (crl != null)
                _memoryCrlCache?.Set(crlUrl, crl);
            return crl;
        }

        public void Set(string crlUrl, Crl crl)
        {
            _fileCrlCache?.Set(crlUrl, crl);
            _memoryCrlCache?.Set(crlUrl, crl);
        }
    }
}
