namespace Snowflake.Data.Core.Revocation
{
    internal class DisabledCrlCache : ICrlCache
    {
        public static readonly DisabledCrlCache Instance = new DisabledCrlCache();

        internal DisabledCrlCache()
        {
        }

        public Crl Get(string crlUrl) => null;

        public void Set(string crlUrl, Crl crl)
        {
        }
    }
}
