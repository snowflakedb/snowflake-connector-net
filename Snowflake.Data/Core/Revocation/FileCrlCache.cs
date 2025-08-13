namespace Snowflake.Data.Core.Revocation
{
    internal class FileCrlCache : ICrlCache
    {
        public static readonly FileCrlCache Instance = new FileCrlCache();

        internal FileCrlCache()
        {
        }

        public Crl Get(string crlUrl)
        {
            return null; // TODO: implement
        }

        public void Set(string crlUrl, Crl crl)
        {
            // TODO: implement
        }
    }
}
