namespace Snowflake.Data.Core.Revocation
{
    internal class FileCrlCache : ICrlCache
    {
        public static readonly FileCrlCache Instance = new FileCrlCache();

        // private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<FileCrlCache>();

        internal FileCrlCache()
        {
        }

        public Crl Get(string crlUrl)
        {
            return null; // TODO: implement
        }

        public void Set(string crlUrl, Crl crl)
        {
            //s_logger.Debug($"Updating file crl cache for crl url: {crlUrl}");
            // TODO: implement
        }
    }
}
