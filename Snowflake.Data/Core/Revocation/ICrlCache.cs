namespace Snowflake.Data.Core.Revocation
{
    internal interface ICrlCache
    {
        Crl Get(string crlUrl);

        void Set(string crlUrl, Crl crl);
    }
}
