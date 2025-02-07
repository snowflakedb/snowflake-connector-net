namespace Snowflake.Data.Core.Session
{
    internal interface ISessionFactory
    {
        SFSession NewSession(string connectionString, SessionPropertiesContext sessionContext);
    }
}
