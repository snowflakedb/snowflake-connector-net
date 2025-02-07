namespace Snowflake.Data.Core.Session
{
    internal class SessionFactory : ISessionFactory
    {
        public SFSession NewSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            return new SFSession(connectionString, sessionContext, EasyLoggingStarter.Instance);
        }
    }
}
