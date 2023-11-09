using System.Security;

namespace Snowflake.Data.Core.Session
{
    internal class SessionFactory : ISessionFactory
    {
        public SFSession NewSession(string connectionString, SecureString password)
        {
            return new SFSession(connectionString, password);
        }
    }
}
