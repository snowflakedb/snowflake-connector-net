using System.Security;

namespace Snowflake.Data.Core.Session
{
    internal class SessionFactory : ISessionFactory
    {
        public SFSession NewSession(string connectionString, SecureString password, SecureString passcode)
        {
            return new SFSession(connectionString, password, passcode);
        }
    }
}
