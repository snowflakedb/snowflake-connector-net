using System.Security;

namespace Snowflake.Data.Core.Session
{
    internal interface ISessionFactory
    {
        SFSession NewSession(string connectionString, SecureString password, SecureString passcode);
    }
}
