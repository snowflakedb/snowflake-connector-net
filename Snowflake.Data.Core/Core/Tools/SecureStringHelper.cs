using System.Net;
using System.Security;

namespace Snowflake.Data.Core.Tools
{
    internal static class SecureStringHelper
    {
        public static string Decode(SecureString password) => new NetworkCredential(string.Empty, password).Password;

        public static SecureString Encode(string password) => new NetworkCredential(string.Empty, password).SecurePassword;
    }
}
