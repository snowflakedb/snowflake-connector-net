using System.Security;

namespace Snowflake.Data.Core.Session
{
    internal class SessionPropertiesContext
    {
        public SecureString Password { get; set; } = null;

        public SecureString Passcode { get; set; } = null;

        public SecureString ClientId { get; set; } = null;

        public bool AllowHttpForIdp { get; set; } = false;
    }
}
