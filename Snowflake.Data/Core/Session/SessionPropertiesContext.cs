using System.Security;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Session
{
    internal class SessionPropertiesContext
    {
        public SecureString Password { get; set; } = null;

        public SecureString Passcode { get; set; } = null;

        public SecureString ClientSecret { get; set; } = null;

        public bool AllowHttpForIdp { get; set; } = false;

        public void FillSecrets(SFSessionProperties properties)
        {
            FillSecret(properties, SFSessionProperty.PASSWORD, Password);
            FillSecret(properties, SFSessionProperty.OAUTHCLIENTSECRET, ClientSecret);
            FillSecret(properties, SFSessionProperty.PASSCODE, Passcode);
        }

        private void FillSecret(SFSessionProperties properties, SFSessionProperty property, SecureString secret)
        {
            if (secret != null && secret.Length > 0)
            {
                properties[property] = SecureStringHelper.Decode(secret);
            }
        }
    }
}
