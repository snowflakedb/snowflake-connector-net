namespace Snowflake.Data.Core.Authenticator
{
    internal class ExternalBrowserToken
    {
        public string Token { get; set; }

        public ExternalBrowserToken(string token)
        {
            Token = token;
        }
    }
}
