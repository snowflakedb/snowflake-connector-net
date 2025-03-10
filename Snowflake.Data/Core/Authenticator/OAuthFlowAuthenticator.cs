namespace Snowflake.Data.Core.Authenticator
{
    internal abstract class OAuthFlowAuthenticator : BaseAuthenticator
    {
        protected OAuthFlowAuthenticator(SFSession session, string authName) : base(session, authName)
        {
        }

    }
}
