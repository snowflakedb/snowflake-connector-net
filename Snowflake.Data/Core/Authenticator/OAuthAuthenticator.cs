using Snowflake.Data.Log;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Authenticator
{
    class OAuthAuthenticator : IAuthenticator
    {
        public static readonly string AUTH_NAME = "oauth";
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<OAuthAuthenticator>();
        private SFSession session;

        internal OAuthAuthenticator(SFSession session)
        {
            this.session = session;
        }

        async Task IAuthenticator.AuthenticateAsync(CancellationToken cancellationToken)
        {
            var loginRequest = BuildLoginRequest();

            var response = await session.restRequester.PostAsync<LoginResponse>(loginRequest, cancellationToken).ConfigureAwait(false);

            session.ProcessLoginResponse(response);
        }

        void IAuthenticator.Authenticate()
        {
            var loginRequest = BuildLoginRequest();

            var response = session.restRequester.Post<LoginResponse>(loginRequest);

            session.ProcessLoginResponse(response);
        }

        private SFRestRequest BuildLoginRequest()
        {
            // build uri
            var loginUrl = session.BuildLoginUrl();

            LoginRequestData data = new LoginRequestData()
            {
                Authenticator = AUTH_NAME,
                loginName = session.properties[SFSessionProperty.USER],
                Token = session.properties[SFSessionProperty.OAUTH_TOKEN],
                accountName = session.properties[SFSessionProperty.ACCOUNT],
                clientAppId = SFEnvironment.DriverName,
                clientAppVersion = SFEnvironment.DriverVersion,
                clientEnv = SFEnvironment.ClientEnv,
                SessionParameters = session.ParameterMap,
            };

            int connectionTimeoutSec = int.Parse(session.properties[SFSessionProperty.CONNECTION_TIMEOUT]);

            return session.BuildTimeoutRestRequest(loginUrl, new LoginRequest() { data = data });
        }
    }
}
