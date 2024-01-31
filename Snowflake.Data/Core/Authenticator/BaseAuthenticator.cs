using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// A base implementation for all authenticators to create and send a login request.
    /// </summary>
    internal abstract class BaseAuthenticator
    {
        // The name of the authenticator.
        private readonly string _authName;

        // The session which created this authenticator.
        protected SFSession Session;

        // The client environment properties
        private readonly LoginRequestClientEnv _clientEnv = SFEnvironment.ClientEnv;

        /// <summary>
        /// The abstract base for all authenticators.
        /// </summary>
        /// <param name="session">The session which created the authenticator.</param>
        protected BaseAuthenticator(SFSession session, string authName)
        {
            this.Session = session;
            this._authName = authName;
            // Update the value for insecureMode because it can be different for each session
            _clientEnv.insecureMode = session.properties[SFSessionProperty.INSECUREMODE];
            if (session.properties.TryGetValue(SFSessionProperty.APPLICATION, out var applicationName))
            {
                // If an application name has been specified in the connection setting, use it
                // Otherwise, it will default to the running process name
                _clientEnv.application = applicationName;
            }
        }

        //// <see cref="IAuthenticator.AuthenticateAsync"/>
        protected async Task LoginAsync(CancellationToken cancellationToken)
        {
            var loginRequest = BuildLoginRequest();

            var response = await Session.restRequester.PostAsync<LoginResponse>(loginRequest, cancellationToken).ConfigureAwait(false);

            Session.ProcessLoginResponse(response);
        }

        /// <see cref="IAuthenticator.Authenticate"/>
        protected void Login()
        {
            var loginRequest = BuildLoginRequest();

            var response = Session.restRequester.Post<LoginResponse>(loginRequest);

            Session.ProcessLoginResponse(response);
        }

        /// <summary>
        /// Specialized authenticator data to add to the login request.
        /// </summary>
        /// <param name="data">The login request data to update.</param>
        protected abstract void SetSpecializedAuthenticatorData(ref LoginRequestData data);

        /// <summary>
        /// Builds a simple login request. Each authenticator will fill the Data part with their
        /// specialized information. The common Data attributes are already filled (clientAppId,
        /// ClienAppVersion...).
        /// </summary>
        /// <returns>A login request to send to the server.</returns>
        private SFRestRequest BuildLoginRequest()
        {
            // build uri
            var loginUrl = Session.BuildLoginUrl();

            var data = new LoginRequestData()
            {
                loginName = Session.properties[SFSessionProperty.USER],
                accountName = Session.properties[SFSessionProperty.ACCOUNT],
                clientAppId = SFEnvironment.DriverName,
                clientAppVersion = SFEnvironment.DriverVersion,
                clientEnv = _clientEnv,
                SessionParameters = Session.ParameterMap,
                Authenticator = _authName,
            };

            SetSpecializedAuthenticatorData(ref data);

            return Session.BuildTimeoutRestRequest(loginUrl, new LoginRequest() { data = data });
        }
    }
}