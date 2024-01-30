﻿/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator.Okta;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator
{
    /// <summary>
    /// Interface for Authenticator
    /// For simplicity, only the Asynchronous function call is created
    /// </summary>
    internal interface IAuthenticator
    {
        /// <summary>
        /// Process the authentication asynchronouly
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="SnowflakeDbException"></exception>
        Task AuthenticateAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Process the authentication synchronously
        /// </summary>
        /// <exception cref="SnowflakeDbException"></exception>
        void Authenticate();
    }

    /// <summary>
    /// Types of authenticators
    /// </summary>
    internal enum SFAuthenticatorType
    {
        SNOWFLAKE,
        OKTA,
    }
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

        /// <summary>
        /// Authenticator Factory to build authenticators
        /// </summary>
        internal class AuthenticatorFactory
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<AuthenticatorFactory>();
        /// <summary>
        /// Generate the authenticator given the session
        /// </summary>
        /// <param name="session">session that requires the authentication</param>
        /// <returns>authenticator</returns>
        /// <exception cref="SnowflakeDbException">when authenticator is unknown</exception>
        internal static IAuthenticator GetAuthenticator(SFSession session)
        {
            string type = session.properties[SFSessionProperty.AUTHENTICATOR];
            if (type.Equals(BasicAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                return new BasicAuthenticator(session);
            }
            else if (type.Equals(ExternalBrowserAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                return new ExternalBrowserAuthenticator(session);
            }
            else if (type.Equals(KeyPairAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
            {
                // Get private key path or private key from connection settings
                if (!session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY_FILE, out var pkPath) &&
                    !session.properties.TryGetValue(SFSessionProperty.PRIVATE_KEY, out var pkContent))
                {
                    // There is no PRIVATE_KEY_FILE defined, can't authenticate with key-pair
                    string invalidStringDetail =
                        "Missing required PRIVATE_KEY_FILE or PRIVATE_KEY for key pair authentication";
                    var error = new SnowflakeDbException(
                        SFError.INVALID_CONNECTION_STRING,
                        new object[] { invalidStringDetail });
                    logger.Error(error.Message, error);
                    throw error;
                }

                return new KeyPairAuthenticator(session);
            }
            else
            {
                if (type.Equals(OAuthAuthenticator.AUTH_NAME, StringComparison.InvariantCultureIgnoreCase))
                {
                    // Get private key path or private key from connection settings
                    if (!session.properties.TryGetValue(SFSessionProperty.TOKEN, out var pkPath))
                    {
                        // There is no TOKEN defined, can't authenticate with oauth
                        string invalidStringDetail =
                            "Missing required TOKEN for Oauth authentication";
                        var error = new SnowflakeDbException(
                            SFError.INVALID_CONNECTION_STRING,
                            new object[] { invalidStringDetail });
                        logger.Error(error.Message, error);
                        throw error;
                    }

                    return new OAuthAuthenticator(session);
                }
                // Okta would provide a url of form: https://xxxxxx.okta.com or https://xxxxxx.oktapreview.com or https://vanity.url/snowflake/okta
                if (type.Contains("okta") && type.StartsWith("https://"))
                {
                    return new OktaAuthenticator(session, type);
                }
            }

            var e = new SnowflakeDbException(SFError.UNKNOWN_AUTHENTICATOR, type);

            logger.Error("Unknown authenticator", e);

            throw e;
        }
    }
}
