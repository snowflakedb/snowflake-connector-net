using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Mock
{
    internal class MockCloseSessionException : MockBaseRestRequester
    {
        internal static readonly int SESSION_CLOSE_ERROR = 390111;

        public override T Get<T>(IRestRequest request)
        {
            return Task.Run(async () => await GetAsync<T>(request, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public override Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken)
        {
            return Task.FromResult<T>((T)(object)null);
        }

        public override T Post<T>(IRestRequest postRequest)
        {
            return Task.Run(async () => await PostAsync<T>(postRequest, CancellationToken.None).ConfigureAwait(false)).Result;
        }

        public override Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken)
        {
            SFRestRequest sfRequest = (SFRestRequest)postRequest;
            if (sfRequest.jsonBody is LoginRequest)
            {
                LoginResponse authnResponse = new LoginResponse
                {
                    data = new LoginResponseData()
                    {
                        token = "session_token",
                        masterToken = "master_token",
                        authResponseSessionInfo = new SessionInfo(),
                        nameValueParameter = new List<NameValueParameter>()
                    },
                    success = true
                };

                // login request return success
                return Task.FromResult<T>((T)(object)authnResponse);
            }
            else
            {
                throw new SnowflakeDbException("", SESSION_CLOSE_ERROR, "Mock generated error", null);
            }
        }
    }
}
