using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityGcpAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityGcpAttestationRetriever>();
        private const string DefaultMetadataServiceHost = "http://169.254.169.254";
        private const string MetadataEndpoint = "/computeMetadata/v1/instance/service-accounts/default/identity";
        private const string AccessTokenEndpoint = "/computeMetadata/v1/instance/service-accounts/default/token";
        private const string IamCredentialsHost = "https://iamcredentials.googleapis.com";

        private static readonly TimeSpan s_defaultTimeout = TimeSpan.FromSeconds(30);

        private TimeSpan HttpTimeout { get; set; } = s_defaultTimeout;

        private TimeSpan RestTimeout { get; set; } = s_defaultTimeout;

        private readonly IRestRequester _restRequester;
        private readonly string _metadataServiceHost;
        private readonly string _iamCredentialsHost;

        public WorkflowIdentityGcpAttestationRetriever(IRestRequester restRequester, string metadataServiceHost)
            : this(restRequester, metadataServiceHost, null)
        {
        }

        internal WorkflowIdentityGcpAttestationRetriever(IRestRequester restRequester, string metadataServiceHost, string iamCredentialsHost)
        {
            _restRequester = restRequester;
            _metadataServiceHost = metadataServiceHost ?? DefaultMetadataServiceHost;
            // For testing: if metadataServiceHost is provided (mock), use the same host for IAM credentials
            // unless an explicit iamCredentialsHost is provided
            _iamCredentialsHost = iamCredentialsHost ?? (metadataServiceHost != null ? metadataServiceHost : IamCredentialsHost);
        }

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.GCP;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string tokenParam, string impersonationPath = null)
        {
            string response;
            if (!string.IsNullOrEmpty(impersonationPath))
            {
                // Transitive impersonation: use the attached identity to get an ID token for the target service account(s)
                // The impersonation path is a comma-separated list of service account emails
                var serviceAccounts = impersonationPath.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                var targetServiceAccount = serviceAccounts[serviceAccounts.Length - 1].Trim();
                var delegates = serviceAccounts.Length > 1
                    ? serviceAccounts.Take(serviceAccounts.Length - 1).Select(sa => sa.Trim()).ToArray()
                    : null;

                s_logger.Debug($"Using transitive service account impersonation to get ID token for: {targetServiceAccount}" +
                              (delegates != null ? $" via delegates: {string.Join(", ", delegates)}" : ""));
                response = GetImpersonatedIdToken(targetServiceAccount, delegates);
            }
            else
            {
                // Direct authentication: get ID token for the attached identity
                var request = PrepareDirectIdentityRequest();
                response = GetIdentityResponse(request);
            }

            var token = new JwtTokenExtractor().ReadJwtToken(response, AttestationError);
            var issuer = token.Issuer;
            var subject = token.Subject;
            if (string.IsNullOrEmpty(issuer) || string.IsNullOrEmpty(subject))
            {
                var errorMessage = "No issuer or subject found in the token.";
                s_logger.Error(errorMessage);
                throw AttestationError(errorMessage);
            }

            return new WorkloadIdentityAttestationData
            {
                Provider = AttestationProvider.GCP,
                Credential = response,
                UserIdentifierComponents = new Dictionary<string, string> { { "sub", subject } }
            };
        }

        /// <summary>
        /// Gets an ID token for the target service account using transitive impersonation.
        /// This uses the attached identity to impersonate the target service account and generate an ID token for it.
        /// The attached identity must have the iam.serviceAccounts.getOpenIdToken permission on the target service account.
        /// </summary>
        /// <param name="targetServiceAccount">The final service account to get an ID token for.</param>
        /// <param name="delegates">Optional array of intermediate service accounts in the impersonation chain.
        /// Each service account in the chain must have the roles/iam.serviceAccountTokenCreator role on the next account.</param>
        private string GetImpersonatedIdToken(string targetServiceAccount, string[] delegates = null)
        {
            // Step 1: Get an access token for the attached identity
            var accessToken = GetAccessToken();

            // Step 2: Use the access token to call the IAM Credentials API to generate an ID token for the target SA
            var idToken = GenerateIdTokenForServiceAccount(accessToken, targetServiceAccount, delegates);

            return idToken;
        }

        /// <summary>
        /// Gets an access token for the attached identity from the GCP metadata service.
        /// </summary>
        private string GetAccessToken()
        {
            var url = $"{_metadataServiceHost}{AccessTokenEndpoint}";
            var uri = new Uri(url);
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.Add("Metadata-Flavor", "Google");
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);

            try
            {
                var response = _restRequester.Get<GcpAccessTokenResponse>(new RestRequestWrapper(request));
                if (string.IsNullOrEmpty(response?.AccessToken))
                {
                    throw AttestationError("Failed to get access token from metadata service: empty response");
                }
                return response.AccessToken;
            }
            catch (Exception exception) when (!(exception is Client.SnowflakeDbException))
            {
                var realException = HttpUtil.UnpackAggregateException(exception);
                s_logger.Error($"Failed to get access token from metadata service: {realException.Message}");
                throw AttestationError($"Failed to get access token from metadata service: {realException.Message}");
            }
        }

        /// <summary>
        /// Generates an ID token for the target service account using the IAM Credentials API.
        /// </summary>
        /// <param name="accessToken">The access token for the attached identity.</param>
        /// <param name="targetServiceAccount">The service account to generate an ID token for.</param>
        /// <param name="delegates">Optional array of intermediate service accounts in the impersonation chain.</param>
        private string GenerateIdTokenForServiceAccount(string accessToken, string targetServiceAccount, string[] delegates = null)
        {
            // Build the request URL: POST https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{SA_EMAIL}:generateIdToken
            var url = $"{_iamCredentialsHost}/v1/projects/-/serviceAccounts/{targetServiceAccount}:generateIdToken";
            var uri = new Uri(url);

            var requestBody = new GcpGenerateIdTokenRequest
            {
                Audience = SnowflakeAudience,
                IncludeEmail = true,
                Delegates = delegates?.Select(sa => $"projects/-/serviceAccounts/{sa}").ToArray()
            };

            var jsonContent = JsonConvert.SerializeObject(requestBody);
            var request = new HttpRequestMessage(HttpMethod.Post, uri)
            {
                Content = new StringContent(jsonContent, Encoding.UTF8, "application/json")
            };
            request.Headers.Add("Authorization", $"Bearer {accessToken}");
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);

            try
            {
                var response = _restRequester.Post<GcpGenerateIdTokenResponse>(new RestRequestWrapper(request));
                if (string.IsNullOrEmpty(response?.Token))
                {
                    throw AttestationError($"Failed to generate ID token for service account {targetServiceAccount}: empty response");
                }
                return response.Token;
            }
            catch (Exception exception) when (!(exception is Client.SnowflakeDbException))
            {
                var realException = HttpUtil.UnpackAggregateException(exception);
                s_logger.Error($"Failed to generate ID token for service account {targetServiceAccount}: {realException.Message}");
                throw AttestationError($"Failed to generate ID token for service account {targetServiceAccount}: {realException.Message}");
            }
        }

        private HttpRequestMessage PrepareDirectIdentityRequest()
        {
            var url = $"{_metadataServiceHost}{MetadataEndpoint}?audience={SnowflakeAudience}";
            var uri = new Uri(url);
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.Add("Metadata-Flavor", "Google");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);
            return request;
        }

        private string GetIdentityResponse(HttpRequestMessage request)
        {
            string response = null;
            try
            {
                using (var responseMessage = _restRequester.Get(new RestRequestWrapper(request)))
                {
                    response = responseMessage.Content.ReadAsStringAsync().Result;
                }
            }
            catch (Exception exception)
            {
                var realException = HttpUtil.UnpackAggregateException(exception);
                s_logger.Error($"Failed to get token in workload_identity authentication for GCP: {realException.Message}");
                throw AttestationError($"Failed to get token: {realException.Message}");
            }
            if (string.IsNullOrEmpty(response))
            {
                s_logger.Error($"Empty response for getting a token in workload_identity authentication for GCP");
                throw AttestationError("Empty response for getting a token");
            }
            return response;
        }
    }

    /// <summary>
    /// Response model for GCP metadata service access token endpoint.
    /// </summary>
    internal class GcpAccessTokenResponse
    {
        [JsonProperty("access_token")]
        public string AccessToken { get; set; }

        [JsonProperty("expires_in")]
        public int ExpiresIn { get; set; }

        [JsonProperty("token_type")]
        public string TokenType { get; set; }
    }

    /// <summary>
    /// Request model for the IAM Credentials generateIdToken API.
    /// </summary>
    internal class GcpGenerateIdTokenRequest
    {
        [JsonProperty("audience")]
        public string Audience { get; set; }

        [JsonProperty("includeEmail")]
        public bool IncludeEmail { get; set; }

        [JsonProperty("delegates", NullValueHandling = NullValueHandling.Ignore)]
        public string[] Delegates { get; set; }
    }

    /// <summary>
    /// Response model for the IAM Credentials generateIdToken API.
    /// </summary>
    internal class GcpGenerateIdTokenResponse
    {
        [JsonProperty("token")]
        public string Token { get; set; }
    }
}
