using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Web;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityAzureAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly TimeSpan s_defaultTimeout = TimeSpan.FromSeconds(30);
        internal const string DefaultWorkloadIdentityEntraResource = "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad";
        private const string DefaultMetadataServiceHost = "http://169.254.169.254";
        private const string MetadataServiceEndpoint = "/metadata/identity/oauth2/token";

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityAzureAttestationRetriever>();

        private readonly EnvironmentOperations _environmentOperations;

        private readonly IRestRequester _restRequester;

        private readonly string _metadataServiceHost;

        private TimeSpan HttpTimeout { get; set; } = s_defaultTimeout;

        private TimeSpan RestTimeout { get; set; } = s_defaultTimeout;

        public WorkflowIdentityAzureAttestationRetriever(EnvironmentOperations environmentOperations, IRestRequester restRequester, string metadataServiceHost)
        {
            _environmentOperations = environmentOperations;
            _restRequester = restRequester;
            _metadataServiceHost = metadataServiceHost ?? DefaultMetadataServiceHost;
        }

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.AZURE;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string tokenParam)
        {
            var request = PrepareRequest(snowflakeEntraResource);
            var accessToken = GetAccessToken(request);
            var token = new JwtTokenExtractor().ReadJwtToken(accessToken, AttestationError);
            var issuer = token.Issuer;
            var subject = token.Subject;
            if (string.IsNullOrEmpty(issuer) || string.IsNullOrEmpty(subject))
            {
                var errorMessage = "No issuer or subject found in the token";
                s_logger.Error(errorMessage);
                throw AttestationError(errorMessage);
            }

            return new WorkloadIdentityAttestationData
            {
                Provider = AttestationProvider.AZURE,
                Credential = accessToken,
                UserIdentifierComponents = new Dictionary<string, string>
                {
                    { "iss", issuer },
                    { "sub", subject }
                }
            };
        }

        private string GetAccessToken(HttpRequestMessage request)
        {
            WifAzureAccessTokenResponse response;
            try
            {
                response = _restRequester.Get<WifAzureAccessTokenResponse>(new RestRequestWrapper(request));
            }
            catch (Exception exception)
            {
                var realException = HttpUtil.UnpackAggregateException(exception);
                s_logger.Error($"Failed to get access token in workload_identity authentication for Azure: {realException.Message}");
                throw AttestationError($"Failed to get access token: {realException.Message}");
            }

            if (string.IsNullOrEmpty(response?.AccessToken))
            {
                s_logger.Error($"No access token found in the response in workload_identity authentication for Azure");
                throw AttestationError("No access token found");
            }

            return response.AccessToken;
        }

        private HttpRequestMessage PrepareRequest(string snowflakeEntraResource)
        {
            var entraResourceOrDefault = string.IsNullOrEmpty(snowflakeEntraResource) ? DefaultWorkloadIdentityEntraResource : snowflakeEntraResource;
            var headers = new Dictionary<string, string> { { "Metadata", "True" } };
            var urlWithoutQueryParams = $"{_metadataServiceHost}{MetadataServiceEndpoint}";
            var queryParams = $"api-version=2018-02-01&resource={HttpUtility.UrlEncode(entraResourceOrDefault)}";
            var identityEndpoint = _environmentOperations.GetEnvironmentVariable("IDENTITY_ENDPOINT");
            var identityHeader = _environmentOperations.GetEnvironmentVariable("IDENTITY_HEADER");
            var useAzureFunctions = !string.IsNullOrEmpty(identityEndpoint);
            if (useAzureFunctions)
            {
                if (string.IsNullOrEmpty(identityHeader))
                {
                    s_logger.Error("Managed identity is not enabled on this Azure function.");
                    throw AttestationError("Managed identity is not enabled on this Azure function.");
                }

                urlWithoutQueryParams = identityEndpoint;
                headers = new Dictionary<string, string> { { "X-IDENTITY-HEADER", identityHeader } };
                queryParams = $"api-version=2019-08-01&resource={HttpUtility.UrlEncode(entraResourceOrDefault)}";

                var clientId = _environmentOperations.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");
                if (!string.IsNullOrEmpty(clientId))
                {
                    queryParams += $"&client_id={HttpUtility.UrlEncode(clientId)}";
                }
            }
            var uri = new Uri(urlWithoutQueryParams + "?" + queryParams);
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            foreach (var keyValuePair in headers)
            {
                request.Headers.Add(keyValuePair.Key, keyValuePair.Value);
            }
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);
            return request;
        }
    }
}
