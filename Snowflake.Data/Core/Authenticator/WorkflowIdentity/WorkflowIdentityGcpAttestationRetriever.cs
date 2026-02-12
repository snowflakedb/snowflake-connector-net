using System;
using System.Collections.Generic;
using System.Net.Http;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityGcpAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityGcpAttestationRetriever>();
        private const string DefaultMetadataServiceHost = "http://169.254.169.254";
        private const string MetadataEndpoint = "/computeMetadata/v1/instance/service-accounts/default/identity";

        private static readonly TimeSpan s_defaultTimeout = TimeSpan.FromSeconds(30);

        private TimeSpan HttpTimeout { get; set; } = s_defaultTimeout;

        private TimeSpan RestTimeout { get; set; } = s_defaultTimeout;

        private readonly IRestRequester _restRequester;
        private readonly string _metadataServiceHost;

        public WorkflowIdentityGcpAttestationRetriever(IRestRequester restRequester, string metadataServiceHost)
        {
            _restRequester = restRequester;
            _metadataServiceHost = metadataServiceHost ?? DefaultMetadataServiceHost;
        }

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.GCP;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string tokenParam)
        {
            var request = PrepareRequest();
            var response = GetIdentityResponse(request);
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

        private HttpRequestMessage PrepareRequest()
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
}
