using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Amazon.Runtime;
using Newtonsoft.Json;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityAwsAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private const string AmazonApiVersion = "2011-06-15";
        private static readonly XNamespace s_amazonStsNamespace = $"https://sts.amazonaws.com/doc/{AmazonApiVersion}/";
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityAwsAttestationRetriever>();
        private static readonly TimeSpan s_defaultTimeout = TimeSpan.FromSeconds(30);

        private readonly TimeProvider _timeProvider;
        private readonly AwsSdkWrapper _awsSdkWrapper;
        private readonly IRestRequester _restRequester;
        private readonly string _stsHost;
        private readonly IEnvironmentFacade _environmentFacade;

        internal WorkflowIdentityAwsAttestationRetriever(IEnvironmentFacade environmentFacade, TimeProvider timeProvider, AwsSdkWrapper awsSdkWrapper, IRestRequester restRequester, string stsHost)
        {
            _timeProvider = timeProvider;
            _awsSdkWrapper = awsSdkWrapper;
            _restRequester = restRequester;
            _stsHost = stsHost;
            _environmentFacade = environmentFacade;
        }

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.AWS;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string token, string impersonationPath = null)
        {
            ImmutableCredentials credentials;
            var region = GetAwsRegion();

            if (!string.IsNullOrEmpty(impersonationPath))
            {
                // Transitive impersonation: assume roles in chain
                // The impersonation path is a comma-separated list of role ARNs
                var roleArns = impersonationPath.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(r => r.Trim()).ToArray();

                s_logger.Debug($"Using transitive role assumption through chain: {string.Join(" -> ", roleArns)}");
                credentials = AssumeRoleChainAsync(roleArns, region, CancellationToken.None).GetAwaiter().GetResult();
            }
            else
            {
                // Direct authentication: use the attached identity's credentials
                credentials = GetAwsCredentials();
            }

            var useOutboundToken = _environmentFacade.GetBool(EnvVars.EnableAwsWifOutboundToken);

            var credential = useOutboundToken
                ? GetWebIdentityTokenAsync(region, credentials, CancellationToken.None).GetAwaiter().GetResult()
                : CreateSignedGetCallerIdentityRequest(region, credentials);

            return new WorkloadIdentityAttestationData
            {
                Provider = AttestationProvider.AWS,
                Credential = credential,
                UserIdentifierComponents = new Dictionary<string, string>()
            };
        }

        /// <summary>
        /// Assumes a chain of IAM roles, starting with the attached identity's credentials.
        /// Each role in the chain assumes the next role until the final role is reached.
        /// </summary>
        /// <param name="roleArns">Array of IAM role ARNs to assume in sequence.</param>
        /// <param name="region">The AWS region.</param>
        /// <param name="cancellationToken">Cancellation support.</param>
        /// <returns>Temporary credentials for the final role in the chain.</returns>
        private async Task<ImmutableCredentials> AssumeRoleChainAsync(string[] roleArns, string region, CancellationToken cancellationToken)
        {
            var credentials = GetAwsCredentials();

            foreach (var roleArn in roleArns)
            {
                credentials = await AssumeRoleAsync(roleArn, region, credentials, cancellationToken).ConfigureAwait(false);
            }

            return credentials;
        }

        /// <summary>
        /// Assumes the target IAM role using STS AssumeRole API and returns the temporary credentials.
        /// </summary>
        /// <param name="targetRoleArn">The ARN of the IAM role to assume (e.g., arn:aws:iam::123456789012:role/TargetRole)</param>
        /// <param name="region">The AWS region</param>
        /// <param name="credentials">The credentials to use for assuming the role.</param>
        /// <param name="cancellationToken">Cancellation support.</param>
        /// <returns>Temporary credentials for the assumed role</returns>
        private async Task<ImmutableCredentials> AssumeRoleAsync(string targetRoleArn, string region, ImmutableCredentials credentials, CancellationToken cancellationToken)
        {
            try
            {
                // Build the AssumeRole request
                var roleSessionName = $"snowflake-wif-{Guid.NewGuid():N}".Substring(0, 32);
                var queryParams = $"Action=AssumeRole&Version={AmazonApiVersion}&RoleArn={Uri.EscapeDataString(targetRoleArn)}&RoleSessionName={Uri.EscapeDataString(roleSessionName)}&DurationSeconds=3600";
                var audienceHeader = new KeyValuePair<string, string>("X-Snowflake-Audience", SnowflakeAudience);
                using var request = BuildStsRequest(region, queryParams, credentials, audienceHeader);
                using var response = await _restRequester.GetAsync(new RestRequestWrapper(request), cancellationToken).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                    throw new WebException($"Failed to call AssumeRole: {response.StatusCode}");

                var responseXml = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                // Parse the XML response to extract credentials
                return ParseAssumeRoleResponse(responseXml, targetRoleArn);
            }
            catch (Exception exception) when (exception is not Client.SnowflakeDbException)
            {
                s_logger.Error($"Failed to assume role {targetRoleArn}: {exception.Message}");
                throw AttestationError($"Failed to assume role {targetRoleArn}: {exception.Message}");
            }
        }

        private string CreateSignedGetCallerIdentityRequest(string region, ImmutableCredentials credentials)
        {
            var domain = region.StartsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
            var stsHostName = $"sts.{region}.{domain}";
            var uri = new Uri($"https://{stsHostName}/?Action=GetCallerIdentity&Version={AmazonApiVersion}");
            var headers = new Dictionary<string, string>
            {
                { "Host", stsHostName },
                { "X-Snowflake-Audience", SnowflakeAudience }
            };
            var requestBuilder = new AttestationRequest
            {
                HttpMethod = HttpMethod.Post,
                Uri = uri,
                Headers = headers
            };

            var awsConfiguration = new AwsConfiguration
            {
                Region = region,
                Service = "sts",
                Credentials = credentials
            };
            var utcNow = _timeProvider.UtcNow();
            AwsSignature4Signer.AddTokenAndSignatureHeaders(requestBuilder, awsConfiguration, utcNow);

            var jsonRequest = JsonConvert.SerializeObject(requestBuilder);
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(jsonRequest));
        }

        internal async Task<string> GetWebIdentityTokenAsync(string region, ImmutableCredentials credentials, CancellationToken cancellationToken)
        {
            try
            {
                var snowflakeAudience = Uri.EscapeDataString(SnowflakeAudience);
                var queryParams = $"Action=GetWebIdentityToken&Version={AmazonApiVersion}&Audience.member.1={snowflakeAudience}&SigningAlgorithm=ES384";
                using var request = BuildStsRequest(region, queryParams, credentials);
                using var response = await _restRequester.GetAsync(new RestRequestWrapper(request), cancellationToken).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                    throw new WebException($"Failed to call GetWebIdentityToken: {response.StatusCode}");

                var responseXml = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                return ParseGetWebIdentityTokenResponse(responseXml);
            }
            catch (Exception exception) when (exception is not Client.SnowflakeDbException)
            {
                s_logger.Error($"Failed to call AWS STS GetWebIdentityToken: {exception.Message}");
                throw AttestationError($"Failed to call AWS STS GetWebIdentityToken: {exception.Message}");
            }
        }

        /// <summary>
        /// Parses the AssumeRole XML response to extract temporary credentials.
        /// </summary>
        private ImmutableCredentials ParseAssumeRoleResponse(string responseXml, string targetRoleArn)
        {
            try
            {
                var doc = XDocument.Parse(responseXml);

                var credentials = doc.Root?.Element(s_amazonStsNamespace + "AssumeRoleResult")?.Element(s_amazonStsNamespace + "Credentials");
                if (credentials == null)
                {
                    throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: no credentials found");
                }

                var accessKeyId = credentials.Element(s_amazonStsNamespace + "AccessKeyId")?.Value;
                var secretAccessKey = credentials.Element(s_amazonStsNamespace + "SecretAccessKey")?.Value;
                var sessionToken = credentials.Element(s_amazonStsNamespace + "SessionToken")?.Value;

                if (string.IsNullOrEmpty(accessKeyId) || string.IsNullOrEmpty(secretAccessKey))
                    throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: missing access key or secret");

                return new ImmutableCredentials(accessKeyId, secretAccessKey, sessionToken);
            }
            catch (Exception exception) when (exception is not Client.SnowflakeDbException)
            {
                s_logger.Error($"Failed to parse AssumeRole response: {exception.Message}");
                throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: {exception.Message}");
            }
        }

        /// <summary>
        /// Parses the GetWebIdentityToken XML response to extract jwt token.
        /// </summary>
        private string ParseGetWebIdentityTokenResponse(string responseXml)
        {
            var doc = XDocument.Parse(responseXml);

            var token = doc.Root?.Element(s_amazonStsNamespace + "GetWebIdentityTokenResult")?.Element(s_amazonStsNamespace + "WebIdentityToken")?.Value;

            if (string.IsNullOrEmpty(token))
                throw AttestationError("AWS STS GetWebIdentityToken returned an empty token");

            return token;
        }

        internal HttpRequestMessage BuildStsRequest(string region, string queryParams, ImmutableCredentials credentials, params KeyValuePair<string, string>[] additionalHeaders)
        {
            var domain = region.StartsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
            var stsHostName = $"sts.{region}.{domain}";
            var baseUrl = string.IsNullOrEmpty(_stsHost) ? $"https://{stsHostName}" : _stsHost;
            var uri = new Uri($"{baseUrl}/?{queryParams}");

            var headers = additionalHeaders
                .Concat([new("Host", stsHostName)])
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            var attestationRequest = new AttestationRequest
            {
                HttpMethod = HttpMethod.Post,
                Uri = uri,
                Headers = headers
            };

            var awsConfiguration = new AwsConfiguration
            {
                Region = region,
                Service = "sts",
                Credentials = credentials
            };
            var utcNow = _timeProvider.UtcNow();
            AwsSignature4Signer.AddTokenAndSignatureHeaders(attestationRequest, awsConfiguration, utcNow);

            var request = new HttpRequestMessage(HttpMethod.Post, uri);
            foreach (var header in attestationRequest.Headers)
            {
                request.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, s_defaultTimeout);
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, s_defaultTimeout);

            return request;
        }

        private ImmutableCredentials GetAwsCredentials()
        {
            var awsCredentials = _awsSdkWrapper.GetAwsCredentials();
            if (awsCredentials == null)
            {
                s_logger.Error("Could not find AWS credentials for workload_identity authentication");
                throw AttestationError("Could not find AWS credentials");
            }
            return awsCredentials;
        }

        private string GetAwsRegion()
        {
            var region = _awsSdkWrapper.GetAwsRegion();
            if (string.IsNullOrEmpty(region))
            {
                s_logger.Debug("Could not find AWS region for workload_identity authentication");
                throw AttestationError("Could not find AWS region");
            }
            return region;
        }
    }
}
