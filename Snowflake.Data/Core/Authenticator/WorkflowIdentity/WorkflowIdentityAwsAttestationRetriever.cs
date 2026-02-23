using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Xml.Linq;
using Amazon.Runtime;
using Newtonsoft.Json;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityAwsAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityAwsAttestationRetriever>();
        private static readonly TimeSpan s_defaultTimeout = TimeSpan.FromSeconds(30);

        private readonly EnvironmentOperations _environmentOperations;
        private readonly TimeProvider _timeProvider;
        private readonly AwsSdkWrapper _awsSdkWrapper;
        private readonly IRestRequester _restRequester;
        private readonly string _stsHost;

        private TimeSpan HttpTimeout { get; set; } = s_defaultTimeout;
        private TimeSpan RestTimeout { get; set; } = s_defaultTimeout;

        public WorkflowIdentityAwsAttestationRetriever(EnvironmentOperations environmentOperations, TimeProvider timeProvider, AwsSdkWrapper awsSdkWrapper, IRestRequester restRequester)
            : this(environmentOperations, timeProvider, awsSdkWrapper, restRequester, null)
        {
        }

        internal WorkflowIdentityAwsAttestationRetriever(EnvironmentOperations environmentOperations, TimeProvider timeProvider, AwsSdkWrapper awsSdkWrapper, IRestRequester restRequester, string stsHost)
        {
            _environmentOperations = environmentOperations;
            _timeProvider = timeProvider;
            _awsSdkWrapper = awsSdkWrapper;
            _restRequester = restRequester;
            _stsHost = stsHost;
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
                credentials = AssumeRoleChain(roleArns, region);
            }
            else
            {
                // Direct authentication: use the attached identity's credentials
                credentials = GetAwsCredentials();
            }

            var requestBuilder = CreateGetCallerIdentityRequest(region);
            var awsConfiguration = new AwsConfiguration
            {
                Region = region,
                Service = "sts",
                Credentials = credentials
            };
            var utcNow = _timeProvider.UtcNow();
            AwsSignature4Signer.AddTokenAndSignatureHeaders(requestBuilder, awsConfiguration, utcNow);

            var jsonRequest = JsonConvert.SerializeObject(requestBuilder);
            var base64EncodedJsonRequest = Convert.ToBase64String(Encoding.UTF8.GetBytes(jsonRequest));

            return new WorkloadIdentityAttestationData
            {
                Provider = AttestationProvider.AWS,
                Credential = base64EncodedJsonRequest,
                UserIdentifierComponents = new Dictionary<string, string>()
            };
        }

        /// <summary>
        /// Assumes a chain of IAM roles, starting with the attached identity's credentials.
        /// Each role in the chain assumes the next role until the final role is reached.
        /// </summary>
        /// <param name="roleArns">Array of IAM role ARNs to assume in sequence.</param>
        /// <param name="region">The AWS region.</param>
        /// <returns>Temporary credentials for the final role in the chain.</returns>
        private ImmutableCredentials AssumeRoleChain(string[] roleArns, string region)
        {
            var credentials = GetAwsCredentials();

            foreach (var roleArn in roleArns)
            {
                credentials = AssumeRole(roleArn, region, credentials);
            }

            return credentials;
        }

        /// <summary>
        /// Assumes the target IAM role using STS AssumeRole API and returns the temporary credentials.
        /// </summary>
        /// <param name="targetRoleArn">The ARN of the IAM role to assume (e.g., arn:aws:iam::123456789012:role/TargetRole)</param>
        /// <param name="region">The AWS region</param>
        /// <param name="credentials">The credentials to use for assuming the role.</param>
        /// <returns>Temporary credentials for the assumed role</returns>
        private ImmutableCredentials AssumeRole(string targetRoleArn, string region, ImmutableCredentials credentials)
        {
            try
            {
                // Build the AssumeRole request
                var stsHostName = GetStsHostName(region);
                var roleSessionName = $"snowflake-wif-{Guid.NewGuid():N}".Substring(0, 32);
                var queryParams = $"Action=AssumeRole&Version=2011-06-15&RoleArn={Uri.EscapeDataString(targetRoleArn)}&RoleSessionName={Uri.EscapeDataString(roleSessionName)}&DurationSeconds=3600";

                // Use mock STS host for testing if provided
                var baseUrl = string.IsNullOrEmpty(_stsHost) ? $"https://{stsHostName}" : _stsHost;
                var uri = new Uri($"{baseUrl}/?{queryParams}");

                var headers = new Dictionary<string, string>
                {
                    { "Host", stsHostName }
                };

                var attestationRequest = new AttestationRequest
                {
                    HttpMethod = HttpMethod.Post,
                    Uri = uri,
                    Headers = headers
                };

                // Sign the request with the credentials
                var awsConfiguration = new AwsConfiguration
                {
                    Region = region,
                    Service = "sts",
                    Credentials = credentials
                };
                var utcNow = _timeProvider.UtcNow();
                AwsSignature4Signer.AddTokenAndSignatureHeaders(attestationRequest, awsConfiguration, utcNow);

                // Make the HTTP request
                var request = new HttpRequestMessage(HttpMethod.Post, uri);
                foreach (var header in attestationRequest.Headers)
                {
                    request.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
                request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, HttpTimeout);
                request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, RestTimeout);

                string responseXml;
                using (var response = _restRequester.Get(new RestRequestWrapper(request)))
                {
                    responseXml = response.Content.ReadAsStringAsync().Result;
                }

                // Parse the XML response to extract credentials
                return ParseAssumeRoleResponse(responseXml, targetRoleArn);
            }
            catch (Exception exception) when (!(exception is Client.SnowflakeDbException))
            {
                var realException = HttpUtil.UnpackAggregateException(exception);
                s_logger.Error($"Failed to assume role {targetRoleArn}: {realException.Message}");
                throw AttestationError($"Failed to assume role {targetRoleArn}: {realException.Message}");
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
                XNamespace ns = "https://sts.amazonaws.com/doc/2011-06-15/";

                var credentials = doc.Root?.Element(ns + "AssumeRoleResult")?.Element(ns + "Credentials");
                if (credentials == null)
                {
                    throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: no credentials found");
                }

                var accessKeyId = credentials.Element(ns + "AccessKeyId")?.Value;
                var secretAccessKey = credentials.Element(ns + "SecretAccessKey")?.Value;
                var sessionToken = credentials.Element(ns + "SessionToken")?.Value;

                if (string.IsNullOrEmpty(accessKeyId) || string.IsNullOrEmpty(secretAccessKey))
                {
                    throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: missing access key or secret");
                }

                return new ImmutableCredentials(accessKeyId, secretAccessKey, sessionToken);
            }
            catch (Exception exception) when (!(exception is Client.SnowflakeDbException))
            {
                s_logger.Error($"Failed to parse AssumeRole response: {exception.Message}");
                throw AttestationError($"Failed to parse AssumeRole response for {targetRoleArn}: {exception.Message}");
            }
        }

        private AttestationRequest CreateGetCallerIdentityRequest(string region)
        {
            var stsHostName = GetStsHostName(region);
            var uri = new Uri($"https://{stsHostName}/?Action=GetCallerIdentity&Version=2011-06-15");
            var headers = new Dictionary<string, string>
            {
                { "Host",  stsHostName },
                { "X-Snowflake-Audience", SnowflakeAudience }
            };
            return new AttestationRequest
            {
                HttpMethod = HttpMethod.Post,
                Uri = uri,
                Headers = headers
            };
        }

        internal static string GetStsHostName(string region)
        {
            var domain = region.StartsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
            return $"sts.{region}.{domain}";
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
