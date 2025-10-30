using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Amazon.Runtime;
using Newtonsoft.Json;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Core.Authenticator.WorkflowIdentity
{
    internal class WorkflowIdentityAwsAttestationRetriever : WorkloadIdentityAttestationRetriever
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WorkflowIdentityAwsAttestationRetriever>();

        private readonly EnvironmentOperations _environmentOperations;
        private readonly TimeProvider _timeProvider;
        private readonly AwsSdkWrapper _awsSdkWrapper;

        public WorkflowIdentityAwsAttestationRetriever(EnvironmentOperations environmentOperations, TimeProvider timeProvider, AwsSdkWrapper awsSdkWrapper)
        {
            _environmentOperations = environmentOperations;
            _timeProvider = timeProvider;
            _awsSdkWrapper = awsSdkWrapper;
        }

        public override AttestationProvider GetAttestationProvider() => AttestationProvider.AWS;

        public override WorkloadIdentityAttestationData CreateAttestationData(string snowflakeEntraResource, string token)
        {
            var credentials = GetAwsCredentials();
            var region = GetAwsRegion();
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
