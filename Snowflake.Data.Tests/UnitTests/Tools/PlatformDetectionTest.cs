using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;
using RichardSzalay.MockHttp;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [CollectionDefinition(nameof(PlatformDetectionTestFixture), DisableParallelization = true)]
    public sealed class PlatformDetectionTestFixture { }

    [Collection(nameof(PlatformDetectionTestFixture))]
    public class PlatformDetectionTest : IDisposable
    {
        private const string LambdaTaskRoot = "LAMBDA_TASK_ROOT";
        private const string GithubActions = "GITHUB_ACTIONS";
        private const string FunctionsWorkerRuntime = "FUNCTIONS_WORKER_RUNTIME";
        private const string FunctionsExtensionVersion = "FUNCTIONS_EXTENSION_VERSION";
        private const string AzureWebJobsStorage = "AzureWebJobsStorage";
        private const string IdentityHeader = "IDENTITY_HEADER";
        private const string KService = "K_SERVICE";
        private const string KRevision = "K_REVISION";
        private const string KConfiguration = "K_CONFIGURATION";
        private const string CloudRunJob = "CLOUD_RUN_JOB";
        private const string CloudRunExecution = "CLOUD_RUN_EXECUTION";

        private void TearDown()
        {
            Environment.SetEnvironmentVariable(LambdaTaskRoot, null);
            Environment.SetEnvironmentVariable(GithubActions, null);
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, null);
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, null);
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, null);
            Environment.SetEnvironmentVariable(IdentityHeader, null);
            Environment.SetEnvironmentVariable(KService, null);
            Environment.SetEnvironmentVariable(KRevision, null);
            Environment.SetEnvironmentVariable(KConfiguration, null);
            Environment.SetEnvironmentVariable(CloudRunJob, null);
            Environment.SetEnvironmentVariable(CloudRunExecution, null);
            Environment.SetEnvironmentVariable(PlatformDetection.DisableEnvVar, null);
        }

        // --- DetectAwsLambda ---

        [SFFact]
        public void TestDetectAwsLambdaWhenEnvVarSet()
        {
            Environment.SetEnvironmentVariable(LambdaTaskRoot, "/var/task");
            Assert.True(PlatformDetection.DetectAwsLambda());
        }

        [SFFact]
        public void TestDetectAwsLambdaWhenEnvVarNotSet()
        {
            Assert.False(PlatformDetection.DetectAwsLambda());
        }

        // --- DetectGithubActions ---

        [SFFact]
        public void TestDetectGithubActionsWhenEnvVarSet()
        {
            Environment.SetEnvironmentVariable(GithubActions, "true");
            Assert.True(PlatformDetection.DetectGithubActions());
        }

        [SFFact]
        public void TestDetectGithubActionsWhenEnvVarNotSet()
        {
            Assert.False(PlatformDetection.DetectGithubActions());
        }

        // --- DetectAzureFunction ---

        [SFFact]
        public void TestDetectAzureFunctionWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");
            Assert.True(PlatformDetection.DetectAzureFunction());
        }

        [SFFact]
        public void TestDetectAzureFunctionWhenOnlyOneEnvVarSet()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Assert.False(PlatformDetection.DetectAzureFunction());
        }

        [SFFact]
        public void TestDetectAzureFunctionWhenNoEnvVarsSet()
        {
            Assert.False(PlatformDetection.DetectAzureFunction());
        }

        // --- DetectGceCloudRunService ---

        [SFFact]
        public void TestDetectGceCloudRunServiceWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(KService, "my-service");
            Environment.SetEnvironmentVariable(KRevision, "my-service-00001-abc");
            Environment.SetEnvironmentVariable(KConfiguration, "my-service");
            Assert.True(PlatformDetection.DetectGceCloudRunService());
        }

        [SFFact]
        public void TestDetectGceCloudRunServiceWhenPartialEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(KService, "my-service");
            Assert.False(PlatformDetection.DetectGceCloudRunService());
        }

        // --- DetectGceCloudRunJob ---

        [SFFact]
        public void TestDetectGceCloudRunJobWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(CloudRunJob, "my-job");
            Environment.SetEnvironmentVariable(CloudRunExecution, "my-job-execution-abc");
            Assert.True(PlatformDetection.DetectGceCloudRunJob());
        }

        [SFFact]
        public void TestDetectGceCloudRunJobWhenOnlyOneEnvVarSet()
        {
            Environment.SetEnvironmentVariable(CloudRunJob, "my-job");
            Assert.False(PlatformDetection.DetectGceCloudRunJob());
        }

        // --- DetectEc2Instance ---

        [SFFact]
        public async Task TestDetectEc2InstanceWhenBothRequestsSucceed()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.OK, "text/plain", "fake-imds-token");
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/latest/dynamic/instance-identity/document")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.True(await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectEc2InstanceWhenTokenRequestFails()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.Forbidden);

            Assert.False(await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectEc2InstanceWhenDocumentRequestFails()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.OK, "text/plain", "fake-imds-token");
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/latest/dynamic/instance-identity/document")
                .Respond(HttpStatusCode.NotFound);

            Assert.False(await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectEc2InstanceReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Throw(new HttpRequestException("Connection refused"));

            Assert.False(await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectAzureVm ---

        [SFFact]
        public async Task TestDetectAzureVmWhenMetadataEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.True(await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectAzureVmWhenMetadataEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Respond(HttpStatusCode.NotFound);

            Assert.False(await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectAzureVmReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Throw(new HttpRequestException("Connection refused"));

            Assert.False(await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectAzureManagedIdentity ---

        [SFFact]
        public async Task TestDetectAzureManagedIdentityOnFunctionWithIdentityHeader()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");
            Environment.SetEnvironmentVariable(IdentityHeader, "some-value");

            Assert.True(await PlatformDetection.DetectAzureManagedIdentityAsync());
        }

        [SFFact]
        public async Task TestDetectAzureManagedIdentityOnFunctionWithoutIdentityHeader()
        {
            // Azure Function without IDENTITY_HEADER: NOT detected, no HTTP fallback
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");

            Assert.False(await PlatformDetection.DetectAzureManagedIdentityAsync());
        }

        [SFFact]
        public async Task TestDetectAzureManagedIdentityOnVmWhenEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/identity/oauth2/token*")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.True(await PlatformDetection.DetectAzureManagedIdentityAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectAzureManagedIdentityOnVmWhenEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/identity/oauth2/token*")
                .Respond(HttpStatusCode.BadRequest);

            Assert.False(await PlatformDetection.DetectAzureManagedIdentityAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectGceVm ---

        [SFFact]
        public async Task TestDetectGceVmWhenMetadataFlavorIsGoogle()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal")
                .Respond(r =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.OK);
                    response.Headers.Add("Metadata-Flavor", "Google");
                    return response;
                });

            Assert.True(await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectGceVmWhenMetadataFlavorIsMissing()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal")
                .Respond(HttpStatusCode.OK);

            Assert.False(await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectGceVmReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal")
                .Throw(new HttpRequestException("Host not found"));

            Assert.False(await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        // --- RunDetectionAsync (disable env var) ---

        [SFFact]
        public async Task TestRunDetectionAsyncReturnsDisabledWhenEnvVarSetToTrue()
        {
            Environment.SetEnvironmentVariable(PlatformDetection.DisableEnvVar, "true");
            var result = await PlatformDetection.RunDetectionAsync();
            Assert.Equal(new[] { "disabled" }, result);
        }

        [SFFact]
        public async Task TestRunDetectionAsyncReturnsDisabledWhenEnvVarSetCaseInsensitive()
        {
            Environment.SetEnvironmentVariable(PlatformDetection.DisableEnvVar, "TRUE");
            var result = await PlatformDetection.RunDetectionAsync();
            Assert.Equal(new[] { "disabled" }, result);
        }

        // --- AggregateResults ---

        [SFFact]
        public void TestAggregateResultsIncludesNameWhenTrue()
        {
            var names = new[] { "platform_a", "platform_b" };
            var results = new bool?[] { true, false };
            var aggregated = PlatformDetection.AggregateResults(names, results);
            Assert.Equal(new[] { "platform_a" }, aggregated);
        }

        [SFFact]
        public void TestAggregateResultsAddsTimeoutSuffixWhenNull()
        {
            var names = new[] { "platform_a", "platform_b" };
            var results = new bool?[] { null, false };
            var aggregated = PlatformDetection.AggregateResults(names, results);
            Assert.Equal(new[] { "platform_a_timeout" }, aggregated);
        }

        [SFFact]
        public void TestAggregateResultsSkipsFalseResults()
        {
            var names = new[] { "platform_a", "platform_b", "platform_c" };
            var results = new bool?[] { false, false, false };
            var aggregated = PlatformDetection.AggregateResults(names, results);
            Assert.Empty(aggregated);
        }

        [SFFact]
        public void TestAggregateResultsMixedDetectedAndTimeout()
        {
            var names = new[] { "platform_a", "platform_b", "platform_c" };
            var results = new bool?[] { true, null, false };
            var aggregated = PlatformDetection.AggregateResults(names, results);
            Assert.Equal(new[] { "platform_a", "platform_b_timeout" }, aggregated);
        }

        // --- DetectGcpIdentity ---

        [SFFact]
        public async Task TestDetectGcpIdentityWhenEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email")
                .Respond(HttpStatusCode.OK, "text/plain", "sa@project.iam.gserviceaccount.com");

            Assert.True(await PlatformDetection.DetectGcpIdentityAsync(mockHttp.ToHttpClient()));
        }

        [SFFact]
        public async Task TestDetectGcpIdentityWhenEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email")
                .Respond(HttpStatusCode.NotFound);

            Assert.False(await PlatformDetection.DetectGcpIdentityAsync(mockHttp.ToHttpClient()));
        }

        public void Dispose()
        {
            TearDown();
        }
    }
}
