using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;
using RichardSzalay.MockHttp;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public class PlatformDetectionTest
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

        [TearDown]
        public void TearDown()
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

        [Test]
        public void TestDetectAwsLambdaWhenEnvVarSet()
        {
            Environment.SetEnvironmentVariable(LambdaTaskRoot, "/var/task");
            Assert.IsTrue(PlatformDetection.DetectAwsLambda());
        }

        [Test]
        public void TestDetectAwsLambdaWhenEnvVarNotSet()
        {
            Assert.IsFalse(PlatformDetection.DetectAwsLambda());
        }

        // --- DetectGithubActions ---

        [Test]
        public void TestDetectGithubActionsWhenEnvVarSet()
        {
            Environment.SetEnvironmentVariable(GithubActions, "true");
            Assert.IsTrue(PlatformDetection.DetectGithubActions());
        }

        [Test]
        public void TestDetectGithubActionsWhenEnvVarNotSet()
        {
            Assert.IsFalse(PlatformDetection.DetectGithubActions());
        }

        // --- DetectAzureFunction ---

        [Test]
        public void TestDetectAzureFunctionWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");
            Assert.IsTrue(PlatformDetection.DetectAzureFunction());
        }

        [Test]
        public void TestDetectAzureFunctionWhenOnlyOneEnvVarSet()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Assert.IsFalse(PlatformDetection.DetectAzureFunction());
        }

        [Test]
        public void TestDetectAzureFunctionWhenNoEnvVarsSet()
        {
            Assert.IsFalse(PlatformDetection.DetectAzureFunction());
        }

        // --- DetectGceCloudRunService ---

        [Test]
        public void TestDetectGceCloudRunServiceWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(KService, "my-service");
            Environment.SetEnvironmentVariable(KRevision, "my-service-00001-abc");
            Environment.SetEnvironmentVariable(KConfiguration, "my-service");
            Assert.IsTrue(PlatformDetection.DetectGceCloudRunService());
        }

        [Test]
        public void TestDetectGceCloudRunServiceWhenPartialEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(KService, "my-service");
            Assert.IsFalse(PlatformDetection.DetectGceCloudRunService());
        }

        // --- DetectGceCloudRunJob ---

        [Test]
        public void TestDetectGceCloudRunJobWhenAllEnvVarsSet()
        {
            Environment.SetEnvironmentVariable(CloudRunJob, "my-job");
            Environment.SetEnvironmentVariable(CloudRunExecution, "my-job-execution-abc");
            Assert.IsTrue(PlatformDetection.DetectGceCloudRunJob());
        }

        [Test]
        public void TestDetectGceCloudRunJobWhenOnlyOneEnvVarSet()
        {
            Environment.SetEnvironmentVariable(CloudRunJob, "my-job");
            Assert.IsFalse(PlatformDetection.DetectGceCloudRunJob());
        }

        // --- DetectEc2Instance ---

        [Test]
        public async Task TestDetectEc2InstanceWhenBothRequestsSucceed()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.OK, "text/plain", "fake-imds-token");
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/latest/dynamic/instance-identity/document")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.AreEqual(true, await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectEc2InstanceWhenTokenRequestFails()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.Forbidden);

            Assert.AreEqual(false, await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectEc2InstanceWhenDocumentRequestFails()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Respond(HttpStatusCode.OK, "text/plain", "fake-imds-token");
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/latest/dynamic/instance-identity/document")
                .Respond(HttpStatusCode.NotFound);

            Assert.AreEqual(false, await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectEc2InstanceReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Put, "http://169.254.169.254/latest/api/token")
                .Throw(new HttpRequestException("Connection refused"));

            Assert.AreEqual(false, await PlatformDetection.DetectEc2InstanceAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectAzureVm ---

        [Test]
        public async Task TestDetectAzureVmWhenMetadataEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.AreEqual(true, await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectAzureVmWhenMetadataEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Respond(HttpStatusCode.NotFound);

            Assert.AreEqual(false, await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectAzureVmReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/instance?api-version=2021-02-01")
                .Throw(new HttpRequestException("Connection refused"));

            Assert.AreEqual(false, await PlatformDetection.DetectAzureVmAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectAzureManagedIdentity ---

        [Test]
        public async Task TestDetectAzureManagedIdentityOnFunctionWithIdentityHeader()
        {
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");
            Environment.SetEnvironmentVariable(IdentityHeader, "some-value");

            Assert.AreEqual(true, await PlatformDetection.DetectAzureManagedIdentityAsync());
        }

        [Test]
        public async Task TestDetectAzureManagedIdentityOnFunctionWithoutIdentityHeader()
        {
            // Azure Function without IDENTITY_HEADER: NOT detected, no HTTP fallback
            Environment.SetEnvironmentVariable(FunctionsWorkerRuntime, "dotnet");
            Environment.SetEnvironmentVariable(FunctionsExtensionVersion, "~4");
            Environment.SetEnvironmentVariable(AzureWebJobsStorage, "DefaultEndpointsProtocol=https;...");

            Assert.AreEqual(false, await PlatformDetection.DetectAzureManagedIdentityAsync());
        }

        [Test]
        public async Task TestDetectAzureManagedIdentityOnVmWhenEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/identity/oauth2/token*")
                .Respond(HttpStatusCode.OK, "application/json", "{}");

            Assert.AreEqual(true, await PlatformDetection.DetectAzureManagedIdentityAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectAzureManagedIdentityOnVmWhenEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://169.254.169.254/metadata/identity/oauth2/token*")
                .Respond(HttpStatusCode.BadRequest);

            Assert.AreEqual(false, await PlatformDetection.DetectAzureManagedIdentityAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectGceVm ---

        [Test]
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

            Assert.AreEqual(true, await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectGceVmWhenMetadataFlavorIsMissing()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal")
                .Respond(HttpStatusCode.OK);

            Assert.AreEqual(false, await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectGceVmReturnsFalseOnHttpException()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal")
                .Throw(new HttpRequestException("Host not found"));

            Assert.AreEqual(false, await PlatformDetection.DetectGceVmAsync(mockHttp.ToHttpClient()));
        }

        // --- DetectGcpIdentity ---

        [Test]
        public async Task TestDetectGcpIdentityWhenEndpointReturns200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email")
                .Respond(HttpStatusCode.OK, "text/plain", "sa@project.iam.gserviceaccount.com");

            Assert.AreEqual(true, await PlatformDetection.DetectGcpIdentityAsync(mockHttp.ToHttpClient()));
        }

        [Test]
        public async Task TestDetectGcpIdentityWhenEndpointReturnsNon200()
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Get, "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email")
                .Respond(HttpStatusCode.NotFound);

            Assert.AreEqual(false, await PlatformDetection.DetectGcpIdentityAsync(mockHttp.ToHttpClient()));
        }
    }
}
