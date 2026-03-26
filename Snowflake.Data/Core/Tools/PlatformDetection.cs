using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Tools
{
    internal static class PlatformDetection
    {
        private sealed class PlatformDetectionLogger { }
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<PlatformDetectionLogger>();

        internal const string DisableEnvVar = "SNOWFLAKE_DISABLE_PLATFORM_DETECTION";
        internal const int DetectionTimeoutMs = 200;

        // Internal for testing - allows overriding base URLs
        internal static string AzureMetadataBaseUrl = "http://169.254.169.254";
        internal static string GceMetadataRootUrl = "http://metadata.google.internal";
        internal static string GcpMetadataBaseUrl = "http://metadata.google.internal/computeMetadata/v1";

        private static readonly HttpClient s_httpClient = new HttpClient(
            new HttpClientHandler
            {
                UseProxy = false,
                AllowAutoRedirect = false
            })
        {
            Timeout = Timeout.InfiniteTimeSpan
        };

        // Background task - starts once at class initialization, result cached for process lifetime
        private static readonly Task<string[]> s_detectionTask = Task.Run(RunDetectionAsync);

        /// <summary>
        /// Ensures platform detection has started. Safe to call multiple times.
        /// </summary>
        internal static void EnsureStarted()
        {
            // Calling this method triggers class initialization, starting s_detectionTask.
        }

        /// <summary>
        /// Returns detected platform names. Blocks until detection is complete (at most DetectionTimeoutMs).
        /// Returns null when no platforms are detected (JSON field omitted).
        /// Returns ["disabled"] when disabled via SNOWFLAKE_DISABLE_PLATFORM_DETECTION=true.
        /// Timed-out detectors appear with a "_timeout" suffix for observability.
        /// </summary>
        internal static string[] GetDetectedPlatforms()
        {
            try
            {
                var result = s_detectionTask.GetAwaiter().GetResult();
                return result != null && result.Length > 0 ? result : null;
            }
            catch
            {
                return null;
            }
        }

        private static async Task<string[]> RunDetectionAsync()
        {
            if (string.Equals(Environment.GetEnvironmentVariable(DisableEnvVar), "true", StringComparison.OrdinalIgnoreCase))
            {
                s_logger.Debug("Platform detection disabled via " + DisableEnvVar);
                return new[] { "disabled" };
            }

            var names = new[]
            {
                "is_aws_lambda",
                "is_azure_function",
                "is_gce_cloud_run_service",
                "is_gce_cloud_run_job",
                "is_github_action",
                "is_ec2_instance",
                "is_azure_vm",
                "has_azure_managed_identity",
                "is_gce_vm",
                "has_gcp_identity",
            };

            // Start all detectors concurrently.
            // Return value: true = detected, false = not detected, null = timed out
            var tasks = new Task<bool?>[]
            {
                Task.FromResult<bool?>(DetectAwsLambda()),
                Task.FromResult<bool?>(DetectAzureFunction()),
                Task.FromResult<bool?>(DetectGceCloudRunService()),
                Task.FromResult<bool?>(DetectGceCloudRunJob()),
                Task.FromResult<bool?>(DetectGithubActions()),
                DetectEc2InstanceAsync(),
                DetectAzureVmAsync(),
                DetectAzureManagedIdentityAsync(),
                DetectGceVmAsync(),
                DetectGcpIdentityAsync(),
            };

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var detected = new List<string>();
            for (int i = 0; i < names.Length; i++)
            {
                var result = tasks[i].Status == TaskStatus.RanToCompletion ? tasks[i].Result : false;
                if (result == true)
                    detected.Add(names[i]);
                else if (result == null)
                    detected.Add(names[i] + "_timeout");
            }

            s_logger.Debug("Platform detection completed. Detected: [" + string.Join(", ", detected) + "]");
            return detected.ToArray();
        }

        // --- Environment variable detectors ---

        internal static bool DetectAwsLambda() =>
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("LAMBDA_TASK_ROOT"));

        internal static bool DetectGithubActions() =>
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"));

        internal static bool DetectAzureFunction() =>
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FUNCTIONS_WORKER_RUNTIME")) &&
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FUNCTIONS_EXTENSION_VERSION")) &&
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));

        internal static bool DetectGceCloudRunService() =>
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("K_SERVICE")) &&
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("K_REVISION")) &&
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("K_CONFIGURATION"));

        internal static bool DetectGceCloudRunJob() =>
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CLOUD_RUN_JOB")) &&
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CLOUD_RUN_EXECUTION"));

        // --- HTTP-based detectors ---
        // Return value: true = detected, false = not detected, null = timed out

        internal static async Task<bool?> DetectEc2InstanceAsync(HttpClient httpClient = null)
        {
            using var cts = new CancellationTokenSource(DetectionTimeoutMs);
            var client = httpClient ?? s_httpClient;
            try
            {
                // IMDSv2: get session token first, then query instance identity document
                using var tokenReq = new HttpRequestMessage(HttpMethod.Put, "http://169.254.169.254/latest/api/token");
                tokenReq.Headers.Add("X-aws-ec2-metadata-token-ttl-seconds", "21600");
                using var tokenResp = await client.SendAsync(tokenReq, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                if (!tokenResp.IsSuccessStatusCode)
                    return false;
                var token = await tokenResp.Content.ReadAsStringAsync().ConfigureAwait(false);

                using var docReq = new HttpRequestMessage(HttpMethod.Get, "http://169.254.169.254/latest/dynamic/instance-identity/document");
                docReq.Headers.Add("X-aws-ec2-metadata-token", token);
                using var docResp = await client.SendAsync(docReq, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                return docResp.StatusCode == HttpStatusCode.OK ? (bool?)true : false;
            }
            catch (TaskCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch
            {
                return false;
            }
        }

        internal static async Task<bool?> DetectAzureVmAsync(HttpClient httpClient = null)
        {
            using var cts = new CancellationTokenSource(DetectionTimeoutMs);
            var client = httpClient ?? s_httpClient;
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, AzureMetadataBaseUrl + "/metadata/instance?api-version=2021-02-01");
                req.Headers.Add("Metadata", "true");
                using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                return resp.StatusCode == HttpStatusCode.OK ? (bool?)true : false;
            }
            catch (TaskCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch
            {
                return false;
            }
        }

        internal static async Task<bool?> DetectAzureManagedIdentityAsync(HttpClient httpClient = null)
        {
            // On Azure Functions: managed identity is indicated solely by IDENTITY_HEADER.
            // Do not fall through to the VM IMDS endpoint - it is not applicable for Functions.
            if (DetectAzureFunction())
                return !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("IDENTITY_HEADER"));

            using var cts = new CancellationTokenSource(DetectionTimeoutMs);
            var client = httpClient ?? s_httpClient;
            try
            {
                var url = AzureMetadataBaseUrl + "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fmanagement.azure.com";
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Add("Metadata", "true");
                using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                return resp.StatusCode == HttpStatusCode.OK ? (bool?)true : false;
            }
            catch (TaskCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch
            {
                return false;
            }
        }

        internal static async Task<bool?> DetectGceVmAsync(HttpClient httpClient = null)
        {
            using var cts = new CancellationTokenSource(DetectionTimeoutMs);
            var client = httpClient ?? s_httpClient;
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, GceMetadataRootUrl);
                using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                return resp.Headers.TryGetValues("Metadata-Flavor", out var values) &&
                       string.Join(",", values).Contains("Google") ? (bool?)true : false;
            }
            catch (TaskCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch
            {
                return false;
            }
        }

        internal static async Task<bool?> DetectGcpIdentityAsync(HttpClient httpClient = null)
        {
            using var cts = new CancellationTokenSource(DetectionTimeoutMs);
            var client = httpClient ?? s_httpClient;
            try
            {
                var url = GcpMetadataBaseUrl + "/instance/service-accounts/default/email";
                using var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.Add("Metadata-Flavor", "Google");
                using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                return resp.StatusCode == HttpStatusCode.OK ? (bool?)true : false;
            }
            catch (TaskCanceledException) when (cts.IsCancellationRequested)
            {
                return null;
            }
            catch
            {
                return false;
            }
        }
    }
}
