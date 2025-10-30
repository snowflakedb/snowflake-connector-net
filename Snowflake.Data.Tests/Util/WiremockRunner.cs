using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.Util
{
    internal class WiremockRunner : IDisposable
    {
        internal const int DefaultHttpsPort = 1443;
        internal const int DefaultHttpPort = 1080;
        private const int MaxRetries = 50;
        private const int RetryInterval = 200;
        private const int WarmupTime = 1000;
        private const string WiremockVersion = "3.11.0";
        private const string WiremockJarFileSha256 = "85f47eecd54ddf6aa275c9a3ceaf8e200cad30d26b529a706dd55e3bf3a4787e"; // pragma: allowlist secret
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WiremockRunner>();
        private static readonly string s_userProfilePath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        private static readonly string s_wiremockPath = Path.Combine(s_userProfilePath, ".m2", "repository", "org", "wiremock", "wiremock-standalone", WiremockVersion);
        private static readonly string s_wiremockJar = $"wiremock-standalone-{WiremockVersion}.jar";
        private static readonly string s_wiremockJarPath = Path.Combine(s_wiremockPath, s_wiremockJar);
        private static readonly string s_wiremockOptions = $"--root-dir {Path.Combine(s_userProfilePath, ".wiremock")} " +
                                                           "--enable-browser-proxying " +
                                                           "--proxy-pass-through false " +
                                                           "--https-keystore ./wiremock/ca-cert.jks " +
                                                           "--ca-keystore ./wiremock/ca-cert.jks";
        private static readonly string s_wiremockUrl =
            $"https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/{WiremockVersion}/wiremock-standalone-{WiremockVersion}.jar";
        private static readonly HttpClient s_httpClient = new();
        private static readonly object s_lock = new();

        internal const string Host = "127.0.0.1";
        private int HttpsPort { get; }
        private int HttpPort { get; }
        public bool IsAvailable { get; private set; }

        public string WiremockBaseHttpsUrl => $"https://{Host}:{HttpsPort}";
        public string WiremockBaseHttpUrl => $"http://{Host}:{HttpPort}";
        private Process _process;

        private WiremockRunner(int httpsPort, int httpPort)
        {
            HttpsPort = httpsPort;
            HttpPort = httpPort;
            IsAvailable = false;
        }

        ~WiremockRunner()
        {
            Stop();
        }

        public static WiremockRunner NewWiremock(string[] mappingFiles = null, int httpsPort = DefaultHttpsPort, int httpPort = DefaultHttpPort)
        {
            DownloadWiremockIfRequired();
            s_logger.Debug($"Starting Wiremock on host: {Host}, https port: {httpsPort}, http port: {httpPort}");
            var runner = new WiremockRunner(httpsPort, httpPort);
            runner.Start();

            s_logger.Debug("Waiting for Wiremock startup...");
            var retries = 0;
            while (retries < MaxRetries)
            {
                if (runner.CheckIfResponds())
                {
                    runner.IsAvailable = true;
                    s_logger.Debug($"Wiremock started on host: {Host}, https port: {httpsPort}, http port: {httpPort}");

                    if (mappingFiles != null)
                    {
                        foreach (var mappingFile in mappingFiles)
                        {
                            runner.AddMappings(mappingFile);
                        }
                    }

                    return runner;
                }
                retries++;
                Thread.Sleep(RetryInterval);
            }

            runner.Stop();
            throw new Exception("Unable to start Wiremock. Response check retries exceeded.");
        }

        public void Dispose()
        {
            Stop();
        }

        private bool CheckIfResponds()
        {
            var wiremockUri = new Uri(WiremockBaseHttpUrl + "/__admin/mappings");
            s_logger.Debug($"Checking if Wiremock responds on: {wiremockUri}");
            try
            {
                var response = Task.Run(async () => await s_httpClient.GetAsync(wiremockUri, CancellationToken.None)).Result;
                s_logger.Debug($"Wiremock responded with status code: {response.StatusCode}");

                return response.IsSuccessStatusCode;
            }
            catch (AggregateException)
            {
                return false;
            }
        }

        private void Start()
        {
            var javaArgs = $"-jar {Path.Combine(s_wiremockPath, s_wiremockJar)} --port {HttpPort} --https-port {HttpsPort} {s_wiremockOptions}";
            s_logger.Debug($"Running command: java {javaArgs}");
            try
            {
                _process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = "java",
                        Arguments = javaArgs,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                    }
                };
                _process.Start();

                // Let it warmup for a moment to catch any exception during startup
                Thread.Sleep(WarmupTime);
                if (_process is { HasExited: true })
                {
                    throw new Exception($"Process is not running. Output: {_process.StandardError.ReadToEnd()}");
                }
            }
            catch (Exception e)
            {
                s_logger.Error($"Unable to start Wiremock: {e.Message}");
                throw;
            }
        }

        public void Stop()
        {
            if (_process != null && !_process.HasExited)
            {
                try
                {
                    _process.Kill();
                }
                catch (InvalidOperationException)
                {
                    // Process already exited, ignore
                }
                _process = null;
            }
            IsAvailable = false;
        }

        public void ResetMapping()
        {
            var response = Task.Run(async () => await s_httpClient.PostAsync(WiremockBaseHttpUrl + "/__admin/reset", null)).Result;
            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Unable to reset Wiremock mappings. Response status code: {response.StatusCode}");
            }
        }

        public void AddMappings(string file, StringTransformations transformations = null)
        {
            s_logger.Debug($"Adding wiremock mappings from {file}");
            var fileContent = File.ReadAllText(file);
            var transformedContent = (transformations ?? StringTransformations.NoTransformationsInstance)
                .Transform(fileContent)
                .Replace("'", "\'");
            var payload = new StringContent(transformedContent, Encoding.UTF8, "application/json");
            var response = Task.Run(async () => await s_httpClient.PostAsync(
                WiremockBaseHttpUrl + "/__admin/mappings/import",
                payload)
            ).Result;

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception($"Unable to add Wiremock mapping. Response status code: {response.StatusCode}");
            }

            s_logger.Debug($"Wiremock mappings added from {file}");
        }

        private static void DownloadWiremockIfRequired()
        {
            lock (s_lock)
            {
                if (File.Exists(s_wiremockJarPath))
                {
                    if (CheckFileSHA256(s_wiremockJarPath, WiremockJarFileSha256))
                    {
                        s_logger.Debug($"Wiremock v{WiremockVersion} exists.");
                        return;
                    }
                    else
                    {
                        s_logger.Debug($"Wiremock v{WiremockVersion} exists but is corrupted. Deleting and downloading again.");
                        File.Delete(s_wiremockJarPath);
                    }
                }

                try
                {
                    s_logger.Debug($"Wiremock v{WiremockVersion} not found. Starting download.");
                    Directory.CreateDirectory(s_wiremockPath);
                    var response = s_httpClient.GetAsync($"{s_wiremockUrl}");
                    Task.Run(async () => await response.Result.Content.CopyToAsync(new FileStream(s_wiremockJarPath, FileMode.CreateNew))).Wait();
                    s_logger.Debug($"Wiremock v{WiremockVersion} has been downloaded into {s_wiremockPath}.");
                }
                catch (Exception)
                {
                    if (File.Exists(s_wiremockJarPath))
                    {
                        File.Delete(s_wiremockJarPath);
                    }

                    throw;
                }
            }
        }

        private static bool CheckFileSHA256(string filePath, string expectedShaValue)
        {
            byte[] bytes;
            try
            {
                bytes = File.ReadAllBytes(filePath);
            }
            catch (IOException exception)
            {
                if (exception.Message.Contains("The process cannot access the file") &&
                    exception.Message.Contains("because it is being used by another process"))
                {
                    s_logger.Debug("Could not read wiremock jar file content because it was used by another process. Assuming the file is not corrupted.");
                    return true;
                }
                throw;
            }
            using (var sha256Encoder = SHA256.Create())
            {
                byte[] sha256Hash = sha256Encoder.ComputeHash(bytes);
                var hash = BitConverter.ToString(sha256Hash).Replace("-", string.Empty);
                return expectedShaValue.Equals(hash, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
