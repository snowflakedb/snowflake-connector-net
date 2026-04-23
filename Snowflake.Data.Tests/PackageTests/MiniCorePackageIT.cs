using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

// Build NuGet, create a fresh consumer app, install the
// packaged connector, and run it to ensure MiniCore ships and loads correctly.
// Unit tests do not cover the nuget packaging step.

namespace Snowflake.Data.Tests.PackageTests
{
    [TestFixture]
    [Category("MiniCore")]
    public class MiniCorePackageIT
    {
        private string _tempDir;
        private string _artifactsDir;
        private string _repoRoot;

        [SetUp]
        public void Setup()
        {
            _repoRoot = Path.GetFullPath(Path.Combine(TestContext.CurrentContext.TestDirectory, "../../../../"));
            _artifactsDir = Path.Combine(_repoRoot, "artifacts");
            _tempDir = Path.Combine(Path.GetTempPath(), $"sf_mc_test_{Guid.NewGuid():N}".Substring(0, 30));

            Directory.CreateDirectory(_artifactsDir);
            Directory.CreateDirectory(_tempDir);
        }

        [TearDown]
        public void TearDown()
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }

        [Test]
        [Timeout(1_000 * 60 * 6)] // 6 mins
        public async Task TestMiniCoreLoadsFromNugetPackage()
        {
            // 1. Pack NuGet
            await RunCommandAsync("dotnet", $"pack \"{Path.Combine(_repoRoot, "Snowflake.Data", "Snowflake.Data.csproj")}\" -c Release -o \"{_artifactsDir}\"", timeoutMs: 1_000 * 60 * 5,  expectedSuccessMessage: "Successfully created package").ConfigureAwait(false);

            var packagePath = Directory.GetFiles(_artifactsDir, "Snowflake.Data.*.nupkg")
                .Where(f => !f.EndsWith(".symbols.nupkg"))
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();
            Assert.IsNotNull(packagePath, "NuGet package not found");

            var version = Path.GetFileNameWithoutExtension(packagePath).Replace("Snowflake.Data.", "");

            // 2. Create consumer app
            await RunCommandAsync("dotnet", "new console --force --verbosity quiet", _tempDir, timeoutMs: 30000).ConfigureAwait(false);
            await RunCommandAsync("dotnet", "add package Microsoft.Extensions.Logging.Abstractions --version 9.0.5 --verbosity quiet", _tempDir, timeoutMs: 60000).ConfigureAwait(false);
            await RunCommandAsync("dotnet", $"add package Snowflake.Data --version {version} --source \"{_artifactsDir}\"", _tempDir, timeoutMs: 60000).ConfigureAwait(false);

            var sourceFile = Path.Combine(TestContext.CurrentContext.TestDirectory, "PackageTests", "MiniCoreVerificationAppSource.cs");
            File.Copy(sourceFile, Path.Combine(_tempDir, "Program.cs"), overwrite: true);

            // 3. Run & Assert
            var (exitCode, output) = await RunCommandAsync("dotnet", "run --verbosity quiet", _tempDir, timeoutMs: 60000).ConfigureAwait(false);

            Assert.AreEqual(0, exitCode, $"Verification app failed: {output}");
            Assert.That(output, Contains.Substring("[PROBE] MiniCore loaded successfully"));
        }

        private async Task<(int exitCode, string output)> RunCommandAsync(string command, string args, string workingDir = null, int timeoutMs = 60000, string expectedSuccessMessage = null)
        {
            TestContext.Progress.WriteLine($"Running: {command} {args} (in {workingDir ?? _repoRoot})");
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = command,
                    Arguments = args,
                    WorkingDirectory = workingDir ?? _repoRoot,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            var outputBuilder = new System.Text.StringBuilder();
            var errorBuilder = new System.Text.StringBuilder();

            process.OutputDataReceived += (_, e) =>
            {
                if (e.Data != null) outputBuilder.AppendLine($"[{DateTime.UtcNow:T}] {e.Data}");
            };
            process.ErrorDataReceived += (_, e) =>
            {
                if (e.Data != null) errorBuilder.AppendLine($"[{DateTime.UtcNow:T}] {e.Data}");
            };

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            // Wait for process to exit with timeout
            var cts = new CancellationTokenSource(timeoutMs);
            try
            {
                #if NET5_0_OR_GREATER
                await process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
                #else
                if (!process.WaitForExit(timeoutMs)) throw new TaskCanceledException();
                #endif
            }
            catch (TaskCanceledException)
            {
                try { process.Kill(); } catch { }
                var partialOutput = outputBuilder + Environment.NewLine + "ERROR (Partial):" + Environment.NewLine + errorBuilder;
                TestContext.Progress.WriteLine($"Command timed out! Partial output:\n{partialOutput}");

                if (string.IsNullOrEmpty(expectedSuccessMessage) || partialOutput.Contains(expectedSuccessMessage)) // sometimes Process component has issues with exiting even though command was successful.
                    throw new TimeoutException($"Command '{command} {args}' timed out after {timeoutMs}ms");
            }

            var stdout = outputBuilder.ToString();
            var stderr = errorBuilder.ToString();
            var output = $"{stdout}{Environment.NewLine} ERROR:{Environment.NewLine}{stderr}";

            return (process.ExitCode, output);
        }
    }
}
