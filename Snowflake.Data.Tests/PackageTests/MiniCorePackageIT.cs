using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
        [Timeout(300_000)]
        public void TestMiniCoreLoadsFromNugetPackage()
        {
            // 1. Pack NuGet
            RunCommand("dotnet", $"pack \"{Path.Combine(_repoRoot, "Snowflake.Data", "Snowflake.Data.csproj")}\" -c Release -o \"{_artifactsDir}\"", timeoutMs: 120000);

            var packagePath = Directory.GetFiles(_artifactsDir, "Snowflake.Data.*.nupkg")
                .Where(f => !f.EndsWith(".symbols.nupkg"))
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();
            Assert.IsNotNull(packagePath, "NuGet package not found");

            var version = Path.GetFileNameWithoutExtension(packagePath).Replace("Snowflake.Data.", "");

            // 2. Create consumer app
            RunCommand("dotnet", "new console --force", _tempDir, timeoutMs: 30000);
            RunCommand("dotnet", "add package Microsoft.Extensions.Logging.Abstractions --version 9.0.5", _tempDir, timeoutMs: 60000);
            RunCommand("dotnet", $"add package Snowflake.Data --version {version} --source \"{_artifactsDir}\"", _tempDir, timeoutMs: 60000);

            var sourceFile = Path.Combine(TestContext.CurrentContext.TestDirectory, "PackageTests", "MiniCoreVerificationAppSource.cs");
            File.Copy(sourceFile, Path.Combine(_tempDir, "Program.cs"), overwrite: true);

            // 3. Run & Assert
            var (exitCode, output) = RunCommand("dotnet", "run", _tempDir, timeoutMs: 60000);

            Assert.AreEqual(0, exitCode, $"Verification app failed: {output}");
            Assert.That(output, Contains.Substring("[PROBE] MiniCore loaded successfully"));
        }

        private (int exitCode, string output) RunCommand(string command, string args, string workingDir = null, int timeoutMs = 60000)
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

            using var outputWaitHandle = new System.Threading.AutoResetEvent(false);
            using var errorWaitHandle = new System.Threading.AutoResetEvent(false);

            process.OutputDataReceived += (sender, e) =>
            {
                if (e.Data == null) outputWaitHandle.Set();
                else outputBuilder.AppendLine(e.Data);
            };
            process.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data == null) errorWaitHandle.Set();
                else errorBuilder.AppendLine(e.Data);
            };

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            // Wait for process to exit with timeout
            if (!process.WaitForExit(timeoutMs))
            {
                try { process.Kill(); } catch { }
                var partialOutput = outputBuilder + Environment.NewLine + "ERROR (Partial):" + Environment.NewLine + errorBuilder;
                TestContext.Progress.WriteLine($"Command timed out! Partial output:\n{partialOutput}");
                throw new TimeoutException($"Command '{command} {args}' timed out after {timeoutMs}ms");
            }

            outputWaitHandle.WaitOne(1000);
            errorWaitHandle.WaitOne(1000);

            var stdout = outputBuilder.ToString();
            var stderr = errorBuilder.ToString();
            var output = stdout + Environment.NewLine + "ERROR:" + Environment.NewLine + stderr;

            TestContext.Progress.WriteLine($"Exit Code: {process.ExitCode}");
            TestContext.Progress.WriteLine($"Output:\n{output}");

            return (process.ExitCode, output);
        }
    }
}
