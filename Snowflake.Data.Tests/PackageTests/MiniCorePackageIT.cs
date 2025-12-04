using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using NUnit.Framework;

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
            RunCommand("dotnet", $"pack \"{Path.Combine(_repoRoot, "Snowflake.Data", "Snowflake.Data.csproj")}\" -c Release -o \"{_artifactsDir}\"");

            var packagePath = Directory.GetFiles(_artifactsDir, "Snowflake.Data.*.nupkg")
                .Where(f => !f.EndsWith(".symbols.nupkg"))
                .OrderByDescending(File.GetLastWriteTimeUtc)
                .FirstOrDefault();
            Assert.IsNotNull(packagePath, "NuGet package not found");

            var version = Path.GetFileNameWithoutExtension(packagePath).Replace("Snowflake.Data.", "");

            // 2. Create consumer app
            RunCommand("dotnet", "new console --force", _tempDir);
            RunCommand("dotnet", "add package Microsoft.Extensions.Logging.Abstractions --version 9.0.5", _tempDir);
            RunCommand("dotnet", $"add package Snowflake.Data --version {version} --source \"{_artifactsDir}\"", _tempDir);

            var sourceFile = Path.Combine(TestContext.CurrentContext.TestDirectory, "PackageTests", "MiniCoreVerificationAppSource.cs");
            File.Copy(sourceFile, Path.Combine(_tempDir, "Program.cs"), overwrite: true);

            // 3. Run & Assert
            var (exitCode, output) = RunCommand("dotnet", "run", _tempDir);

            Assert.AreEqual(0, exitCode, $"Verification app failed: {output}");
            Assert.That(output, Contains.Substring("[PROBE] MiniCore loaded successfully"));
        }

        private (int exitCode, string output) RunCommand(string command, string args, string workingDir = null)
        {
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = command,
                Arguments = args,
                WorkingDirectory = workingDir ?? _repoRoot,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            });
            process.WaitForExit();
            return (process.ExitCode, process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd());
        }
    }
}
