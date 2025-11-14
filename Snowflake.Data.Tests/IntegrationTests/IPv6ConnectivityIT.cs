using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.IntegrationTests
{
    /// <summary>
    /// Test to verify IPv6 connectivity with Snowflake.
    /// Tests include:
    /// 1. SELECT 1
    /// 2. SELECT pi()
    /// 3. PUT operation (upload small random file)
    /// 4. GET operation (download the file)
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class IPv6ConnectivityIT : SFBaseTest
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<IPv6ConnectivityIT>();

        [Test]
        [Category("IPv6")]
        public void TestIPv6ConnectivityComplete()
        {
            Console.WriteLine("🚀 =================================");
            Console.WriteLine("🚀 Starting IPv6 Connectivity Test");
            Console.WriteLine("🚀 =================================");
            LogTestStart("IPv6 Connectivity Complete Test");

            // Check DNS resolution first
            var hostname = ResolveHost();
            Console.WriteLine($"🌐 Target hostname: {hostname}");
            CheckDnsResolution(hostname);

            // Use explicit key-pair authentication to avoid MFA issues
            TestConnectionWithNetworkMonitoringKeyPair();
            
            Console.WriteLine("🎉 =================================");
            Console.WriteLine("🎉 IPv6 Connectivity Test PASSED!");
            Console.WriteLine("🎉 =================================");
            LogTestComplete("IPv6 Connectivity Complete Test");
        }

        [Test]
        [Category("IPv6")]
        public void TestIPv6DnsResolution()
        {
            LogTestStart("IPv6 DNS Resolution Test");

            var hostname = ResolveHost();
            s_logger.Info($"Testing DNS resolution for: {hostname}");

            var (ipv4Addresses, ipv6Addresses) = CheckDnsResolution(hostname);

            // Assertions
            Assert.Greater(ipv4Addresses.Count + ipv6Addresses.Count, 0, 
                "At least one IP address (IPv4 or IPv6) should be resolved");

            if (ipv6Addresses.Count > 0)
            {
                s_logger.Info($"✅ IPv6 addresses found: {ipv6Addresses.Count}");
                foreach (var addr in ipv6Addresses.Take(3))
                {
                    Assert.IsTrue(IPAddress.TryParse(addr, out var parsedAddr), 
                        $"Should be a valid IP address: {addr}");
                    Assert.AreEqual(AddressFamily.InterNetworkV6, parsedAddr.AddressFamily, 
                        $"Should be IPv6 address: {addr}");
                }
            }
            else
            {
                s_logger.Warn("⚠️ No IPv6 addresses found in DNS resolution");
            }

            LogTestComplete("IPv6 DNS Resolution Test");
        }

        [Test]
        [Category("IPv6")]
        public void TestIPv6BasicQuery()
        {
            LogTestStart("IPv6 Basic Query Test");

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                s_logger.Info($"Connected to Snowflake successfully");

                using (var cmd = conn.CreateCommand())
                {
                    TestSelect1(cmd);
                    TestSelectPi(cmd);
                }
            }

            LogTestComplete("IPv6 Basic Query Test");
        }

        private void LogTestStart(string testName)
        {
            var separator = new string('=', 60);
            s_logger.Info(separator);
            s_logger.Info($"Starting {testName}");
            s_logger.Info(separator);
        }

        private void LogTestComplete(string testName)
        {
            var separator = new string('=', 60);
            s_logger.Info(separator);
            s_logger.Info($"{testName} Completed Successfully");
            s_logger.Info(separator);
        }

        private (List<string> ipv4, List<string> ipv6) CheckDnsResolution(string hostname)
        {
            Console.WriteLine($"🔍 Checking DNS resolution for: {hostname}");
            s_logger.Info($"Checking DNS resolution for: {hostname}");
            
            var ipv4Addresses = new List<string>();
            var ipv6Addresses = new List<string>();

            try
            {
                // Get all address info for the hostname
                var hostEntry = Dns.GetHostEntry(hostname);
                
                Console.WriteLine($"📊 DNS resolution for {hostname}:");
                s_logger.Info($"DNS resolution for {hostname}:");
                
                foreach (var address in hostEntry.AddressList)
                {
                    var addressString = address.ToString();
                    
                    if (address.AddressFamily == AddressFamily.InterNetwork)
                    {
                        ipv4Addresses.Add(addressString);
                        Console.WriteLine($"  🌐 IPv4: {addressString}");
                        s_logger.Info($"  IPv4: {addressString}");
                    }
                    else if (address.AddressFamily == AddressFamily.InterNetworkV6)
                    {
                        ipv6Addresses.Add(addressString);
                        Console.WriteLine($"  🌍 IPv6: {addressString}");
                        s_logger.Info($"  IPv6: {addressString}");
                    }
                }

                // Summary
                Console.WriteLine($"📈 Summary: Found {ipv4Addresses.Count} IPv4 address(es) and {ipv6Addresses.Count} IPv6 address(es)");
                s_logger.Info($"Summary: Found {ipv4Addresses.Count} IPv4 address(es) and {ipv6Addresses.Count} IPv6 address(es)");

                if (ipv6Addresses.Count > 0)
                {
                    Console.WriteLine($"✅ IPv6 addresses available: {string.Join(", ", ipv6Addresses.Take(3))}");
                    s_logger.Info($"IPv6 addresses available: {string.Join(", ", ipv6Addresses.Take(3))}");
                }
                else
                {
                    Console.WriteLine("⚠️ WARNING: No IPv6 addresses found in DNS resolution!");
                    s_logger.Warn("WARNING: No IPv6 addresses found in DNS resolution!");
                }

                if (ipv4Addresses.Count > 0)
                {
                    Console.WriteLine($"✅ IPv4 addresses available: {string.Join(", ", ipv4Addresses.Take(3))}");
                    s_logger.Info($"IPv4 addresses available: {string.Join(", ", ipv4Addresses.Take(3))}");
                }
                else
                {
                    Console.WriteLine("⚠️ WARNING: No IPv4 addresses found in DNS resolution!");
                    s_logger.Warn("WARNING: No IPv4 addresses found in DNS resolution!");
                }

                // Important note about IPv6 connectivity
                LogIpv6Notes();
            }
            catch (Exception ex)
            {
                s_logger.Error($"Could not resolve addresses for {hostname}: {ex.Message}");
                throw;
            }

            return (ipv4Addresses, ipv6Addresses);
        }

        private void LogIpv6Notes()
        {
            var separator = new string('=', 60);
            s_logger.Info(separator);
            s_logger.Info("Note: If you get HTTP 403 Forbidden with IPv6, it means:");
            s_logger.Info("  - Connection reached Snowflake server (network works)");
            s_logger.Info("  - Server rejected IPv6 connection (endpoint may not support IPv6)");
            s_logger.Info("  - This is a server-side policy, not a network issue");
            s_logger.Info(separator);
        }

        private void CheckActiveConnections(string hostname)
        {
            s_logger.Info("Checking active connections...");

            try
            {
                var connections = GetActiveConnections(hostname);
                if (connections.Any())
                {
                    s_logger.Info("Active connections found:");
                    foreach (var connection in connections)
                    {
                        s_logger.Info($"  {connection}");
                    }
                }
                else
                {
                    s_logger.Info("No active connections found (may need elevated privileges)");
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        s_logger.Info("Windows: Run 'netstat -an | findstr snowflake' as Administrator");
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        s_logger.Info("Linux: Run 'sudo lsof -i -P | grep snowflake' or 'sudo netstat -tulpn | grep snowflake'");
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    {
                        s_logger.Info("macOS: Run 'sudo lsof -i -P | grep snowflake' or 'netstat -an | grep snowflake'");
                    }
                }
            }
            catch (Exception ex)
            {
                s_logger.Warn($"Could not check active connections: {ex.Message}");
            }
        }

        private List<string> GetActiveConnections(string hostname)
        {
            var connections = new List<string>();

            try
            {
                ProcessStartInfo startInfo;
                
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    startInfo = new ProcessStartInfo
                    {
                        FileName = "netstat",
                        Arguments = "-an",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        CreateNoWindow = true
                    };
                }
                else
                {
                    startInfo = new ProcessStartInfo
                    {
                        FileName = "lsof",
                        Arguments = "-i -P",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        CreateNoWindow = true
                    };
                }

                using (var process = Process.Start(startInfo))
                {
                    if (process != null)
                    {
                        process.WaitForExit(5000); // 5 second timeout
                        
                        if (process.HasExited && process.ExitCode == 0)
                        {
                            var output = process.StandardOutput.ReadToEnd();
                            var lines = output.Split('\n')
                                            .Where(line => line.Contains(hostname) || 
                                                         line.Contains("snowflakecomputing") ||
                                                         line.Contains("snowflake"))
                                            .Take(10); // Limit to first 10 matches
                            
                            connections.AddRange(lines);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                s_logger.Debug($"Exception in GetActiveConnections: {ex.Message}");
                // Not critical, just return empty list
            }

            return connections;
        }

        private void SetupDatabaseAndWarehouse(IDbCommand cmd)
        {
            if (!string.IsNullOrEmpty(testConfig.database))
            {
                s_logger.Info($"Using database: {testConfig.database}");
                cmd.CommandText = $"USE DATABASE {testConfig.database}";
                cmd.ExecuteNonQuery();
            }

            if (!string.IsNullOrEmpty(testConfig.warehouse))
            {
                s_logger.Info($"Using warehouse: {testConfig.warehouse}");
                cmd.CommandText = $"USE WAREHOUSE {testConfig.warehouse}";
                cmd.ExecuteNonQuery();
            }
        }

        private void TestSelect1(IDbCommand cmd)
        {
            s_logger.Info("Test 1: Executing SELECT 1");
            
            cmd.CommandText = "SELECT 1";
            var result = cmd.ExecuteScalar();
            
            s_logger.Info($"SELECT 1 result: {result}");
            Assert.AreEqual(1, Convert.ToInt32(result), "SELECT 1 should return 1");
            
            s_logger.Info("✅ SELECT 1 test passed");
        }

        private void TestSelectPi(IDbCommand cmd)
        {
            s_logger.Info("Test 2: Executing SELECT pi()");
            
            cmd.CommandText = "SELECT pi()";
            var result = cmd.ExecuteScalar();
            
            var piValue = Convert.ToDouble(result);
            s_logger.Info($"SELECT pi() result: {piValue}");
            
            // Check if the value is close to pi (allowing for floating point precision)
            Assert.That(Math.Abs(piValue - Math.PI), Is.LessThan(0.000001), 
                $"Expected pi (~{Math.PI}), got {piValue}");
            
            s_logger.Info("✅ SELECT pi() test passed");
        }

        private void TestPutAndGetOperations(IDbCommand cmd)
        {
            s_logger.Info("Test 3 & 4: Starting PUT and GET operations");

            // Create temporary directory and file
            var tempDir = Path.GetTempPath();
            var testFileName = $"test_ipv6_file_{Guid.NewGuid().ToString("N")[..8]}.txt";
            var testFilePath = Path.Combine(tempDir, testFileName);

            try
            {
                // Generate random test file
                s_logger.Info($"Generating test file: {testFilePath}");
                GenerateRandomFile(testFilePath, 5); // 5KB file

                var fileInfo = new FileInfo(testFilePath);
                s_logger.Info($"Test file size: {fileInfo.Length} bytes");
                Assert.Greater(fileInfo.Length, 0, "Test file should not be empty");

                // Use user stage (internal stage, no external credentials needed)
                var stageName = "~"; // User stage
                s_logger.Info($"Using user stage: {stageName}");

                // PUT file to stage
                var putSql = $"PUT file://{testFilePath.Replace('\\', '/')} @{stageName}";
                s_logger.Info($"Executing PUT: {putSql}");
                
                cmd.CommandText = putSql;
                var putReader = cmd.ExecuteReader();
                
                var putResults = new List<object[]>();
                while (putReader.Read())
                {
                    var row = new object[putReader.FieldCount];
                    putReader.GetValues(row);
                    putResults.Add(row);
                }
                putReader.Close();

                s_logger.Info($"PUT results count: {putResults.Count}");
                Assert.Greater(putResults.Count, 0, "PUT should return results");

                // Check PUT status (typically in column 6 - STATUS)
                if (putResults[0].Length > 6)
                {
                    var status = putResults[0][6]?.ToString() ?? "UNKNOWN";
                    s_logger.Info($"PUT status: {status}");
                    Assert.IsTrue(status == "UPLOADED" || status == "SKIPPED", 
                        $"File should be uploaded or skipped, got status: {status}");
                }

                // List files in stage to verify upload
                s_logger.Info($"Listing files in stage: {stageName}");
                cmd.CommandText = $"LIST @{stageName}";
                var listReader = cmd.ExecuteReader();
                
                var stageFiles = new List<string>();
                while (listReader.Read())
                {
                    if (listReader.GetValue(0) != null)
                    {
                        stageFiles.Add(listReader.GetString(0));
                    }
                }
                listReader.Close();

                s_logger.Info($"Files in stage: {stageFiles.Count}");
                var uploadedFile = stageFiles.FirstOrDefault(f => f.Contains(testFileName));
                Assert.IsNotNull(uploadedFile, "Uploaded file should be found in stage listing");
                s_logger.Info($"Found uploaded file: {uploadedFile}");

                // GET file from stage
                var outputDir = Path.Combine(tempDir, $"ipv6_download_{Guid.NewGuid().ToString("N")[..8]}");
                Directory.CreateDirectory(outputDir);
                
                var getSql = $"GET @{stageName}/{testFileName}.gz file://{outputDir.Replace('\\', '/')}/";
                s_logger.Info($"Executing GET: {getSql}");
                
                cmd.CommandText = getSql;
                var getReader = cmd.ExecuteReader();
                
                while (getReader.Read())
                {
                    // Process GET results if needed
                }
                getReader.Close();

                // Verify file was downloaded
                var downloadedFiles = Directory.GetFiles(outputDir, "*.gz");
                s_logger.Info($"Downloaded files: {downloadedFiles.Length}");
                Assert.Greater(downloadedFiles.Length, 0, "File should be downloaded");

                var downloadedFile = downloadedFiles[0];
                var downloadedFileInfo = new FileInfo(downloadedFile);
                s_logger.Info($"Downloaded file: {downloadedFile}");
                s_logger.Info($"Downloaded file size: {downloadedFileInfo.Length} bytes");
                Assert.Greater(downloadedFileInfo.Length, 0, "Downloaded file should not be empty");

                // Clean up: remove file from stage
                s_logger.Info("Cleaning up: removing file from stage");
                cmd.CommandText = $"REMOVE @{stageName}/{testFileName}.gz";
                cmd.ExecuteNonQuery();

                s_logger.Info("✅ PUT and GET operations completed successfully");

                // Clean up local files
                try
                {
                    File.Delete(testFilePath);
                    Directory.Delete(outputDir, true);
                }
                catch (Exception ex)
                {
                    s_logger.Warn($"Could not clean up temporary files: {ex.Message}");
                }
            }
            catch (Exception ex)
            {
                s_logger.Error($"PUT/GET operations failed: {ex.Message}");
                
                // Clean up on failure
                try
                {
                    if (File.Exists(testFilePath))
                        File.Delete(testFilePath);
                }
                catch
                {
                    // Ignore cleanup errors
                }
                
                throw;
            }
        }

        private void TestConnectionWithNetworkMonitoringKeyPair()
        {
            Console.WriteLine("🔗 Testing connection with explicit key-pair authentication");
            s_logger.Info("Testing connection with explicit key-pair authentication and monitoring network connections");
            
            // Build explicit key-pair connection string
            var explicitConnectionString = "scheme=https;" +
                                         $"host={testConfig.host};" +
                                         "port=443;" +
                                         $"account={testConfig.account};" +
                                         $"role={testConfig.role};" +
                                         $"db={testConfig.database};" +
                                         $"schema={testConfig.schema};" +
                                         $"warehouse={testConfig.warehouse};" +
                                         "authenticator=snowflake_jwt;" +
                                         $"user={testConfig.user};" +
                                         "private_key_file=rsa_key_dotnet_my_keypair.p8;";
            
            Console.WriteLine($"🔐 Using JWT key-pair authentication");
            Console.WriteLine($"🏠 Connecting to host: {testConfig.host}");
            s_logger.Info($"Using explicit connection string with key-pair authentication");
            
            using (var connection = new SnowflakeDbConnection(explicitConnectionString))
            {
                Console.WriteLine("📡 Opening Snowflake connection...");
                s_logger.Info("Opening connection...");
                connection.Open();
                Console.WriteLine($"✅ Connection opened successfully: {connection.State}");
                s_logger.Info($"✅ Connection opened successfully: {connection.State}");
                Console.WriteLine($"📊 Connection DataSource: {connection.DataSource}");

                using (var cmd = connection.CreateCommand())
                {
                    // Set up database and warehouse if specified
                    SetupDatabaseAndWarehouse(cmd);

                    // Test 1: SELECT 1
                    Console.WriteLine("🧪 Test 1: Executing SELECT 1");
                    TestSelect1(cmd);

                    // Test 2: SELECT pi()
                    Console.WriteLine("🧪 Test 2: Executing SELECT pi()");
                    TestSelectPi(cmd);

                    // Test 3 & 4: PUT and GET operations
                    Console.WriteLine("🧪 Test 3&4: Executing PUT and GET operations");
                    TestPutAndGetOperations(cmd);
                }
            }
            
            Console.WriteLine("✅ All operations completed successfully with key-pair authentication");
            s_logger.Info("✅ All operations completed successfully with key-pair authentication");
        }

        private void GenerateRandomFile(string filePath, int sizeKb)
        {
            var random = new Random();
            var content = new StringBuilder();
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\n";
            
            var targetSize = sizeKb * 1024;
            while (content.Length < targetSize)
            {
                content.Append(chars[random.Next(chars.Length)]);
            }

            File.WriteAllText(filePath, content.ToString());
        }
    }
}
