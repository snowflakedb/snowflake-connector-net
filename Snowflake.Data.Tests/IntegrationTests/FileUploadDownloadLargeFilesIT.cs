using System;
using System.IO;
using System.Security.Cryptography;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    public class FileUploadDownloadLargeFilesIT : SFBaseTest
    {
        private const string FileName = "large_file_to_test_dotnet_driver.json";
        private static readonly string s_uniqueId = TestDataGenarator.NextAlphaNumeric(6);
        private static readonly string s_localFolderName = Path.Combine(Path.GetTempPath(), s_uniqueId);
        private static readonly string s_remoteFolderName = $"files_to_test_put_get_{s_uniqueId}";
        private static readonly string s_downloadFolderName = Path.Combine(s_localFolderName, "download");
        private static readonly string s_fullFileName = Path.Combine(s_localFolderName, FileName);
        private static readonly string s_fullDownloadedFileName = Path.Combine(s_downloadFolderName, FileName);
        private static readonly MD5 s_md5 = MD5.Create();

        [OneTimeSetUp]
        public static void GenerateLargeFileForTests()
        {
            CreateLocalDirectory(s_localFolderName);
            GenerateLargeFile(s_fullFileName);
        }

        [OneTimeTearDown]
        public static void DeleteGeneratedLargeFile()
        {
            RemoveLocalFile(s_fullFileName);
            RemoveDirectory(s_localFolderName);
        }

        [Test]
        public void TestThatUploadsAndDownloadsTheSameFile()
        {
            // act
            UploadFile(s_fullFileName, s_remoteFolderName);
            DownloadFile(s_remoteFolderName, s_downloadFolderName, FileName);

            // assert
            Assert.AreEqual(
                CalcualteMD5(s_fullFileName),
                CalcualteMD5(s_fullDownloadedFileName));

            // cleanup
            RemoveFilesFromServer(s_remoteFolderName);
            RemoveLocalFile(s_fullDownloadedFileName);
        }

        private static void GenerateLargeFile(string fullFileName)
        {
            File.Delete(fullFileName);
            RandomJsonGenerator.GenerateRandomJsonFile(fullFileName, 128 * 1024);
        }

        private void UploadFile(string fullFileName, string remoteFolderName)
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "FILE_TRANSFER_MEMORY_THRESHOLD=1048576;";
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = $"PUT file://{fullFileName} @~/{remoteFolderName} AUTO_COMPRESS=FALSE";
                command.ExecuteNonQuery();
            }
        }

        private void DownloadFile(string remoteFolderName, string downloadFolderName, string fileName)
        {
            var filePattern = $"{remoteFolderName}/{fileName}";
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = $"GET @~/{remoteFolderName} file://{downloadFolderName} PATTERN='{filePattern}'";
                command.ExecuteNonQuery();
            }
        }

        private void RemoveFilesFromServer(string remoteFolderName)
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = $"remove @~/{remoteFolderName};";
                command.ExecuteNonQuery();
            }
        }

        private static string CalcualteMD5(string fullFileName)
        {
            using (var fileStream = File.OpenRead(fullFileName))
            {
                var hash = s_md5.ComputeHash(fileStream);
                return BitConverter.ToString(hash);
            }
        }

        private static void RemoveLocalFile(string fullFileName) => File.Delete(fullFileName);

        private static void CreateLocalDirectory(string path) => Directory.CreateDirectory(path);

        private static void RemoveDirectory(string path) => Directory.Delete(path, true);
    }
}
