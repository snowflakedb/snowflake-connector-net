using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public class FileUploadDownloadLargeFilesIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public FileUploadDownloadLargeFilesIT(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        private const string FileName = "large_file_to_test_dotnet_driver.json";
        private static readonly string s_uniqueId = TestDataGenarator.NextAlphaNumeric(6);
        private static readonly string s_localFolderName = Path.Combine(Path.GetTempPath(), s_uniqueId);
        private static readonly string s_remoteFolderName = $"files_to_test_put_get_{s_uniqueId}";
        private static readonly string s_downloadFolderName = Path.Combine(s_localFolderName, "download");
        private static readonly string s_fullFileName = Path.Combine(s_localFolderName, FileName);
        private static readonly string s_fullDownloadedFileName = Path.Combine(s_downloadFolderName, FileName);
        private static readonly MD5 s_md5 = MD5.Create();
        public static void GenerateLargeFileForTests()
        {
            CreateLocalDirectory(s_localFolderName);
            GenerateLargeFile(s_fullFileName);
        }
        public static void DeleteGeneratedLargeFile()
        {
            RemoveLocalFile(s_fullFileName);
            RemoveDirectory(s_localFolderName);
        }

        [Fact]
        public async Task TestThatUploadsAndDownloadsTheSameFile()
        {
            // act
            await UploadFileAsync(s_fullFileName, s_remoteFolderName);
            await DownloadFileAsync(s_remoteFolderName, s_downloadFolderName, FileName);

            // assert
            Assert.Equal(
                CalcualteMD5(s_fullFileName),
                CalcualteMD5(s_fullDownloadedFileName));

            // cleanup
            await RemoveFilesFromServerAsync(s_remoteFolderName);
            RemoveLocalFile(s_fullDownloadedFileName);
        }

        private static void GenerateLargeFile(string fullFileName)
        {
            File.Delete(fullFileName);
            RandomJsonGenerator.GenerateRandomJsonFile(fullFileName, 128 * 1024);
        }

        private async Task UploadFileAsync(string fullFileName, string remoteFolderName)
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "FILE_TRANSFER_MEMORY_THRESHOLD=1048576;";
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                command.CommandText = $"PUT file://{fullFileName} @~/{remoteFolderName} AUTO_COMPRESS=FALSE";
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task DownloadFileAsync(string remoteFolderName, string downloadFolderName, string fileName)
        {
            var filePattern = $"{remoteFolderName}/{fileName}";
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                command.CommandText = $"GET @~/{remoteFolderName} file://{downloadFolderName} PATTERN='{filePattern}'";
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task RemoveFilesFromServerAsync(string remoteFolderName)
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                command.CommandText = $"remove @~/{remoteFolderName};";
                await command.ExecuteNonQueryAsync();
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
