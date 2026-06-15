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
    public sealed class FileUploadDownloadLargeFilesITFixture : IDisposable
    {
        internal const string FileName = "large_file_to_test_dotnet_driver.json";
        internal readonly string s_uniqueId;
        internal readonly string s_localFolderName;
        internal readonly string s_remoteFolderName;
        internal readonly string s_downloadFolderName;
        internal readonly string s_fullFileName;
        internal readonly string s_fullDownloadedFileName;

        public FileUploadDownloadLargeFilesITFixture()
        {
            s_uniqueId = TestDataGenarator.NextAlphaNumeric(6);
            s_localFolderName = Path.Combine(Path.GetTempPath(), s_uniqueId);
            s_remoteFolderName = $"files_to_test_put_get_{s_uniqueId}";
            s_downloadFolderName = Path.Combine(s_localFolderName, "download");
            s_fullFileName = Path.Combine(s_localFolderName, FileName);
            s_fullDownloadedFileName = Path.Combine(s_downloadFolderName, FileName);
            CreateLocalDirectory(s_localFolderName);
            GenerateLargeFile(s_fullFileName);
        }

        public void Dispose()
        {
            RemoveLocalFile(s_fullFileName);
            RemoveDirectory(s_localFolderName);
        }

        private static void GenerateLargeFile(string fullFileName)
        {
            File.Delete(fullFileName);
            RandomJsonGenerator.GenerateRandomJsonFile(fullFileName, 128 * 1024);
        }

        internal static void RemoveLocalFile(string fullFileName) => File.Delete(fullFileName);

        private static void CreateLocalDirectory(string path) => Directory.CreateDirectory(path);

        private static void RemoveDirectory(string path) => Directory.Delete(path, true);
    }

    public class FileUploadDownloadLargeFilesIT : SFBaseTestAsync, IClassFixture<FileUploadDownloadLargeFilesITFixture>
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        private readonly FileUploadDownloadLargeFilesITFixture _fixture2;

        public FileUploadDownloadLargeFilesIT(SFBaseTestAsyncFixture fixture, FileUploadDownloadLargeFilesITFixture fixture2) : base(fixture)
        {
            _fixture = fixture;
            _fixture2 = fixture2;
        }

        private static readonly MD5 s_md5 = MD5.Create();

        [SFFact]
        public async Task TestThatUploadsAndDownloadsTheSameFile()
        {
            // act
            await UploadFileAsync(_fixture2.s_fullFileName, _fixture2.s_remoteFolderName);
            await DownloadFileAsync(_fixture2.s_remoteFolderName, _fixture2.s_downloadFolderName, FileUploadDownloadLargeFilesITFixture.FileName);

            // assert
            Assert.Equal(
                CalcualteMD5(_fixture2.s_fullFileName),
                CalcualteMD5(_fixture2.s_fullDownloadedFileName));

            // cleanup
            await RemoveFilesFromServerAsync(_fixture2.s_remoteFolderName);
            FileUploadDownloadLargeFilesITFixture.RemoveLocalFile(_fixture2.s_fullDownloadedFileName);
        }

        private async Task UploadFileAsync(string fullFileName, string remoteFolderName)
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "FILE_TRANSFER_MEMORY_THRESHOLD=1048576;";
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                command.CommandText = $"PUT file://{fullFileName} @~/{remoteFolderName} AUTO_COMPRESS=FALSE";
                command.ExecuteNonQuery();
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
                command.ExecuteNonQuery();
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

    }
}
