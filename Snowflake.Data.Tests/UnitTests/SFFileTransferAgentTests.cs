using System.Linq;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;
using System.IO.Compression;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading;
    using System.IO;
    using System.Text;
    using System;
    public class SFFileTransferAgentTest : IDisposable
    {
        // Mock data for file metadata
        [ThreadStatic] private static string t_locationStage;
        const string LocationId = "mock-id";
        const string LocationTables = "tables";
        const string LocationKey = "mock-key";
        const string LocationPath = LocationTables + "/" + LocationKey + "/";
        string _location;

        // Connection string for mock session
        const string ConnectionStringMock = "user=user;password=password;account=account;";

        // File name for the mock file
        [ThreadStatic] private static string t_realSourceFilePath;

        // File size of the mock file
        long _sourceFileSize;

        // Mock encryption material for the files
        List<PutGetEncryptionMaterial> _encryptionMaterial = new List<PutGetEncryptionMaterial>()
        {
            new PutGetEncryptionMaterial()
            {
                queryId = "MOCK/QUERY/ID/==",
                queryStageMasterKey = "MOCKQUERYSTAGEMASTERKE==",
                smkId = 9999
            }
        };

        // Mock response data properties
        [ThreadStatic] private static string t_localLocation;
        List<string> _srcLocations;
        const string AutoDetect = "auto_detect";
        const int Parallel = 1;

        // Mock response data
        PutGetResponseData _responseData;
        SFFileTransferAgent _fileTransferAgent;
        SFSession _session;

        // Mock PUT/GET queries
        string _putQuery;
        const string GetQuery = "GET @DB.SCHEMA.%MOCKTABLE file://;";

        // Mock file content
        const string FileContent = "FTAFileContent";

        // Mock file paths
        static readonly string s_filePathWithoutSpaces = Path.Combine("C:\\Users\\Test\\", "folder_without_space", "*.*");
        static readonly string s_filePathWithSpaces = Path.Combine("C:\\Users\\Test\\", "folder with space", "*.*");

        public SFFileTransferAgentTest()
        {
            BeforeEachTest();
        }

        public void Dispose()
        {
            AfterEachTest();
        }

        private void BeforeEachTest()
        {
            // Base object's names on worker thread id
            var threadSuffix = Thread.CurrentThread.ManagedThreadId.ToString()?.Replace('#', '_');

            // Set values for thread variables
            t_realSourceFilePath = $"realSrcFilePath_{threadSuffix}.txt";
            t_localLocation = $"mockLocalLocation_{threadSuffix}";
            t_locationStage = $"mock-customer-stage_{threadSuffix}";

            // Set values for members that depend on thread variables
            _srcLocations = new List<string>()
            {
                t_realSourceFilePath
            };
            _putQuery = "PUT file://" + t_realSourceFilePath + " @DB.SCHEMA.%MOCKTABLE;";
            _location = Path.GetFullPath(t_locationStage + "/" + LocationId + "/" + LocationPath);

            _responseData = new PutGetResponseData()
            {
                autoCompress = false,
                encryptionMaterial = _encryptionMaterial,
                localLocation = t_localLocation,
                overwrite = false,
                parallel = Parallel,
                presignedUrl = null,
                presignedUrls = null,
                queryId = null,
                rowSet = null,
                rowType = null,
                sourceCompression = AutoDetect,
                sqlState = null,
                src_locations = _srcLocations,
                stageInfo = new PutGetStageInfo()
                {
                    location = _location,
                    locationType = SFRemoteStorageUtil.LOCAL_FS, // Use local storage for testing
                    path = LocationPath,
                    presignedUrl = null,
                    stageCredentials = null
                },
                statementTypeId = 0,
                threshold = 20971520 // Server default threshold
            };

            _session = new SFSession(ConnectionStringMock, new SessionPropertiesContext());
        }

        private void AfterEachTest()
        {
            // Delete stage directory recursively
            if (Directory.Exists(t_locationStage))
            {
                Directory.Delete(t_locationStage, true);
            }

            // Upload teardown
            // Delete mock files
            foreach (string location in _srcLocations)
            {
                File.Delete(location);
            }

            // Download teardown
            // Delete local directory recursively
            if (Directory.Exists(t_localLocation))
            {
                Directory.Delete(t_localLocation, true);
            }
        }

        private void UploadSetUpFile()
        {
            // Upload setup
            // Write mock file to upload
            File.WriteAllText(_srcLocations[0], FileContent);
            _sourceFileSize = new FileInfo(_srcLocations[0]).Length;

        }

        private string GetResultValue(SFResultSet result, SFResultSet.PutGetResponseRowTypeInfo typeInfo)
        {
            return result.GetObjectInternal((int)typeInfo).ToString();
        }

        [SFFact]
        public void TestUploadUsingFilepath()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.Equal(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.Equal(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [SFFact]
        public async Task TestUploadAsyncUsingFilepath()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData);

            // Act
            await _fileTransferAgent.executeAsync(CancellationToken.None).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.Equal(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.Equal(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [SFFact]
        public void TestUploadUsingMemoryStream()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            MemoryStream memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false));

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [SFFact]
        public async Task TestUploadAsyncUsingMemoryStream()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            MemoryStream memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false)
                );

            // Act
            await _fileTransferAgent.executeAsync(CancellationToken.None).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [SFFact]
        public void TestUploadWithGZIPCompression()
        {
            // Arrange
            UploadSetUpFile();

            // Compresses the file with GZIP by default
            _responseData.autoCompress = true;
            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the destination file includes the gzip extension
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal(t_realSourceFilePath + ".gz", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the source file compression is none and the destination file compression is gzip
            Assert.Equal("none", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [SFFact]
        public void TestUploadMemoryStreamWithGZIPCompression()
        {
            // Arrange
            UploadSetUpFile();

            _responseData.autoCompress = true;
            _responseData.command = CommandTypes.UPLOAD.ToString();
            var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false)
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt.gz", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            Assert.Equal("none", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [SFFact]
        public void TestUploadMemoryStreamWithGZIPCompressionDisabledByEnvVar()
        {
            // Arrange
            UploadSetUpFile();

            _responseData.autoCompress = true;
            _responseData.command = CommandTypes.UPLOAD.ToString();
            var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            var mockEnv = new Mock<IEnvironmentFacade>();
            mockEnv.Setup(e => e.GetBool(EnvVars.PutDisableInMemoryCompress)).Returns(true);

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false),
                mockEnv.Object);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert - compression still happens but via temp files, not in-memory
            Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Matches("stream.realSrcFilePath_[0-9]*\\.txt.gz", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            Assert.Equal("none", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [SFFact]
        public void TestUploadMemoryStreamWithSpillToFileCompressesCorrectly()
        {
            // Arrange — use a large repetitive payload so gzip is noticeably smaller
            UploadSetUpFile();

            _responseData.autoCompress = true;
            _responseData.command = nameof(CommandTypes.UPLOAD);
            var payload = string.Concat(Enumerable.Repeat("AAAAAAAAAA,BBBBBBBBBB,CCCCCCCCCC\n", 200));
            var rawBytes = Encoding.UTF8.GetBytes(payload);
            var memoryStream = new MemoryStream(rawBytes);

            var mockEnv = new Mock<IEnvironmentFacade>();
            mockEnv.Setup(e => e.GetBool(EnvVars.PutDisableInMemoryCompress)).Returns(true);

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false),
                mockEnv.Object);

            // Act
            _fileTransferAgent.execute();
            var result = _fileTransferAgent.result();
            result.Next();

            // Assert — dest (compressed) must be smaller than source (raw)
            var srcSize = long.Parse(GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            var destSize = long.Parse(GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
            Assert.Equal(rawBytes.Length, srcSize);
            Assert.True(destSize < srcSize, $"Compressed size {destSize} should be smaller than raw size {srcSize}");
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));

            // The caller's stream must still be usable
            Assert.True(memoryStream.CanRead, "Caller's stream should not be disposed");
        }

        [SFFact]
        public void TestGetQueryWithMemoryStreamThrowsNotSupportedException()
        {
            // Arrange
            _responseData.command = nameof(CommandTypes.DOWNLOAD);
            var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            // Act & Assert
            Assert.Throws<NotSupportedException>(() =>
                new SFFileTransferAgent(GetQuery, _session, _responseData, new BorrowedMemoryStream(memoryStream, false)));
        }

        [SFFact]
        public void TestUploadMemoryStreamWithPathInPutCommand()
        {
            // Arrange
            UploadSetUpFile();

            _responseData.command = nameof(CommandTypes.UPLOAD);
            _responseData.src_locations = new List<string> { "subdir/data.csv" };
            var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, false));

            // Act
            _fileTransferAgent.execute();
            var result = _fileTransferAgent.result();
            result.Next();

            // Assert - only the file name (not the directory) is preserved with "stream." prefix
            Assert.Equal(nameof(ResultStatus.UPLOADED), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Equal("stream.data.csv", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal("stream.data.csv", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [SFFact]
        public void TestUploadDriversOwnMemoryStreamWithGZIPCompressionSpillsToFile()
        {
            // Arrange
            UploadSetUpFile();

            _responseData.autoCompress = true;
            _responseData.command = nameof(CommandTypes.UPLOAD);
            var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(FileContent));

            // IsDriversOwn=true means canCompressSpillToFile=true, compression goes to temp file not in-memory
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(memoryStream, true));

            // Act
            _fileTransferAgent.execute();
            var result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(nameof(ResultStatus.UPLOADED), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.Equal("none", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [SFFact]
        public void TestUploadPreGzippedMemoryStreamDoesNotDisposeCallersStream()
        {
            // Arrange
            UploadSetUpFile();

            _responseData.autoCompress = true;
            _responseData.command = nameof(CommandTypes.UPLOAD);

            // Create a pre-gzipped memory stream
            var gzippedStream = new MemoryStream();
            using (var gzip = new GZipStream(gzippedStream, CompressionMode.Compress, leaveOpen: true))
                gzip.Write(Encoding.UTF8.GetBytes(FileContent));
            gzippedStream.Position = 0;

            // FILE_TRANSFER_MEMORY_THRESHOLD=-1 means all in-memory (no spill-to-file)
            _session = new SFSession(ConnectionStringMock + "FILE_TRANSFER_MEMORY_THRESHOLD=-1;", new SessionPropertiesContext());

            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                new BorrowedMemoryStream(gzippedStream, false));

            // Act
            _fileTransferAgent.execute();
            var result = _fileTransferAgent.result();
            result.Next();

            // Assert - caller's stream must not be disposed
            Assert.Equal(nameof(ResultStatus.UPLOADED), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            Assert.True(gzippedStream.CanRead, "Caller's stream should not be disposed after upload");
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.Equal("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [SFFact]
        public void TestUploadWithWildcardInTheFilename()
        {
            // Arrange
            UploadSetUpFile();

            // The file name used for creating multiple files
            string mockFileName = "testUploadWithMultipleFiles";
            string extension = "txt";

            // Create source location with wildcard in its filename
            _responseData.src_locations = new List<string>()
            {
                // Add wildcard in the source location
                $"{mockFileName}*.{extension}",
            };

            // Write the mock files
            int numberOfFiles = 3;
            for (int index = 0; index < numberOfFiles; index++)
            {
                File.WriteAllText($"{mockFileName}{index}.{extension}", FileContent);
            }

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int index = 0; index < numberOfFiles; index++)
            {
                result.Next();

                // Assert the file is uploaded
                Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.Contains(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.Contains(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));

                File.Delete($"{mockFileName}{index}.{extension}");
            }
        }

        [SFFact]
        public void TestUploadWithWildcardInTheRootDirectory()
        {
            // Arrange
            UploadSetUpFile();

            // Create the mock directory and files
            string mockFileName = "testUploadWithMultipleDirectory.txt";
            string tempUploadRootDirectory = "mockDirectoryWithWildcardInRootDirectory";
            int numberOfDirectories = 3;

            for (int i = 0; i < numberOfDirectories; i++)
            {
                Directory.CreateDirectory($"{tempUploadRootDirectory}{i}");
                File.WriteAllText($"{tempUploadRootDirectory}{i}/{mockFileName}", FileContent);
            }

            // Create source location with wildcard in its filename
            _responseData.src_locations = new List<string>()
            {
                // Add wildcard in the source location
                $"{tempUploadRootDirectory}*/{mockFileName}",
            };

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int i = 0; i < numberOfDirectories; i++)
            {
                result.Next();

                // Assert the file is uploaded
                Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.Equal(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.Equal(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));

                Directory.Delete($"{tempUploadRootDirectory}{i}", true);
            }
        }

        [SFFact]
        public void TestUploadWithWildcardInTheDirectoryPath()
        {
            // Arrange
            UploadSetUpFile();

            // Create the mock directory and files
            string mockFileName = "testUploadWithMultipleDirectory.txt";
            string tempUploadRootDirectory = "mockDirectoryWithWildcardInDirectoryPath";
            string mockPath = $"{tempUploadRootDirectory}/mockDirectory";
            int numberOfDirectories = 3;

            for (int i = 0; i < numberOfDirectories; i++)
            {
                Directory.CreateDirectory($"{mockPath}{i}");
                File.WriteAllText($"{mockPath}{i}/{mockFileName}", FileContent);
            }

            // Create source location with wildcard in its filename
            _responseData.src_locations = new List<string>()
            {
                // Add wildcard in the source location
                $"{mockPath}*/{mockFileName}",
            };

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int i = 0; i < numberOfDirectories; i++)
            {
                result.Next();
                // Assert the file is uploaded
                Assert.Equal(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.Equal(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.Equal(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            }

            Directory.Delete(tempUploadRootDirectory, true);
        }

        [SFFact]
        public void TestUploadThrowsExceptionForMissingRootDirectoryWithWildcard()
        {
            // Arrange
            UploadSetUpFile();

            // Create the mock directory and files
            string mockFileName = "testUploadWithMultipleDirectory.txt";
            string tempUploadRootDirectory = "mockRootDirectoryWithWildcard";
            string tempUploadSecondDirectory = "secondDirectoryWithWilcard";
            int numberOfDirectories = 3;

            // Do not create the root directory

            // Create the second directory and write to file but the test should still fail
            for (int i = 0; i < numberOfDirectories; i++)
            {
                Directory.CreateDirectory($"{tempUploadSecondDirectory}{i}");
                File.WriteAllText($"{tempUploadSecondDirectory}{i}/{mockFileName}", FileContent);
            }

            // Create source location with wildcard in its filename
            _responseData.src_locations = new List<string>()
            {
                // Add wildcard in the source location
                $"{tempUploadRootDirectory}*/{tempUploadSecondDirectory}*/{mockFileName}",
            };

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _responseData.queryId = Guid.NewGuid().ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData
                );

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.Equal(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.Matches($"No file found for: {tempUploadRootDirectory}\\*/{tempUploadSecondDirectory}\\*/{mockFileName}", ex.Message);

            for (int i = 0; i < numberOfDirectories; i++)
            {
                Directory.Delete($"{tempUploadSecondDirectory}{i}", true);
            }
        }

        private void DownloadSetUpFile()
        {
            // Download setup
            // Write mock file in the local location to download
            if (!Directory.Exists(_location))
            {
                Directory.CreateDirectory(_location);
            }
            File.WriteAllText(_location + t_realSourceFilePath, FileContent);
        }

        [SFFact]
        public void TestDownload()
        {
            // Arrange
            DownloadSetUpFile();

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData
                );

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.DOWNLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [SFFact]
        public async Task TestDownloadAsync()
        {
            // Arrange
            DownloadSetUpFile();

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData
                );

            // Act
            await _fileTransferAgent.executeAsync(CancellationToken.None).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.Equal(ResultStatus.DOWNLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.Equal(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [SFFact]
        public void TestDownloadThrowsErrorFileNotFound()
        {
            // Arrange
            DownloadSetUpFile();

            // Use a fake file name to trigger the file error
            _responseData.src_locations = new List<string>()
            {
                "fakeFile.txt"
            };

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _responseData.queryId = Guid.NewGuid().ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData
                );

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.Equal(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.IsType<AggregateException>(ex.InnerException);
            var innerException = ((AggregateException)ex.InnerException)?.InnerExceptions[0];
            Assert.IsType<FileNotFoundException>(innerException);
            Assert.Matches("Could not find file .*", innerException?.Message);
        }

        [SFFact]
        public void TestDownloadThrowsErrorDirectoryNotFound()
        {
            // Arrange
            DownloadSetUpFile();

            // Delete the directory to trigger the directory error
            if (Directory.Exists(_location))
            {
                Directory.Delete(_location, true);
            }

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _responseData.queryId = Guid.NewGuid().ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData
                );

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.Equal(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.IsType<AggregateException>(ex.InnerException);
            var innerException = ((AggregateException)ex.InnerException)?.InnerExceptions[0];
            Assert.IsType<DirectoryNotFoundException>(innerException);
            Assert.Matches("Could not find a part of the path .*", innerException?.Message);
        }

        [SFFact]
        public void TestGetFilePathWithoutSpacesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT file://" + s_filePathWithoutSpaces + " @TestStage", s_filePathWithoutSpaces);
        }

        [SFFact]
        public void TestGetFilePathWithSpacesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT file://" + s_filePathWithSpaces + "  @TestStage", s_filePathWithSpaces);
        }

        [SFFact]
        public void TestGetFilePathWithoutSpacesAndWithSingleQuotesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT 'file://" + s_filePathWithoutSpaces + "' @TestStage", s_filePathWithoutSpaces);
        }

        [SFFact]
        public void TestGetFilePathWithSpacesAndWithSingleQuotesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT 'file://" + s_filePathWithSpaces + "'  @TestStage", s_filePathWithSpaces);
        }

        private void TestGetFilePathFromPutCommand(string query, string expectedFilePath)
        {
            var actualFilePath = SFFileTransferAgent.getFilePathFromPutCommand(query);
            Assert.Equal(expectedFilePath, actualFilePath);
        }
    }
}
