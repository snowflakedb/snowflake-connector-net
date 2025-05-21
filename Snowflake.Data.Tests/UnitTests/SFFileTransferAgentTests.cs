using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading;
    using System.IO;
    using System.Text;
    using System;

    [TestFixture]
    class SFFileTransferAgentTest : SFBaseTest
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

        // Token for async tests
        CancellationToken _cancellationToken;

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

        [SetUp]
        public void BeforeEachTest()
        {
            // Base object's names on worker thread id
            var threadSuffix = TestContext.CurrentContext.WorkerId?.Replace('#', '_');

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
                threshold = 209715200 // Server default threshold
            };

            _cancellationToken = new CancellationToken();

            _session = new SFSession(ConnectionStringMock, new SessionPropertiesContext());
        }

        [TearDown]
        public void AfterEachTest()
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

        [Test]
        public void TestUploadUsingFilepath()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [Test]
        public async Task TestUploadAsyncUsingFilepath()
        {
            // Arrange
            UploadSetUpFile();

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            await _fileTransferAgent.executeAsync(_cancellationToken).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [Test]
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
                ref memoryStream,
                null,
                null,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [Test]
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
                ref memoryStream,
                null,
                null,
                _cancellationToken);

            // Act
            await _fileTransferAgent.executeAsync(_cancellationToken).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize));
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the destination file includes the gzip extension
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath + ".gz", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the source file compression is none and the destination file compression is gzip
            Assert.AreEqual("none", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
            Assert.AreEqual("gzip", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int index = 0; index < numberOfFiles; index++)
            {
                result.Next();

                // Assert the file is uploaded
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.IsTrue(GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName).Contains(mockFileName));
                Assert.IsTrue(GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName).Contains(mockFileName));

                File.Delete($"{mockFileName}{index}.{extension}");
            }
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int i = 0; i < numberOfDirectories; i++)
            {
                result.Next();

                // Assert the file is uploaded
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.AreEqual(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.AreEqual(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));

                Directory.Delete($"{tempUploadRootDirectory}{i}", true);
            }
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();

            // Assert
            for (int i = 0; i < numberOfDirectories; i++)
            {
                result.Next();
                // Assert the file is uploaded
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.AreEqual(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.AreEqual(mockFileName, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            }

            Directory.Delete(tempUploadRootDirectory, true);
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.AreEqual(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.That(ex.Message, Does.Match($"No file found for: {tempUploadRootDirectory}\\*/{tempUploadSecondDirectory}\\*/{mockFileName}"));

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

        [Test]
        public void TestDownload()
        {
            // Arrange
            DownloadSetUpFile();

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.DOWNLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [Test]
        public async Task TestDownloadAsync()
        {
            // Arrange
            DownloadSetUpFile();

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            await _fileTransferAgent.executeAsync(_cancellationToken).ConfigureAwait(false);
            SFResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.DOWNLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.AreEqual(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.IsInstanceOf<AggregateException>(ex.InnerException);
            var innerException = ((AggregateException)ex.InnerException)?.InnerExceptions[0];
            Assert.IsInstanceOf<FileNotFoundException>(innerException);
            Assert.That(innerException?.Message, Does.Match("Could not find file .*"));
        }

        [Test]
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
                _responseData,
                _cancellationToken);

            // Act
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.AreEqual(_responseData.queryId, ex.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(ex, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            Assert.IsInstanceOf<AggregateException>(ex.InnerException);
            var innerException = ((AggregateException)ex.InnerException)?.InnerExceptions[0];
            Assert.IsInstanceOf<DirectoryNotFoundException>(innerException);
            Assert.That(innerException?.Message, Does.Match("Could not find a part of the path .*"));
        }

        [Test]
        public void TestGetFilePathWithoutSpacesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT file://" + s_filePathWithoutSpaces + " @TestStage", s_filePathWithoutSpaces);
        }

        [Test]
        public void TestGetFilePathWithSpacesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT file://" + s_filePathWithSpaces + "  @TestStage", s_filePathWithSpaces);
        }

        [Test]
        public void TestGetFilePathWithoutSpacesAndWithSingleQuotesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT 'file://" + s_filePathWithoutSpaces + "' @TestStage", s_filePathWithoutSpaces);
        }

        [Test]
        public void TestGetFilePathWithSpacesAndWithSingleQuotesFromPutCommand()
        {
            TestGetFilePathFromPutCommand("PUT 'file://" + s_filePathWithSpaces + "'  @TestStage", s_filePathWithSpaces);
        }

        public void TestGetFilePathFromPutCommand(string query, string expectedFilePath)
        {
            var actualFilePath = SFFileTransferAgent.getFilePathFromPutCommand(query);
            Assert.AreEqual(expectedFilePath, actualFilePath);
        }
    }
}
