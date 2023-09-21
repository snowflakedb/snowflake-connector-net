﻿/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;
    using System.Collections.Generic;
    using Snowflake.Data.Tests.Mock;
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

        [SetUp]
        public void BeforeTest()
        {
            // Set values for thread variables
            t_realSourceFilePath = TestNameWithWorker + "_realSrcFilePath.txt";
            t_localLocation = TestNameWithWorker + "mockLocalLocation";
            t_locationStage = TestNameWithWorker + "mock-customer-stage";

            // Set values for members that depend on thread variables
            _srcLocations = new List<string>()
            {
                t_realSourceFilePath
            };
            _putQuery = "PUT file://" + t_realSourceFilePath + " @DB.SCHEMA.%MOCKTABLE;";
            _location = t_locationStage + "/" + LocationId + "/" + LocationPath;

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

            _session = new SFSession(ConnectionStringMock, null);

            // Upload setup
            // Write mock file to upload
            File.WriteAllText(_srcLocations[0], FileContent);
            _sourceFileSize = new FileInfo(_srcLocations[0]).Length;

            // Download setup
            // Write mock file in the local location to download
            if (!Directory.Exists(_location))
            {
                Directory.CreateDirectory(_location);
            }
            File.WriteAllText(_location + t_realSourceFilePath, FileContent);
        }

        [TearDown]
        public void AfterTest()
        {
            // Upload teardown
            // Delete mock files
            foreach (string location in _srcLocations)
            {
                File.Delete(location);
            }

            // Download teardown
            // Delete stage/local directory recursively
            if (Directory.Exists(t_locationStage))
            {
                Directory.Delete(t_locationStage, true);
            }
            if (Directory.Exists(t_localLocation))
            {
                Directory.Delete(t_localLocation, true);
            }
        }

        [Test]
        [Ignore("FileTransferAgentTest")]
        public void FileTransferAgentTestDone()
        {
            // Do nothing;
        }

        private string GetResultValue(SFBaseResultSet result, SFResultSet.PutGetResponseRowTypeInfo typeInfo)
        {
            return result.getObjectInternal((int)typeInfo).ToString();
        }

        [Test]
        public void TestUploadUsingFilepath()
        {
            // Arrange
            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFBaseResultSet result = _fileTransferAgent.result();
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
            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            await _fileTransferAgent.executeAsync(_cancellationToken).ConfigureAwait(false);
            SFBaseResultSet result = _fileTransferAgent.result();
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
            SFBaseResultSet result = _fileTransferAgent.result();
            result.Next();

            // Assert
            Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check the name of the source file and destination file are the same
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
            Assert.AreEqual(t_realSourceFilePath, GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));
            // Check the file size of the source file and destination file are the same
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileSize));
            Assert.AreEqual(_sourceFileSize.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileSize)); ;
        }

        [Test]
        public async Task TestUploadAsyncUsingMemoryStream()
        {
            // Arrange
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
            SFBaseResultSet result = _fileTransferAgent.result();
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
            SFBaseResultSet result = _fileTransferAgent.result();
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
        public void TestUploadWithWilcardInTheFilename()
        {
            // Arrange
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
            for (int i = 0; i < numberOfFiles; i++)
            {
                File.WriteAllText($"{mockFileName}{i}.{extension}", FileContent);
            }

            // Set command to upload
            _responseData.command = CommandTypes.UPLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(_putQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFBaseResultSet result = _fileTransferAgent.result();

            // Assert
            for (int i = 0; i < numberOfFiles; i++)
            {
                result.Next();

                // Assert the file is uploaded
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(), GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check the name of the source file and destination file are the same
                Assert.AreEqual($"{mockFileName}{i}.{extension}", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.SourceFileName));
                Assert.AreEqual($"{mockFileName}{i}.{extension}", GetResultValue(result, SFResultSet.PutGetResponseRowTypeInfo.DestinationFileName));

                File.Delete($"{mockFileName}{i}.{extension}");
            }
        }

        [Test]
        public void TestUploadWithWildcardInTheRootDirectory()
        {
            // Arrange
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
            SFBaseResultSet result = _fileTransferAgent.result();

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
            SFBaseResultSet result = _fileTransferAgent.result();

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
        public void TestDownload()
        {
            // Arrange
            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            _fileTransferAgent.execute();
            SFBaseResultSet result = _fileTransferAgent.result();
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
            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            await _fileTransferAgent.executeAsync(_cancellationToken).ConfigureAwait(false);
            SFBaseResultSet result = _fileTransferAgent.result();
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
            // Use a fake file name to trigger the file error
            _responseData.src_locations = new List<string>()
            {
                "fakeFile.txt"
            };

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            Exception ex = Assert.Throws<AggregateException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.IsInstanceOf<FileNotFoundException>(ex.InnerException);
            Assert.That(ex.InnerException.Message, Does.Match("Could not find file .*"));
        }

        [Test]
        public void TestDownloadThrowsErrorDirectoryNotFound()
        {
            // Arrange
            // Delete the directory to trigger the directory error
            if (Directory.Exists(_location))
            {
                Directory.Delete(_location, true);
            }

            // Set command to download
            _responseData.command = CommandTypes.DOWNLOAD.ToString();
            _fileTransferAgent = new SFFileTransferAgent(GetQuery,
                _session,
                _responseData,
                _cancellationToken);

            // Act
            Exception ex = Assert.Throws<AggregateException>(() => _fileTransferAgent.execute());

            // Assert
            Assert.IsInstanceOf<DirectoryNotFoundException>(ex.InnerException);
            Assert.That(ex.InnerException.Message, Does.Match("Could not find a part of the path .*"));
        }
    }
}
