using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO.Compression;
using System.Text;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Linq;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;

    [TestFixture]
    [Parallelizable(ParallelScope.Children)]
    class SFPutGetTest : SFBaseTest
    {
        private const int NumberOfRows = 4;
        private static readonly string[] s_colName = { "C1", "C2", "C3" };
        private static readonly string[] s_colData = { "FIRST", "SECOND", "THIRD" };
        private static string s_outputDirectory;

        [ThreadStatic] private static string t_schemaName;
        [ThreadStatic] private static string t_tableName;
        [ThreadStatic] private static string t_stageName;
        [ThreadStatic] private static string t_stageNameSse; // server side encryption without client side encryption
        [ThreadStatic] private static string t_fileName;
        [ThreadStatic] private static string t_outputFileName;
        [ThreadStatic] private static string t_inputFilePath;
        [ThreadStatic] private static string t_outputFilePath;
        [ThreadStatic] private static string t_internalStagePath;
        [ThreadStatic] private static StageType t_stageType;
        [ThreadStatic] private static string t_sourceCompressionType;
        [ThreadStatic] private static string t_destCompressionType;
        [ThreadStatic] private static bool t_autoCompress;
        [ThreadStatic] private static List<string> t_filesToDelete;

        public enum StageType
        {
            USER,
            TABLE,
            NAMED
        }

        [OneTimeSetUp]
        public static void OneTimeSetUp()
        {
            // Create temp output directory for downloaded files
            s_outputDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(s_outputDirectory);
        }

        [OneTimeTearDown]
        public static void OneTimeTearDown()
        {
            // Delete temp output directory and downloaded files
            Directory.Delete(s_outputDirectory, true);
        }

        [SetUp]
        public void SetUp()
        {
            // Base object's names on on worker thread id
            var threadSuffix = TestContext.CurrentContext.WorkerId?.Replace('#', '_');

            t_schemaName = testConfig.schema;
            t_tableName = $"TABLE_{threadSuffix}";
            t_stageName = $"STAGE_{threadSuffix}";
            t_stageNameSse = $"STAGE_{threadSuffix}_SSE";
            t_filesToDelete = new List<string>();

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Create temp table
                    var columnNamesWithTypes = string.Join(",", s_colName.Select(col => col + " STRING"));
                    command.CommandText = $"CREATE OR REPLACE TABLE {t_schemaName}.{t_tableName} ({columnNamesWithTypes})";
                    command.ExecuteNonQuery();

                    // Create temp stage
                    command.CommandText = $"CREATE OR REPLACE STAGE {t_schemaName}.{t_stageName}";
                    command.ExecuteNonQuery();

                    // Create temp stage without client side encryption
                    command.CommandText = $"CREATE OR REPLACE STAGE {t_schemaName}.{t_stageNameSse} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Drop temp stage
                    command.CommandText = $"DROP STAGE IF EXISTS {t_schemaName}.{t_stageName}";
                    command.ExecuteNonQuery();

                    // Drop temp table
                    command.CommandText = $"DROP TABLE IF EXISTS {t_schemaName}.{t_tableName}";
                    command.ExecuteNonQuery();
                }
            }

            // Delete temp files if necessary
            if (t_filesToDelete != null)
            {
                foreach (var file in t_filesToDelete)
                {
                    File.Delete(file);
                }
            }
        }

        [Test]
        public void TestPutFileAsteriskWildcard()
        {
            var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
            var files = new List<string> {
                $"{absolutePathPrefix}_one.csv",
                $"{absolutePathPrefix}_two.csv",
                $"{absolutePathPrefix}_three.csv"
            };
            PrepareFileData(files);

            // Set the PUT query variables
            t_inputFilePath = $"{absolutePathPrefix}*";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutFileAsteriskWildcardWithExtension()
        {
            var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
            var files = new List<string> {
                $"{absolutePathPrefix}_one.csv",
                $"{absolutePathPrefix}_two.csv",
                $"{absolutePathPrefix}_three.csv"
            };
            PrepareFileData(files);
            // Create file with the same name structure but with a different extension
            PrepareFileData($"{absolutePathPrefix}_four.txt");

            // Set the PUT query variables
            t_inputFilePath = $"{absolutePathPrefix}*.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutFileQuestionMarkWildcard()
        {
            var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
            var files = new List<string> {
                $"{absolutePathPrefix}_1.csv",
                $"{absolutePathPrefix}_2.csv",
                $"{absolutePathPrefix}_3.csv"
            };
            PrepareFileData(files);
            // Create file which should be omitted during the transfer
            PrepareFileData($"{absolutePathPrefix}_four.csv");

            // Set the PUT query variables
            t_inputFilePath = $"{absolutePathPrefix}_?.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutFileRelativePathWithoutDirectory()
        {
            // Set the PUT query variables
            t_inputFilePath = $"{Guid.NewGuid()}_1.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            PrepareFileData(t_inputFilePath);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, new List<string> { t_inputFilePath }, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutGetOnClosedConnectionThrowsWithoutQueryId([Values("GET", "PUT")] string command)
        {
            t_inputFilePath = "unexisting_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                // conn.Open(); // intentionally closed
                var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => ProcessFile(command, conn));
                Assert.NotNull(snowflakeDbException);
                Assert.IsNull(snowflakeDbException.QueryId);
                SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);
            }
        }

        [Test]
        public void TestGetNonExistentFileReturnsFalseAndDoesNotThrow()
        {
            t_inputFilePath = "non_existent_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var sql = $"GET {t_internalStagePath}/{t_fileName} file://{s_outputDirectory}";
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = sql;
                    var reader = command.ExecuteReader();
                    Assert.AreEqual(false, reader.Read());
                }
            }
        }

        [Test]
        public void TestPutNonExistentFileThrowsWithQueryId()
        {
            t_inputFilePath = "non_existent_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
                Assert.IsNotNull(snowflakeDbException);
                Assert.IsNotNull(snowflakeDbException.QueryId);
                SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            }
        }

        [Test]
        public void TestPutFileProvidesQueryIdOnFailure()
        {
            // Arrange
            // Set the PUT query variables but do not create a file
            t_inputFilePath = "unexisting_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
                var queryId = snowflakeDbException.QueryId;

                // Assert
                Assert.IsNotEmpty(queryId);
                Assert.DoesNotThrow(() => Guid.Parse(queryId));
                SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.IO_ERROR_ON_GETPUT_COMMAND);
            }
        }

        [Test]
        public void TestPutFileWithSyntaxErrorProvidesQueryIdOnFailure()
        {
            // Arrange
            // Set the PUT query variables but do not create a file
            t_inputFilePath = "unexisting_file.csv SOME CODE FORCING SYNTAX ERROR";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
                var queryId = snowflakeDbException.QueryId;

                // Assert
                Assert.IsNotEmpty(queryId);
                Assert.DoesNotThrow(() => Guid.Parse(queryId));
                Assert.That(snowflakeDbException.ErrorCode, Is.EqualTo(1003));
                Assert.That(snowflakeDbException.InnerException, Is.Null);
            }
        }

        [Test]
        public void TestPutFileProvidesQueryIdOnSuccess()
        {
            // Arrange
            // Set the PUT query variables
            t_inputFilePath = $"{Guid.NewGuid()}_1.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";
            PrepareFileData(t_inputFilePath);

            // Act
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                var queryId = PutFile(conn);

                // Assert
                Assert.IsNotNull(queryId);
                Assert.DoesNotThrow(() => Guid.Parse(queryId));
                VerifyFilesAreUploaded(conn, new List<string> { t_inputFilePath }, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutFileRelativePathWithDirectory()
        {
            var guid = Guid.NewGuid();
            var relativePath = $"{guid}";
            Directory.CreateDirectory(relativePath);

            // Set the PUT query variables
            t_inputFilePath = $"{relativePath}{Path.DirectorySeparatorChar}{guid}_1.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            PrepareFileData(t_inputFilePath);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, new List<string> { t_inputFilePath }, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutFileRelativePathAsteriskWildcard()
        {
            var relativePath = $"{Guid.NewGuid()}";
            var files = new List<string> {
                $"{relativePath}_one.csv",
                $"{relativePath}_two.csv",
                $"{relativePath}_three.csv"
            };
            PrepareFileData(files);

            // Set the PUT query variables
            t_inputFilePath = $"{relativePath}*";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        // presigned url is enabled on CI so we need to disable the test
        // it should be enabled when downscoped credential is the default option
        [IgnoreOnEnvIs("snowflake_cloud_env", new[] { "GCP" })]
        public void TestPutFileWithoutOverwriteFlagSkipsSecondUpload()
        {
            // Set the PUT query variables
            t_inputFilePath = $"{Guid.NewGuid()}.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            PrepareFileData(t_inputFilePath);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn, expectedStatus: ResultStatus.UPLOADED);
                VerifyFilesAreUploaded(conn, new List<string> { t_inputFilePath }, t_internalStagePath);
                PutFile(conn, expectedStatus: ResultStatus.SKIPPED);
            }
        }

        [Test]
        public void TestPutFileWithOverwriteFlagRunsSecondUpload()
        {
            var overwriteAttribute = "OVERWRITE=TRUE";

            // Set the PUT query variables
            t_inputFilePath = $"{Guid.NewGuid()}.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            PrepareFileData(t_inputFilePath);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn, overwriteAttribute, expectedStatus: ResultStatus.UPLOADED);
                VerifyFilesAreUploaded(conn, new List<string> { t_inputFilePath }, t_internalStagePath);
                PutFile(conn, overwriteAttribute, expectedStatus: ResultStatus.UPLOADED);
            }
        }

        [Test]
        public void TestPutDirectoryAsteriskWildcard()
        {
            // Prepare the data files to be copied
            var guid = Guid.NewGuid();
            var path = $"{Path.GetTempPath()}{guid}";
            var files = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var filePath = $"{path}_{i}";
                Directory.CreateDirectory(filePath);
                var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
                PrepareFileData(fullPath);
                files.Add(fullPath);
            }

            // Set the PUT query variables
            t_inputFilePath = $"{path}*{Path.DirectorySeparatorChar}*";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutDirectoryQuestionMarkWildcard()
        {
            // Prepare the data files to be copied
            var guid = Guid.NewGuid();
            var path = $"{Path.GetTempPath()}{guid}";
            var files = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var filePath = $"{path}_{i}";
                Directory.CreateDirectory(filePath);
                var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
                PrepareFileData(fullPath);
                files.Add(fullPath);
            }

            // Set the PUT query variables
            t_inputFilePath = $"{path}_?{Path.DirectorySeparatorChar}{guid}_?_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutDirectoryMixedWildcard()
        {
            // Prepare the data files to be copied
            var guid = Guid.NewGuid();
            var path = $"{Path.GetTempPath()}{guid}";
            var files = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var filePath = $"{path}_{i}";
                Directory.CreateDirectory(filePath);
                var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
                PrepareFileData(fullPath);
                files.Add(fullPath);
            }

            // Set the PUT query variables
            t_inputFilePath = $"{path}_*{Path.DirectorySeparatorChar}{guid}_?_file.csv";
            t_internalStagePath = $"@{t_schemaName}.{t_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                VerifyFilesAreUploaded(conn, files, t_internalStagePath);
            }
        }

        [Test]
        public void TestPutGetCommand(
            [Values("none", "gzip", "bzip2", "brotli", "deflate", "raw_deflate", "zstd")] string sourceFileCompressionType,
            [Values] StageType stageType,
            [Values("", "/TEST_PATH", "/DEEP/TEST_PATH")] string stagePath,
            [Values] bool autoCompress)
        {
            PrepareTest(sourceFileCompressionType, stageType, stagePath, autoCompress);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                CopyIntoTable(conn);
                GetFile(conn);
            }
        }

        [Test]
        public void TestPutGetCommandForNamedStageWithoutClientSideEncryption(
            [Values("none", "gzip")] string sourceFileCompressionType,
            [Values("", "/DEEP/TEST_PATH")] string stagePath,
            [Values] bool autoCompress)
        {
            PrepareTest(sourceFileCompressionType, StageType.NAMED, stagePath, autoCompress, false);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                CopyIntoTable(conn);
                GetFile(conn);
            }
        }

        // Test small file upload/download with GCS_USE_DOWNSCOPED_CREDENTIAL set to true
        [Test]
        [IgnoreOnEnvIs("snowflake_cloud_env", new[] { "AWS", "AZURE" })]
        public void TestPutGetGcsDownscopedCredential(
            [Values] StageType stageType,
            [Values("", "/TEST_PATH")] string stagePath)
        {
            PrepareTest(null, stageType, stagePath, false);

            using (var conn = new SnowflakeDbConnection(ConnectionString + ";GCS_USE_DOWNSCOPED_CREDENTIAL=true"))
            {
                conn.Open();

                PutFile(conn);
                CopyIntoTable(conn);
                GetFile(conn);
            }
        }

        [Test]
        public void TestPutGetFileWithSpaceAndSingleQuote(
            [Values] StageType stageType,
            [Values("/STAGE PATH WITH SPACE")] string stagePath)
        {
            PrepareTest(null, stageType, stagePath, false, true, true);
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn, "", ResultStatus.UPLOADED, true);
                CopyIntoTable(conn, true);
                GetFile(conn, true);
            }
        }

        private void PrepareTest(string sourceFileCompressionType, StageType stageType, string stagePath,
            bool autoCompress, bool clientEncryption = true, bool makeFilePathWithSpace = false)
        {
            t_stageType = stageType;
            t_sourceCompressionType = sourceFileCompressionType;
            t_autoCompress = autoCompress;
            // Prepare temp file name with specified file extension
            t_fileName = Guid.NewGuid() + ".csv" +
                        (t_autoCompress ? SFFileCompressionTypes.LookUpByName(t_sourceCompressionType).FileExtension : "");
            var sourceFolderWithSpace = $"{Guid.NewGuid()} source file path with space";
            var inputPathBase = makeFilePathWithSpace ?
                Path.Combine(s_outputDirectory, sourceFolderWithSpace) :
                Path.GetTempPath();
            t_inputFilePath = Path.Combine(inputPathBase, t_fileName);

            if (IsCompressedByTheDriver())
            {
                t_destCompressionType = "gzip";
                t_outputFileName = $"{t_fileName}.gz";
            }
            else
            {
                t_destCompressionType = t_sourceCompressionType;
                t_outputFileName = t_fileName;
            }
            var destinationFolderWithSpace = $"{Guid.NewGuid()} destination file path with space";
            var outputPathBase = makeFilePathWithSpace ?
                Path.Combine(s_outputDirectory, destinationFolderWithSpace) :
                s_outputDirectory;
            t_outputFilePath = Path.Combine(outputPathBase, t_outputFileName);
            if (makeFilePathWithSpace)
            {
                Directory.CreateDirectory(inputPathBase);
                Directory.CreateDirectory(outputPathBase);
            }
            t_filesToDelete.Add(t_outputFilePath);
            PrepareFileData(t_inputFilePath);

            // Prepare stage name
            switch (t_stageType)
            {
                case StageType.USER:
                    t_internalStagePath = $"@~{stagePath}";
                    break;
                case StageType.TABLE:
                    t_internalStagePath = $"@{t_schemaName}.%{t_tableName}{stagePath}";
                    break;
                case StageType.NAMED:
                    t_internalStagePath = clientEncryption
                        ? $"@{t_schemaName}.{t_stageName}{stagePath}"
                        : $"@{t_schemaName}.{t_stageNameSse}{stagePath}";
                    break;
            }
        }

        private static bool IsCompressedByTheDriver()
        {
            return t_sourceCompressionType == "none" && t_autoCompress;
        }

        // PUT - upload file from local directory to the stage
        string PutFile(
            SnowflakeDbConnection conn,
            String additionalAttribute = "",
            ResultStatus expectedStatus = ResultStatus.UPLOADED,
            bool encloseInSingleQuotes = false)
        {
            string queryId;
            using (var command = conn.CreateCommand())
            {
                // Prepare PUT query
                var putQuery = encloseInSingleQuotes ?
                    $"PUT 'file://{t_inputFilePath.Replace("\\", "/")}' '{t_internalStagePath}'" :
                    $"PUT file://{t_inputFilePath} {t_internalStagePath}";
                putQuery += $" AUTO_COMPRESS={(t_autoCompress ? "TRUE" : "FALSE")}" + $" {additionalAttribute}";
                // Upload file
                command.CommandText = putQuery;
                var reader = command.ExecuteReader();
                try
                {
                    Assert.IsTrue(reader.Read());
                }
                catch (SnowflakeDbException e)
                {
                    // to make sure in a failure case command was set properly with a failed QueryId
                    Assert.AreEqual(e.QueryId, ((SnowflakeDbCommand)command).GetQueryId());
                    throw;
                }
                // Checking query id when reader succeeded
                queryId = ((SnowflakeDbDataReader)reader).GetQueryId();
                // Checking if query Id is provided on the command level as well
                Assert.AreEqual(queryId, ((SnowflakeDbCommand)command).GetQueryId());
                // Check file status
                Assert.AreEqual(expectedStatus.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check source and destination compression type
                if (t_autoCompress)
                {
                    Assert.AreEqual(t_sourceCompressionType,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                    Assert.AreEqual(t_destCompressionType,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                }
                else
                {
                    Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                    Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                }
                Assert.IsNull(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails));
            }
            return queryId;
        }

        // COPY INTO - Copy data from the stage into temp table
        private void CopyIntoTable(SnowflakeDbConnection conn, bool encloseInSingleQuotes = false)
        {
            using (var command = conn.CreateCommand())
            {
                switch (t_stageType)
                {
                    case StageType.TABLE:
                        command.CommandText = $"COPY INTO {t_schemaName}.{t_tableName}";
                        break;
                    default:
                        command.CommandText = encloseInSingleQuotes ?
                            $"COPY INTO {t_schemaName}.{t_tableName} FROM '{t_internalStagePath}/{t_fileName}'" :
                            $"COPY INTO {t_schemaName}.{t_tableName} FROM {t_internalStagePath}/{t_fileName}";
                        break;
                }

                command.ExecuteNonQuery();

                // Check contents are correct
                command.CommandText = $"SELECT * FROM {t_schemaName}.{t_tableName}";
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    for (var i = 0; i < s_colData.Length; i++)
                    {
                        Assert.AreEqual(reader.GetString(i), s_colData[i]);
                    }
                }

                // Check row count is correct
                command.CommandText = $"SELECT COUNT(*) FROM {t_schemaName}.{t_tableName}";
                Assert.AreEqual(NumberOfRows, command.ExecuteScalar());
            }
        }

        // GET - Download from the stage into local directory
        private void GetFile(DbConnection conn, bool encloseInSingleQuotes = false)
        {
            using (var command = conn.CreateCommand())
            {
                // Prepare GET query
                var getQuery = encloseInSingleQuotes ?
                    $"GET '{t_internalStagePath}/{t_fileName}' 'file://{Path.GetDirectoryName(t_outputFilePath).Replace("\\", "/")}'" :
                    $"GET {t_internalStagePath}/{t_fileName} file://{s_outputDirectory}";

                // Download file
                command.CommandText = getQuery;
                var reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());

                // Check file status
                Assert.AreEqual(ResultStatus.DOWNLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

                // Check file contents
                foreach (var line in ReadOutputFileLines())
                {
                    if (string.IsNullOrEmpty(line)) continue;
                    var values = line.Split(',');

                    for (var i = 0; i < s_colData.Length; i++)
                    {
                        Assert.AreEqual(s_colData[i], values[i]);
                    }
                }
            }
        }

        private void ProcessFile(String command, SnowflakeDbConnection connection)
        {
            switch (command)
            {
                case "GET":
                    GetFile(connection);
                    break;
                case "PUT":
                    PutFile(connection);
                    break;
            }
        }

        private static string[] ReadOutputFileLines()
        {
            using (var outputStream = File.OpenRead(t_outputFilePath))
            {
                using (var processedOutputStream = IsCompressedByTheDriver() ? Unzip(outputStream) : outputStream)
                {
                    return splitLines(processedOutputStream);
                }
            }
        }

        private static Stream Unzip(Stream stream)
        {
            using (var decompressingStream = new GZipStream(stream, CompressionMode.Decompress))
            {
                var decompressedStream = new MemoryStream();
                decompressingStream.CopyTo(decompressedStream);
                return decompressedStream;
            }
        }

        private static string[] splitLines(Stream stream)
        {
            var bytes = new byte[stream.Length];
            stream.Position = 0;
            var readBytes = stream.Read(bytes, 0, (int)stream.Length);
            Assert.AreEqual(stream.Length, readBytes);
            return Encoding.UTF8.GetString(bytes).Split('\n');
        }

        private static void PrepareFileData(string file)
        {
            // Prepare csv raw data and write to temp files
            var rawDataRow = string.Join(",", s_colData) + "\n";
            var rawData = string.Concat(Enumerable.Repeat(rawDataRow, NumberOfRows));


            using (var stream = FileOperations.Instance.Create(file))
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(rawData);
            }
            t_filesToDelete.Add(file);
        }

        private static void PrepareFileData(List<string> files)
        {
            files.ForEach(PrepareFileData);
        }

        private static void VerifyFilesAreUploaded(DbConnection conn, ICollection<string> files, string stage)
        {
            // Verify that all files have been uploaded
            using (var cmd = conn.CreateCommand())
            {
                var command = $"LIST {stage}";
                cmd.CommandText = command;
                var dbDataReader = cmd.ExecuteReader();
                var dt = new DataTable();
                dt.Load(dbDataReader);
                Assert.AreEqual(files.Count, dt.Rows.Count);
            }
        }
    }
}
