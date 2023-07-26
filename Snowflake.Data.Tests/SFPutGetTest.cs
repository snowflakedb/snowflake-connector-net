/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Linq;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;

    [TestFixture]
    class SFPutGetTest : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFPutGetTest>();
        private static string[] COL_NAME = {"C1", "C2", "C3"};
        private static string[] COL_DATA = {"FIRST", "SECOND", "THIRD"};
        private const int NUMBER_OF_ROWS = 4;

        private string _outputDirectory;

        [ThreadStatic] private static string _schemaName;
        [ThreadStatic] private static string _tableName;
        [ThreadStatic] private static string _stageName;
        [ThreadStatic] private static string _fileName;
        [ThreadStatic] private static string _inputFilePath;
        [ThreadStatic] private static string _outputFilePath;
        [ThreadStatic] private static string _internalStagePath;
        [ThreadStatic] private static StageType _stageType;
        [ThreadStatic] private static string _compressionType;
        [ThreadStatic] private static bool _autoCompress;
        [ThreadStatic] private static List<string> _filesToDelete;

        public enum StageType
        {
            USER,
            TABLE,
            NAMED
        }

        [Test]
        [Ignore("PutGetTest")]
        public void PutGetTestDone()
        {
            // Do nothing;
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            // Create temp output directory for downloaded files
            _outputDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(_outputDirectory);
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            // Delete temp output directory and downloaded files
            Directory.Delete(_outputDirectory, true);
        }
        
        [SetUp]
        public void SetUp()
        {
            // Base object's names on on worker thread id
            //var threadSuffix = TestContext.CurrentContext.WorkerId.Replace('#', '_');
            var threadSuffix = "suffix";

            _schemaName = testConfig.schema;
            _tableName = $"TABLE_{threadSuffix}";
            _stageName = $"STAGE_{threadSuffix}";
            _filesToDelete = new List<string>();

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Create temp table
                    var columnNamesWithTypes = string.Join(",", COL_NAME.Select(col => col + " STRING"));
                    command.CommandText = $"CREATE OR REPLACE TABLE {_schemaName}.{_tableName} ({columnNamesWithTypes})";
                    command.ExecuteNonQuery();

                    // Create temp stage
                    command.CommandText = $"CREATE OR REPLACE STAGE {_schemaName}.{_stageName}";
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
                    command.CommandText = $"DROP STAGE IF EXISTS {_schemaName}.{_stageName}";
                    command.ExecuteNonQuery();

                    // Drop temp table
                    command.CommandText = $"DROP TABLE IF EXISTS {_schemaName}.{_tableName}";
                    command.ExecuteNonQuery();
                }
            }
            
            // Delete temp files if necessary
            if (_filesToDelete == null)
                return;
            
            foreach (var file in _filesToDelete)
            {
                File.Delete(file);
            }
        }

        [Test]
        public void TestPutFileWildcard()
        {
            // Prepare the data files to be copied
            var prefix = Guid.NewGuid();
            var absolutePath = $"{Path.GetTempPath()}{prefix}";
            var files = new List<string> {
                $"{absolutePath}_one.csv",
                $"{absolutePath}_two.csv",
                $"{absolutePath}_three.csv"
            };
            logger.Warn("Files to be uploaded:\n" + string.Join("\n", files));
            PrepareFileData(files);
            
            // Set the PUT query variables
            _inputFilePath = $"{absolutePath}*";
            logger.Warn($"_inputFilePath: {_inputFilePath}");
            _internalStagePath = $"@{_schemaName}.{_stageName}";
            logger.Warn($"_internalStagePath: {_internalStagePath}");

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                // Verify that all files have been uploaded
                VerifyFilesAreUploaded(conn, files, _internalStagePath, $"{prefix}.*");
            }
        }
        
        [Test]
        public void TestPutDirectoryWildcard()
        {
            // Prepare the data files to be copied
            var prefix = Guid.NewGuid();
            var path = $"{Path.GetTempPath()}{prefix}";
            var files = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var filePath = $"{path}_{i}";
                Directory.CreateDirectory(filePath);
                var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{prefix}_{i}_file.csv";
                PrepareFileData(fullPath);
                files.Add(fullPath);
            }
            
            // Set the PUT query variables
            _inputFilePath = $"{path}*{Path.DirectorySeparatorChar}*.csv";
            _internalStagePath = $"@{_schemaName}.{_stageName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                PutFile(conn);
                // Verify that all files have been uploaded
                VerifyFilesAreUploaded(conn, files, _internalStagePath, $"{prefix}.*");
            }
        }
        
        [Test]
        // PutGetTest hang on AWS so ignore it for now until we find the root cause
        [IgnoreOnEnvIs("snowflake_cloud_env", new [] { "AWS" })]
        [Parallelizable(ParallelScope.All)]
        public void TestPutGetCommand(
            [Values("gzip", "bzip2", "brotli", "deflate", "raw_deflate", "zstd")] string compressionType, 
            [Values] StageType stageType,
            [Values("", "/TEST_PATH", "/DEEP/TEST_PATH")] string stagePath,
            [Values] bool autoCompress)
        {
            PrepareTest(compressionType, stageType, stagePath, autoCompress);

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
        [IgnoreOnEnvIs("snowflake_cloud_env", new [] { "AWS", "AZURE" })]
        [Parallelizable(ParallelScope.All)]
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
        
        private void PrepareTest(string compressionType, StageType stageType, string stagePath, bool autoCompress)
        {
            _stageType = stageType;
            _compressionType = compressionType;
            _autoCompress = autoCompress;
            
            // Prepare temp file name with specified file extension
            _fileName = Guid.NewGuid() + ".csv" + 
                        (_autoCompress? SFFileCompressionTypes.LookUpByName(_compressionType).FileExtension: "");
            _inputFilePath = Path.GetTempPath() + _fileName;
            _outputFilePath = $@"{_outputDirectory}/{_fileName}";
            _filesToDelete.Add(_outputFilePath);
            PrepareFileData(_inputFilePath);

            // Prepare stage name
            switch (_stageType)
            {
                case StageType.USER:
                    _internalStagePath = $"@~{stagePath}";
                    break;
                case StageType.TABLE:
                    _internalStagePath = $"@{_schemaName}.%{_tableName}{stagePath}";
                    break;
                case StageType.NAMED:
                    _internalStagePath = $"@{_schemaName}.{_stageName}{stagePath}";
                    break;
            }
        }

        // PUT - upload file from local directory to the stage
        private void PutFile(SnowflakeDbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // Prepare PUT query
                string putQuery =
                    $"PUT file://{_inputFilePath} {_internalStagePath} AUTO_COMPRESS={(_autoCompress ? "TRUE" : "FALSE")}";

                // Upload file
                command.CommandText = putQuery;
                var reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());

                // Check file status
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check source and destination compression type
                if (_autoCompress)
                {
                    Assert.AreEqual(_compressionType,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                    Assert.AreEqual(_compressionType,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                }
                else
                {
                    Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                    Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                }
            }
        }

        // COPY INTO - Copy data from the stage into temp table
        private void CopyIntoTable(SnowflakeDbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                switch (_stageType)
                {
                    case StageType.TABLE:
                        command.CommandText = $"COPY INTO {_schemaName}.{_tableName}";
                        break;
                    default:
                        command.CommandText =
                            $"COPY INTO {_schemaName}.{_tableName} FROM {_internalStagePath}/{_fileName}";
                        break;
                }

                command.ExecuteNonQuery();

                // Check contents are correct
                command.CommandText = $"SELECT * FROM {_schemaName}.{_tableName}";
                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    for (var i = 0; i < COL_DATA.Length; i++)
                    {
                        Assert.AreEqual(reader.GetString(i), COL_DATA[i]);
                    }
                }

                // Check row count is correct
                command.CommandText = $"SELECT COUNT(*) FROM {_schemaName}.{_tableName}";
                Assert.AreEqual(NUMBER_OF_ROWS, command.ExecuteScalar());
            }
        }

        // GET - Download from the stage into local directory
        private void GetFile(DbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // Prepare GET query
                var getQuery = $"GET {_internalStagePath}/{_fileName} file://{_outputDirectory}";

                // Download file
                command.CommandText = getQuery;
                var reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());

                // Check file status
                Assert.AreEqual(ResultStatus.DOWNLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

                // Check file contents
                using (var streamReader = new StreamReader(_outputFilePath))
                {
                    while (!streamReader.EndOfStream)
                    {
                        var line = streamReader.ReadLine();
                        if (line == null) continue;
                        var values = line.Split(',');

                        for (var i = 0; i < COL_DATA.Length; i++)
                        {
                            Assert.AreEqual(COL_DATA[i], values[i]);
                        }
                    }
                }
            }
        }

        private static void PrepareFileData(string file)
        {
            logger.Warn($"Preparing data for file: {file}");
            // Prepare csv raw data and write to temp files
            var rawDataRow = string.Join(",", COL_DATA) + "\n";
            var rawData = string.Concat(Enumerable.Repeat(rawDataRow, NUMBER_OF_ROWS));
            
            File.WriteAllText(file, rawData);
            logger.Warn($"is file created in the filesystem?: {File.Exists(file)}");
            _filesToDelete.Add(file);
        }

        private static void PrepareFileData(List<string> files)
        {
            files.ForEach(PrepareFileData);
        }

        private static void VerifyFilesAreUploaded(DbConnection conn, ICollection<string> files, string stage, string pattern)
        {
            // Verify that all files have been uploaded
            using (var cmd = conn.CreateCommand())
            {
                var command = $"LIST {stage} PATTERN = '{pattern}'";
                logger.Warn($"Verify command: {command}");
                cmd.CommandText = (command);
                var dbDataReader = cmd.ExecuteReader();
                var dt = new DataTable();
                dt.Load(dbDataReader);
                Assert.AreEqual(files.Count, dt.Rows.Count);
            }
        }
    }
}
