﻿/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections.Generic;
using System.Data;
using System.Data.Common;

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
        private static readonly string[] s_colName = {"C1", "C2", "C3"};
        private static readonly string[] s_colData = {"FIRST", "SECOND", "THIRD"};
        private const int NumberOfRows = 4;
        
        [ThreadStatic] private static string t_schemaName;
        [ThreadStatic] private static string t_tableName;
        [ThreadStatic] private static string t_stageName;
        [ThreadStatic] private static string t_fileName;
        [ThreadStatic] private static string t_inputFilePath;
        [ThreadStatic] private static string t_outputFilePath;
        [ThreadStatic] private static string t_internalStagePath;
        [ThreadStatic] private static StageType t_stageType;
        [ThreadStatic] private static string t_compressionType;
        [ThreadStatic] private static bool t_autoCompress;
        [ThreadStatic] private static List<string> t_filesToDelete;
        
        private string _outputDirectory;

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
            var threadSuffix = TestContext.CurrentContext.WorkerId.Replace('#', '_');

            t_schemaName = testConfig.schema;
            t_tableName = $"TABLE_{threadSuffix}";
            t_stageName = $"STAGE_{threadSuffix}";
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
            if (t_filesToDelete == null)
                return;
            
            foreach (var file in t_filesToDelete)
            {
                File.Delete(file);
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
            t_stageType = stageType;
            t_compressionType = compressionType;
            t_autoCompress = autoCompress;
            
            // Prepare temp file name with specified file extension
            t_fileName = Guid.NewGuid() + ".csv" + 
                        (t_autoCompress? SFFileCompressionTypes.LookUpByName(t_compressionType).FileExtension: "");
            t_inputFilePath = Path.GetTempPath() + t_fileName;
            t_outputFilePath = $@"{_outputDirectory}/{t_fileName}";
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
                    t_internalStagePath = $"@{t_schemaName}.{t_stageName}{stagePath}";
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
                    $"PUT file://{t_inputFilePath} {t_internalStagePath} AUTO_COMPRESS={(t_autoCompress ? "TRUE" : "FALSE")}";

                // Upload file
                command.CommandText = putQuery;
                var reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());

                // Check file status
                Assert.AreEqual(ResultStatus.UPLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                // Check source and destination compression type
                if (t_autoCompress)
                {
                    Assert.AreEqual(t_compressionType,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                    Assert.AreEqual(t_compressionType,
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
                switch (t_stageType)
                {
                    case StageType.TABLE:
                        command.CommandText = $"COPY INTO {t_schemaName}.{t_tableName}";
                        break;
                    default:
                        command.CommandText =
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
        private void GetFile(DbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // Prepare GET query
                var getQuery = $"GET {t_internalStagePath}/{t_fileName} file://{_outputDirectory}";

                // Download file
                command.CommandText = getQuery;
                var reader = command.ExecuteReader();
                Assert.IsTrue(reader.Read());

                // Check file status
                Assert.AreEqual(ResultStatus.DOWNLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

                // Check file contents
                using (var streamReader = new StreamReader(t_outputFilePath))
                {
                    while (!streamReader.EndOfStream)
                    {
                        var line = streamReader.ReadLine();
                        if (line == null) continue;
                        var values = line.Split(',');

                        for (var i = 0; i < s_colData.Length; i++)
                        {
                            Assert.AreEqual(s_colData[i], values[i]);
                        }
                    }
                }
            }
        }

        private static void PrepareFileData(string file)
        {
            // Prepare csv raw data and write to temp files
            var rawDataRow = string.Join(",", s_colData) + "\n";
            var rawData = string.Concat(Enumerable.Repeat(rawDataRow, NumberOfRows));
            
            File.WriteAllText(file, rawData);
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
