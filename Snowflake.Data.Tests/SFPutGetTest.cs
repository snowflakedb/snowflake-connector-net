/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Linq;
    using Snowflake.Data.Client;
    using Snowflake.Data.Log;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.FileTransfer;

    [TestFixture]
    class SFPutGetTest : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFPutGetTest>();

        private const string TEST_TEMP_TABLE_NAME = "TEST_TEMP_TABLE_NAME";
        private const string TEST_TEMP_STAGE_NAME = "TEST_TEMP_STAGE_NAME";
        
        private const string UPLOADED = "UPLOADED";
        private const string DOWNLOADED = "DOWNLOADED";

        private static string[] COL_NAME = {"C1", "C2", "C3"};
        private static string[] COL_DATA = {"FIRST", "SECOND", "THIRD"};
        private const int NUMBER_OF_ROWS = 4;

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

        // Base schema name on worker thread id
        private string GetSchemaName() => TestContext.CurrentContext.WorkerId.Replace('#', '_');
        
        [SetUp]
        public void SetUp()
        {
            var schemaName = GetSchemaName(); 

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Create schema
                    command.CommandText = $"CREATE OR REPLACE SCHEMA {schemaName}";
                    command.ExecuteNonQuery();
                    
                    // Create temp table
                    var columnNamesWithTypes = string.Join(",", COL_NAME.Select(col => col + " STRING"));
                    command.CommandText = $"CREATE OR REPLACE TABLE {schemaName}.{TEST_TEMP_TABLE_NAME} ({columnNamesWithTypes})";
                    command.ExecuteNonQuery();

                    // Create temp stage
                    command.CommandText = $"CREATE OR REPLACE STAGE {schemaName}.{TEST_TEMP_STAGE_NAME}";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            var schemaName = GetSchemaName();

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Drop temp stage
                    command.CommandText = $"DROP STAGE IF EXISTS {schemaName}.{TEST_TEMP_STAGE_NAME}";
                    command.ExecuteNonQuery();

                    // Drop temp table
                    command.CommandText = $"DROP TABLE IF EXISTS {schemaName}.{TEST_TEMP_TABLE_NAME}";
                    command.ExecuteNonQuery();

                    // Drop schema
                    command.CommandText = $"DROP SCHEMA {schemaName}";
                    command.ExecuteNonQuery();
                }
            }
        }

        [Test]
        [Parallelizable(ParallelScope.All)]
        public void TestPutGetCommand(
            [Values("gzip", "bzip2", "brotli", "deflate", "raw_deflate", "zstd")] string compressionType, 
            [Values] StageType stageType,
            [Values("", "/TEST_PATH", "/DEEP/TEST_PATH")] string stagePath,
            [Values] bool autoCompress)
        {
            string compressionTypeExtension = SFFileCompressionTypes.LookUpByName(compressionType).FileExtension;
            string schemaName = GetSchemaName();
            
            // Prepare temp file name with specified file extension
            string fileName = Guid.NewGuid().ToString() + ".csv" + (autoCompress? compressionTypeExtension : "");
            string inputFilePath = Path.GetTempPath() + fileName;
            string outputFilePath = $@"{_outputDirectory}/{fileName}";

            // Prepare csv raw data and write to temp file
            string rawDataRow = string.Join(",", COL_DATA) + "\n";
            string rawData = string.Concat(Enumerable.Repeat(rawDataRow, NUMBER_OF_ROWS));
            File.WriteAllText(inputFilePath, rawData);
            
            // Prepare stage name
            string internalStage = "";
            switch (stageType)
            {
                case StageType.USER: 
                    internalStage = $"@~{stagePath}";
                    break;
                case StageType.TABLE: 
                    internalStage = $"@{schemaName}.%{TEST_TEMP_TABLE_NAME}{stagePath}";
                    break;
                case StageType.NAMED:
                    internalStage = $"@{schemaName}.{TEST_TEMP_STAGE_NAME}{stagePath}";
                    break;
            }
            
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // ********************************************************************
                    // ** 1. PUT - upload file from local directory to the stage
                    // ********************************************************************
                    
                    // Prepare PUT query
                    string putQuery = $"PUT file://{inputFilePath} {internalStage} AUTO_COMPRESS=" + (autoCompress ? "TRUE" : "FALSE");

                    // Upload file
                    command.CommandText = putQuery;
                    var reader = command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // Check file status
                    Assert.AreEqual(UPLOADED,
                        reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
                    // Check source and destination compression type
                    if (autoCompress)
                    {
                        Assert.AreEqual(compressionType, 
                            reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                        Assert.AreEqual(compressionType,
                            reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                    }
                    else
                    {
                        Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                            reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                        Assert.AreEqual(SFFileCompressionTypes.NONE.Name,
                            reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
                    }

                    // ********************************************************************
                    // ** 2. Copy from the stage into temp table
                    // ********************************************************************
                    switch (stageType)
                    {
                        case StageType.TABLE:
                            command.CommandText = $"COPY INTO {schemaName}.{TEST_TEMP_TABLE_NAME}";
                            break;
                        default:
                            command.CommandText = $"COPY INTO {schemaName}.{TEST_TEMP_TABLE_NAME} FROM {internalStage}/{fileName}";
                            break;
                    }
                    command.ExecuteNonQuery();

                    // Check contents are correct
                    command.CommandText = $"SELECT * FROM {schemaName}.{TEST_TEMP_TABLE_NAME}";
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        for (var i = 0; i < COL_DATA.Length; i++)
                        {
                            Assert.AreEqual(reader.GetString(i), COL_DATA[i]);
                        }
                    }

                    // Check row count is correct
                    command.CommandText = $"SELECT COUNT(*) FROM {schemaName}.{TEST_TEMP_TABLE_NAME}";
                    Assert.AreEqual(NUMBER_OF_ROWS, command.ExecuteScalar());

                    // ********************************************************************
                    // ** 3. GET - download from the stage into local directory
                    // ********************************************************************

                    // Prepare GET query
                    string getQuery = $"GET {internalStage}/{fileName} file://{_outputDirectory}";

                    // Download file
                    command.CommandText = getQuery;
                    reader = command.ExecuteReader();
                    Assert.IsTrue(reader.Read());

                    // Check file status
                    Assert.AreEqual(DOWNLOADED, reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

                    // Check file contents
                    using (var streamReader = new StreamReader(outputFilePath))
                    {
                        while (!streamReader.EndOfStream)
                        {
                            var line = streamReader.ReadLine();
                            var values = line.Split(',');

                            for (var i = 0; i < COL_DATA.Length; i++)
                            {
                                Assert.AreEqual(COL_DATA[i], values[i]);
                            }
                        }
                    }

                    // Remove file from staging
                    command.CommandText = $"REMOVE {internalStage}/{fileName}";
                    command.ExecuteNonQuery();
                }
            }

            // Delete temp files
            File.Delete(outputFilePath);
            File.Delete(inputFilePath);
        }
    }
}
