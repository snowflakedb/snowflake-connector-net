/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using Snowflake.Data.Log;
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Data.Common;
    using Snowflake.Data.Core;

    [TestFixture]
    class SFPutGetTest : SFBaseTest
    {
        [Test]
        [Ignore("PutGetTest")]
        public void PutGetTestDone()
        {
            // Do nothing;
        }

        [Test]
        // PutGetTest hang on AWS so ignore it for now until we find the root cause
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AWS" })]
        [TestCase("gzip")]
        [TestCase("bzip2")]
        [TestCase("brotli")]
        [TestCase("deflate")]
        [TestCase("raw_deflate")]
        [TestCase("zstd")]
        public void TestPutGetCommand(string compressionType)
        {
            string DATABASE_NAME = testConfig.database;
            string SCHEMA_NAME = testConfig.schema;
            const string TEST_TEMP_TABLE_NAME = "TEST_TEMP_TABLE_NAME";
            const string TEST_TEMP_STAGE_NAME = "TEST_TEMP_STAGE_NAME";

            const string USER_STAGE = "USER_STAGE";
            const string TABLE_STAGE = "TABLE_STAGE";
            const string NAMED_STAGE = "NAMED_STAGE";

            const string FALSE_COMPRESS = "FALSE";
            const string TRUE_COMPRESS = "TRUE";

            const string UPLOADED = "UPLOADED";
            const string DOWNLOADED = "DOWNLOADED";

            const string COL1 = "C1";
            const string COL2 = "C2";
            const string COL3 = "C3";
            const string COL1_DATA = "FIRST";
            const string COL2_DATA = "SECOND";
            const string COL3_DATA = "THIRD";
            const string ROW_DATA =
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n";

            string createTable = $"create or replace table {TEST_TEMP_TABLE_NAME} ({COL1} STRING," +
            $"{COL2} STRING," +
            $"{COL3} STRING)";
            string createStage = $"create or replace stage {TEST_TEMP_STAGE_NAME}";

            string copyIntoTable = $"COPY INTO {TEST_TEMP_TABLE_NAME}";
            string copyIntoStage = $"COPY INTO {TEST_TEMP_TABLE_NAME} FROM @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";

            string removeFile = $"REMOVE @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
            string removeFileUser = $"REMOVE @~/";

            string dropStage = $"DROP STAGE IF EXISTS {TEST_TEMP_STAGE_NAME}";
            string dropTable = $"DROP TABLE IF EXISTS {TEST_TEMP_TABLE_NAME}";

            string[] stageTypes = { USER_STAGE, TABLE_STAGE, NAMED_STAGE };
            string[] autoCompressTypes = { FALSE_COMPRESS, TRUE_COMPRESS };

            string compressionTypeExtension = Core.FileTransfer.SFFileCompressionTypes.LookUpByName(compressionType).FileExtension;

            foreach (string stageType in stageTypes)
            {
                foreach (string autoCompressType in autoCompressTypes)
                {
                    using (DbConnection conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = ConnectionString;
                        conn.Open();

                        // Create a temp file with specified file extension
                        string filePath = Path.GetTempPath() + Guid.NewGuid().ToString() + ".csv" +
                            (autoCompressType == FALSE_COMPRESS ? "" : compressionTypeExtension);
                        // Write row data to temp file
                        File.WriteAllText(filePath, ROW_DATA);

                        string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                        Directory.CreateDirectory(tempDirectory);

                        string putQuery = "";
                        if (stageType == USER_STAGE)
                        {
                            putQuery = $"PUT file://{filePath} @~/";
                        }
                        else if (stageType == TABLE_STAGE)
                        {
                            putQuery = $"PUT file://{filePath} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
                        }
                        else if (stageType == NAMED_STAGE)
                        {
                            putQuery = $"PUT file://{filePath} @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";
                        }

                        string getQuery = $"GET @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME} file://{tempDirectory}";

                        string fileName = "";
                        string copyIntoUser = $"COPY INTO {TEST_TEMP_TABLE_NAME} FROM @~/";
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        {
                            fileName = filePath.Substring(filePath.LastIndexOf('\\') + 1);
                            removeFileUser += fileName;
                            copyIntoUser += fileName;
                        }
                        else
                        {
                            fileName = filePath.Substring(filePath.LastIndexOf('/') + 1);
                            removeFileUser += fileName;
                            copyIntoUser += fileName;
                        }

                        // Windows user contains a '~' in the path which causes an error
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                        {
                            if (stageType == USER_STAGE)
                            {
                                putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @~/";
                            }
                            else if (stageType == TABLE_STAGE)
                            {
                                putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
                            }
                            else if (stageType == NAMED_STAGE)
                            {
                                putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";
                            }
                        }

                        // Add PUT compress option
                        putQuery += $" AUTO_COMPRESS={autoCompressType}";

                        using (DbCommand command = conn.CreateCommand())
                        {
                            // Create temp table
                            command.CommandText = createTable;
                            command.ExecuteNonQuery();

                            // Create temp stage
                            command.CommandText = createStage;
                            command.ExecuteNonQuery();

                            // Upload file
                            command.CommandText = putQuery;
                            DbDataReader reader = command.ExecuteReader();
                            while (reader.Read())
                            {
                                // Check file status
                                Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus), UPLOADED);
                                // Check source and destination compression type
                                if (autoCompressType == FALSE_COMPRESS)
                                {
                                    Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType), "none");
                                    Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType), "none");
                                }
                                else
                                {
                                    Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType), compressionType);
                                    Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType), compressionType);
                                }
                            }

                            // Copy into temp table
                            if (stageType == USER_STAGE)
                            {
                                command.CommandText = copyIntoUser;
                            }
                            else if (stageType == TABLE_STAGE)
                            {
                                command.CommandText = copyIntoTable;
                            }
                            else if (stageType == NAMED_STAGE)
                            {
                                command.CommandText = copyIntoStage;
                            }
                            command.ExecuteNonQuery();

                            // Check contents are correct
                            command.CommandText = $"SELECT * FROM {TEST_TEMP_TABLE_NAME}";
                            reader = command.ExecuteReader();
                            while (reader.Read())
                            {
                                Assert.AreEqual(reader.GetString(0), COL1_DATA);
                                Assert.AreEqual(reader.GetString(1), COL2_DATA);
                                Assert.AreEqual(reader.GetString(2), COL3_DATA);
                            }

                            // Check row count is correct
                            command.CommandText = $"SELECT COUNT(*) FROM {TEST_TEMP_TABLE_NAME}";
                            Assert.AreEqual(command.ExecuteScalar(), 4);

                            // Download file
                            command.CommandText = getQuery;
                            reader = command.ExecuteReader();
                            while (reader.Read())
                            {
                                // Check file status
                                Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus), DOWNLOADED);

                                // Check file contents
                                using (var streamReader = new StreamReader($@"{tempDirectory}/{fileName}"))
                                {
                                    while (!streamReader.EndOfStream)
                                    {
                                        var line = streamReader.ReadLine();
                                        var values = line.Split(',');

                                        Assert.AreEqual(COL1_DATA, values[0]);
                                        Assert.AreEqual(COL2_DATA, values[1]);
                                        Assert.AreEqual(COL3_DATA, values[2]);
                                    }
                                }
                            }

                            // Delete downloaded files
                            Directory.Delete(tempDirectory, true);

                            // Remove files from staging
                            command.CommandText = removeFile;
                            command.ExecuteNonQuery();

                            // Remove user file from staging
                            command.CommandText = removeFileUser;
                            command.ExecuteNonQuery();

                            // Drop temp stage
                            command.CommandText = dropStage;
                            command.ExecuteNonQuery();

                            // Drop temp table
                            command.CommandText = dropTable;
                            command.ExecuteNonQuery();
                        }

                        // Delete temp file
                        File.Delete(filePath);

                        conn.Close();
                        Assert.AreEqual(ConnectionState.Closed, conn.State);
                    }
                }
            }
        }

        // Test small file upload/download with GCS_USE_DOWNSCOPED_CREDENTIAL set to true
        [Test]
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AWS", "AZURE" })]
        public void TestPutGetGcsDownscopedCredential()
        {
            string DATABASE_NAME = testConfig.database;
            string SCHEMA_NAME = testConfig.schema;
            const string TEST_TEMP_TABLE_NAME = "TEST_TEMP_TABLE_NAME";
            const string TEST_TEMP_STAGE_NAME = "TEST_TEMP_STAGE_NAME";

            const string UPLOADED = "UPLOADED";
            const string DOWNLOADED = "DOWNLOADED";

            const string COL1 = "C1";
            const string COL2 = "C2";
            const string COL3 = "C3";
            const string COL1_DATA = "FIRST";
            const string COL2_DATA = "SECOND";
            const string COL3_DATA = "THIRD";
            const string ROW_DATA =
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n" +
              COL1_DATA + "," + COL2_DATA + "," + COL3_DATA + "\n";

            string createTable = $"create or replace table {TEST_TEMP_TABLE_NAME} ({COL1} STRING," +
            $"{COL2} STRING," +
            $"{COL3} STRING)";
            string createStage = $"create or replace stage {TEST_TEMP_STAGE_NAME}";

            string copyIntoTable = $"COPY INTO {TEST_TEMP_TABLE_NAME}";

            string removeFile = $"REMOVE @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";

            string dropTable = $"DROP TABLE IF EXISTS {TEST_TEMP_TABLE_NAME}";

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString
                    + String.Format(
                    ";GCS_USE_DOWNSCOPED_CREDENTIAL=true");
                conn.Open();

                // Create a temp file
                string filePath = Path.GetTempPath() + Guid.NewGuid().ToString() + ".csv";
                // Write row data to temp file
                File.WriteAllText(filePath, ROW_DATA);

                string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                Directory.CreateDirectory(tempDirectory);

                string putQuery = $"PUT file://{filePath} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";

                string getQuery = $"GET @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME} file://{tempDirectory}";

                string fileName = "";
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    fileName = filePath.Substring(filePath.LastIndexOf('\\') + 1);
                }
                else
                {
                    fileName = filePath.Substring(filePath.LastIndexOf('/') + 1);
                }

                // Windows user contains a '~' in the path which causes an error
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
                }
                putQuery += " AUTO_COMPRESS=FALSE";

                using (DbCommand command = conn.CreateCommand())
                {
                    // Create temp table
                    command.CommandText = createTable;
                    command.ExecuteNonQuery();

                    // Create temp stage
                    command.CommandText = createStage;
                    command.ExecuteNonQuery();

                    // Upload file
                    command.CommandText = putQuery;
                    DbDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        // Check file status
                        Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus), UPLOADED);
                    }

                    // Copy into temp table
                    command.CommandText = copyIntoTable;
                    command.ExecuteNonQuery();

                    // Check contents are correct
                    command.CommandText = $"SELECT * FROM {TEST_TEMP_TABLE_NAME}";
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        Assert.AreEqual(reader.GetString(0), COL1_DATA);
                        Assert.AreEqual(reader.GetString(1), COL2_DATA);
                        Assert.AreEqual(reader.GetString(2), COL3_DATA);
                    }

                    // Check row count is correct
                    command.CommandText = $"SELECT COUNT(*) FROM {TEST_TEMP_TABLE_NAME}";
                    Assert.AreEqual(command.ExecuteScalar(), 4);

                    // Download file
                    command.CommandText = getQuery;
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        // Check file status
                        Assert.AreEqual(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus), DOWNLOADED);

                        // Check file contents
                        using (var streamReader = new StreamReader($@"{tempDirectory}/{fileName}"))
                        {
                            while (!streamReader.EndOfStream)
                            {
                                var line = streamReader.ReadLine();
                                var values = line.Split(',');

                                Assert.AreEqual(COL1_DATA, values[0]);
                                Assert.AreEqual(COL2_DATA, values[1]);
                                Assert.AreEqual(COL3_DATA, values[2]);
                            }
                        }
                    }

                    // Delete downloaded files
                    Directory.Delete(tempDirectory, true);

                    // Remove files from staging
                    command.CommandText = removeFile;
                    command.ExecuteNonQuery();

                    // Drop temp table
                    command.CommandText = dropTable;
                    command.ExecuteNonQuery();
                }

                // Delete temp file
                File.Delete(filePath);

                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }
    }
}
