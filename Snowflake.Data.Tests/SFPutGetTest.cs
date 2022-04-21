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

    [TestFixture]
    class SFPutGetTest : SFBaseTest
    {
        [Test]
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

            const string SNOWFLAKE_FULL = "'SNOWFLAKE_FULL'";
            const string SNOWFLAKE_SSE = "'SNOWFLAKE_SSE'";

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

            string copyIntoTable = $"COPY INTO {TEST_TEMP_TABLE_NAME}";
            string copyIntoStage = $"COPY INTO {TEST_TEMP_TABLE_NAME} FROM @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";

            string removeFile = $"REMOVE @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
            string removeFileUser = $"REMOVE @~/";

            string dropStage = $"DROP STAGE IF EXISTS {TEST_TEMP_STAGE_NAME}";
            string dropTable = $"DROP TABLE IF EXISTS {TEST_TEMP_TABLE_NAME}";

            string[] stageTypes = { USER_STAGE, TABLE_STAGE, NAMED_STAGE };
            string[] autoCompressTypes = { TRUE_COMPRESS, FALSE_COMPRESS };
            string[] encryptionTypes = { SNOWFLAKE_FULL, SNOWFLAKE_SSE };

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Create a temp file with specified file extension
                string filePath = Path.GetTempPath() + Guid.NewGuid().ToString() + ".csv";
                // Write row data to temp file
                File.WriteAllText(filePath, ROW_DATA);

                string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                Directory.CreateDirectory(tempDirectory);

                string createStage = $"create or replace stage {TEST_TEMP_STAGE_NAME}";

                putQuery = $"PUT file://{filePath} @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";
                createStage += $" ENCRYPTION=(TYPE={SNOWFLAKE_FULL})";
                

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
                    putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @{DATABASE_NAME}.{SCHEMA_NAME}.{TEST_TEMP_STAGE_NAME}";
                    createStage += $" ENCRYPTION=(TYPE={SNOWFLAKE_FULL})";
                }

                // Add PUT compress option
                putQuery += $" AUTO_COMPRESS={FALSE_COMPRESS}";

                using (DbCommand command = conn.CreateCommand())
                {
                    // Create temp table
                    command.CommandText = createTable;
                    command.ExecuteNonQuery();

                    // Create temp stage
                    command.CommandText = createStage;
                    Console.WriteLine("STAGE QUERY: " + createStage);
                    command.ExecuteNonQuery();

                    // Upload file
                    command.CommandText = putQuery;
                    Console.WriteLine("PUT QUERY: " + putQuery);
                    DbDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        // Check file status
                        Assert.AreEqual(reader.GetString(4), UPLOADED);
                        // Check source and destination compression type
                        Assert.AreEqual(reader.GetString(6), "none");
                        Assert.AreEqual(reader.GetString(7), "none");
                    }

                    // Copy into temp table
                    command.CommandText = copyIntoStage;
                    Console.WriteLine("COPY INTO QUERY: " + command.CommandText);
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
                    Console.WriteLine("GET QUERY: " + getQuery);
                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        // Check file status
                        Assert.AreEqual(reader.GetString(4), DOWNLOADED);
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
