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
    class SFPutTest : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Test]
        [TestCase("gzip")]
        [TestCase("bzip2")]
        [TestCase("brotli")]
        [TestCase("deflate")]
        [TestCase("raw_deflate")]
        [TestCase("zstd")]
        public void TestPutCommand(string compressionType)
        {
            string DATABASE_NAME = testConfig.database;
            string SCHEMA_NAME = testConfig.schema;
            const string TEST_TEMP_TABLE_NAME = "TEST_TEMP_TABLE_NAME";

            Console.WriteLine(testConfig.host);
            Console.WriteLine(testConfig.database);
            Console.WriteLine(testConfig.schema);

            const string UPLOADED = "UPLOADED";

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
            string copyInto = $"COPY INTO {TEST_TEMP_TABLE_NAME}";
            string removeFile = $"REMOVE @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
            string dropTable = $"DROP TABLE IF EXISTS {TEST_TEMP_TABLE_NAME}";

            using (DbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                // Create a temp file with specified file extension
                string filePath = Path.GetTempPath() + Guid.NewGuid().ToString() + ".csv." + compressionType;
                // Write row data to temp file
                File.WriteAllText(filePath, ROW_DATA);

                string putQuery = $"PUT file://${filePath} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";

                // Windows user contains a '~' in the path which causes an error
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    string fileName = filePath.Substring(filePath.LastIndexOf('\\') + 1);
                    putQuery = $"PUT file://C:\\\\Users\\{Environment.UserName}\\AppData\\Local\\Temp\\{fileName} @{DATABASE_NAME}.{SCHEMA_NAME}.%{TEST_TEMP_TABLE_NAME}";
                }

                using (DbCommand command = conn.CreateCommand())
                {
                    // Create temp table
                    command.CommandText = createTable;
                    command.ExecuteNonQuery();

                    // Upload file
                    command.CommandText = putQuery;
                    DbDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        // Check file status
                        Assert.AreEqual(reader.GetString(4), UPLOADED);
                        // Check source and destination compression type
                        Assert.AreEqual(reader.GetString(6), compressionType);
                        Assert.AreEqual(reader.GetString(7), compressionType);
                    }

                    // Copy into temp table
                    command.CommandText = copyInto;
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
