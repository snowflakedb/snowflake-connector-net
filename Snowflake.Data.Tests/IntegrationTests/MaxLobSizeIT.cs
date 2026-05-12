using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    class MaxLobSizeIT : SFBaseTest
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public MaxLobSizeIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        private ResultFormat _resultFormat;

        //private const int MaxLobSize = (128 * 1024 * 1024); // new max LOB size
        private const int MaxLobSize = (16 * 1024 * 1024); // current max LOB size
        private const int MediumSize = (MaxLobSize / 4);
        private const int OriginSize = (MediumSize / 2);
        private const int LobRandomRange = 100000 + 1; // range to use for generating random numbers (0 - 100000)

        private static string s_outputDirectory;
        private static readonly string[] s_colName = { "C1", "C2", "C3" };
        [ThreadStatic] private static string t_tableName;
        [ThreadStatic] private static string t_insertQuery;
        [ThreadStatic] private static string t_positionalBindingInsertQuery;
        [ThreadStatic] private static string t_namedBindingInsertQuery;
        [ThreadStatic] private static string t_selectQuery;
        [ThreadStatic] private static string t_fileName;
        [ThreadStatic] private static string t_inputFilePath;
        [ThreadStatic] private static string t_outputFilePath;
        [ThreadStatic] private static List<string> t_filesToDelete;
        [ThreadStatic] private static string[] t_colData;
        public static void OneTimeSetUp()
        {
            // Create temp output directory for downloaded files
            s_outputDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(s_outputDirectory);
        }
        public static void OneTimeTearDown()
        {
            // Delete temp output directory and downloaded files
            Directory.Delete(s_outputDirectory, true);
        }
        public void SetUp()
        {
            // Base object's names on worker thread id
            var threadSuffix = Thread.CurrentThread.ManagedThreadId.ToString()?.Replace('#', '_');

            t_tableName = $"LOB_TABLE_{threadSuffix}";
            var tableNameWithColumns = $"{t_tableName}({string.Join(", ", s_colName.Select(col => col))})";
            t_insertQuery = $"INSERT INTO {tableNameWithColumns} VALUES ";
            t_positionalBindingInsertQuery = $"INSERT INTO {tableNameWithColumns} VALUES (?, ?, ?)";
            t_namedBindingInsertQuery = $"INSERT INTO {tableNameWithColumns} VALUES (:1, :2, :3)";
            t_selectQuery = $"SELECT * FROM {t_tableName}";
            t_filesToDelete = new List<string>();

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Create temp table
                    var columnNamesWithTypes = $"{s_colName[0]} VARCHAR({MaxLobSize}), {s_colName[1]} VARCHAR({MaxLobSize}), {s_colName[2]} INT";
                    command.CommandText = $"CREATE OR REPLACE TABLE {t_tableName} ({columnNamesWithTypes})";
                    command.ExecuteNonQuery();
                }
            }
        }
        public void TearDown()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Drop temp table
                    command.CommandText = $"DROP TABLE IF EXISTS {t_tableName}";
                    command.ExecuteNonQuery();
                }
            }

            if (t_filesToDelete != null)
            {
                foreach (var file in t_filesToDelete)
                {
                    File.Delete(file);
                }
            }
        }
        [IgnoreOnJenkinsTheory, MemberData(nameof(SelectOnSpecifiedSizeTestCases))]
        public void TestSelectOnSpecifiedSize(ResultFormat resultFormat, int size)
        {
            // arrange
            _resultFormat = resultFormat;

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"SELECT RANDSTR({size}, 124)";
                    string row = (string)command.ExecuteScalar();

                    // assert
                    Assert.Equal(size, row.Length);
                }
            }
        }


        [IgnoreOnJenkinsTheory, MemberData(nameof(LiteralInsertTestCases))]
        public void TestLiteralInsert(ResultFormat resultFormat, int lobSize)
        {
            // arrange
            _resultFormat = resultFormat;
            var c1 = GenerateRandomString(lobSize);
            var c2 = GenerateRandomString(lobSize);
            var c3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                AlterSessionSettings(conn);

                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_insertQuery} ('{c1}', '{c2}', '{c3}')";
                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.True(reader.Read());
                    Assert.Equal(c1, reader.GetString(0));
                    Assert.Equal(c2, reader.GetString(1));
                    Assert.Equal(c3, reader.GetInt64(2));
                    CheckColumnMetadata(reader);
                }
            }
        }

        [IgnoreOnJenkinsTheory, MemberData(nameof(PositionalInsertTestCases))]
        public void TestPositionalInsert(ResultFormat resultFormat, int lobSize)
        {
            // arrange
            _resultFormat = resultFormat;
            var c1 = GenerateRandomString(lobSize);
            var c2 = GenerateRandomString(lobSize);
            var c3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                AlterSessionSettings(conn);

                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_positionalBindingInsertQuery}";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.String;
                    p1.Value = c1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = c2;
                    command.Parameters.Add(p2);

                    var p3 = command.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.UInt32;
                    p3.Value = c3;
                    command.Parameters.Add(p3);

                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.True(reader.Read());
                    Assert.Equal(c1, reader.GetString(0));
                    Assert.Equal(c2, reader.GetString(1));
                    Assert.Equal(c3, reader.GetInt64(2));
                    CheckColumnMetadata(reader);
                }
            }
        }


        [IgnoreOnJenkinsTheory, MemberData(nameof(NamedInsertTestCases))]
        public void TestNamedInsert(ResultFormat resultFormat, int lobSize)
        {
            // arrange
            _resultFormat = resultFormat;
            var c1 = GenerateRandomString(lobSize);
            var c2 = GenerateRandomString(lobSize);
            var c3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                AlterSessionSettings(conn);

                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_namedBindingInsertQuery}";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.String;
                    p1.Value = c1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = c2;
                    command.Parameters.Add(p2);

                    var p3 = command.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.UInt32;
                    p3.Value = c3;
                    command.Parameters.Add(p3);

                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.True(reader.Read());
                    Assert.Equal(c1, reader.GetString(0));
                    Assert.Equal(c2, reader.GetString(1));
                    Assert.Equal(c3, reader.GetInt64(2));
                    CheckColumnMetadata(reader);
                }
            }
        }

        [IgnoreOnJenkinsTheory, MemberData(nameof(PutGetCommandTestCases))]
        public void TestPutGetCommand(ResultFormat resultFormat, int lobSize)
        {
            // arrange
            _resultFormat = resultFormat;
            var c1 = GenerateRandomString(lobSize);
            var c2 = GenerateRandomString(lobSize);
            var c3 = new Random().Next(LobRandomRange);
            t_colData = new string[] { c1, c2, c3.ToString() };

            PrepareTest();

            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                AlterSessionSettings(conn);

                PutFile(conn);
                CopyIntoTable(conn);
                GetFile(conn);
            }
        }

        static IEnumerable<int> LobSizeTestCases = new[]
        {
            OriginSize,
            MediumSize,
            MaxLobSize
        };

        static IEnumerable<ResultFormat> ResultFormats => new[]
            { ResultFormat.ARROW, ResultFormat.JSON };

        static IEnumerable<object[]> CombinedTestCases(string baseTestName)
        {
            foreach (var resultFormat in ResultFormats)
            {
                foreach (var lobSize in LobSizeTestCases)
                {
                    yield return new object[] { resultFormat, lobSize };
                }
            }
        }

        static IEnumerable<object[]> SelectOnSpecifiedSizeTestCases() =>
            CombinedTestCases(nameof(TestSelectOnSpecifiedSize));

        static IEnumerable<object[]> LiteralInsertTestCases() =>
            CombinedTestCases(nameof(TestLiteralInsert));

        static IEnumerable<object[]> PositionalInsertTestCases() =>
            CombinedTestCases(nameof(TestPositionalInsert));

        static IEnumerable<object[]> NamedInsertTestCases() =>
            CombinedTestCases(nameof(TestNamedInsert));

        static IEnumerable<object[]> PutGetCommandTestCases() =>
            CombinedTestCases(nameof(TestPutGetCommand));

        void PrepareTest()
        {
            t_fileName = $"{Guid.NewGuid()}.csv";
            t_inputFilePath = Path.GetTempPath() + t_fileName;

            var data = $"{t_colData[0]},{t_colData[1]},{t_colData[2]}";
            using (var stream = FileOperations.Instance.Create(t_inputFilePath))
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(data);
            }

            t_outputFilePath = $@"{s_outputDirectory}/{t_fileName}";
            t_filesToDelete.Add(t_inputFilePath);
            t_filesToDelete.Add(t_outputFilePath);
        }

        void PutFile(SnowflakeDbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // arrange
                command.CommandText = $"PUT file://{t_inputFilePath} @%{t_tableName} AUTO_COMPRESS=FALSE";

                // act
                var reader = command.ExecuteReader();

                // assert
                Assert.True(reader.Read());
                Assert.Equal(ResultStatus.UPLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            }
        }

        private void CopyIntoTable(SnowflakeDbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // arrange
                command.CommandText = $"COPY INTO {t_tableName}";

                // act
                command.ExecuteNonQuery();

                // arrange
                command.CommandText = $"SELECT * FROM {t_tableName}";

                // act
                var reader = command.ExecuteReader();

                // assert
                Assert.True(reader.Read());
                AssertOnColData(reader, 0);
                AssertOnColData(reader, 1);
                AssertOnColData(reader, 2);
                CheckColumnMetadata(reader);
            }
        }

        private static void AssertOnColData(DbDataReader reader, int index)
        {
            var actual = reader.GetString(index);
            var substringLength = t_colData[index].Length > 20 ? 20 : t_colData[index].Length;
            Assert.True(t_colData[index] == actual, $"Expected strings to be equal. Expected: '{t_colData[index].Substring(0, substringLength)}' [...], got '{actual}' instead.");
        }

        private void GetFile(DbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // arrange
                command.CommandText = $"GET @%{t_tableName}/{t_fileName} file://{s_outputDirectory}";

                // act
                var reader = command.ExecuteReader();

                // assert
                Assert.True(reader.Read());
                Assert.Equal(ResultStatus.DOWNLOADED.ToString(),
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

                // arrange
                var content = File.ReadAllText(t_outputFilePath).Split(',');

                // assert
                Assert.Equal(t_colData[0], content[0]);
                Assert.Equal(t_colData[1], content[1]);
                Assert.Equal(t_colData[2], content[2]);
            }
        }

        private void AlterSessionSettings(SnowflakeDbConnection conn)
        {
            using (var command = conn.CreateCommand())
            {
                // Alter session result format
                command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {_resultFormat}";
                command.ExecuteNonQuery();

                //// Alter session max lob
                //command.CommandText = "ALTER SESSION SET FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
                //command.ExecuteNonQuery();
                //command.CommandText = "alter session set ALLOW_LARGE_LOBS_IN_EXTERNAL_SCAN = true";
                //command.ExecuteNonQuery();
            }
        }

        private void CheckColumnMetadata(DbDataReader reader)
        {
            var dataTable = reader.GetSchemaTable();

            DataRow row = dataTable.Rows[0];
            Assert.Equal(s_colName[0], row[SchemaTableColumn.ColumnName]);
            Assert.Equal(0, row[SchemaTableColumn.ColumnOrdinal]);
            Assert.Equal(MaxLobSize, row[SchemaTableColumn.ColumnSize]);
            Assert.Equal(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);

            row = dataTable.Rows[1];
            Assert.Equal(s_colName[1], row[SchemaTableColumn.ColumnName]);
            Assert.Equal(1, row[SchemaTableColumn.ColumnOrdinal]);
            Assert.Equal(MaxLobSize, row[SchemaTableColumn.ColumnSize]);
            Assert.Equal(SFDataType.TEXT, (SFDataType)row[SchemaTableColumn.ProviderType]);

            row = dataTable.Rows[2];
            Assert.Equal(s_colName[2], row[SchemaTableColumn.ColumnName]);
            Assert.Equal(2, row[SchemaTableColumn.ColumnOrdinal]);
            Assert.Equal(0, row[SchemaTableColumn.ColumnSize]);
            Assert.Equal(SFDataType.FIXED, (SFDataType)row[SchemaTableColumn.ProviderType]);
        }

        private static string GenerateRandomString(int sizeInBytes)
        {
            int bufferSize = sizeInBytes / 2;
            Random rand = new Random();
            Byte[] bytes = new Byte[bufferSize];
            rand.NextBytes(bytes);

            // Convert to hex and remove the '-' character for the correct string length
            return BitConverter.ToString(bytes).Replace("-", "");
        }
    }
}
