/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture(ResultFormat.ARROW)]
    [TestFixture(ResultFormat.JSON)]
    [Parallelizable(ParallelScope.Children)]
    class MaxLobSizeIT : SFBaseTest
    {
        private readonly ResultFormat _resultFormat;

        //private const int MaxLobSize = (128 * 1024 * 1024); // new max LOB size
        private const int MaxLobSize = (16 * 1024 * 1024); // current max LOB size
        private const int LargeSize = (MaxLobSize / 2);
        private const int MediumSize = (LargeSize / 2);
        private const int OriginSize = (MediumSize / 2);
        private const int SmallSize = 16;
        private const int LobRandomRange = 100000 + 1; // range to use for generating random numbers (0 - 100000)

        private static readonly string[] s_colName = { "C1", "C2", "C3" };
        [ThreadStatic] private static string t_tableName;
        [ThreadStatic] private static string t_insertQuery;
        [ThreadStatic] private static string t_positionalBindingInsertQuery;
        [ThreadStatic] private static string t_namedBindingInsertQuery;
        [ThreadStatic] private static string t_selectQuery;

        public MaxLobSizeIT(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }

        [SetUp]
        public void SetUp()
        {
            // Base object's names on on worker thread id
            var threadSuffix = TestContext.CurrentContext.WorkerId?.Replace('#', '_');

            t_tableName = $"LOB_TABLE_{threadSuffix}";
            var tableNameWithColumns = $"{t_tableName}({string.Join(", ", s_colName.Select(col => col))})";
            t_insertQuery = $"INSERT INTO {tableNameWithColumns} VALUES ";
            t_positionalBindingInsertQuery = $"INSERT INTO {tableNameWithColumns} VALUES (?, ?, ?)";
            t_namedBindingInsertQuery = $"INSERT INTO {tableNameWithColumns} VALUES (:1, :2, :3)";
            t_selectQuery = $"SELECT * FROM {t_tableName}";

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // Create temp table
                    var columnNamesWithTypes = $"{s_colName[0]} VARCHAR, {s_colName[1]} VARCHAR, {s_colName[2]} INT";
                    command.CommandText = $"CREATE OR REPLACE TABLE {t_tableName} ({columnNamesWithTypes})";
                    command.ExecuteNonQuery();

                    // Alter session result format
                    command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {_resultFormat}";
                    command.ExecuteNonQuery();

                    // Alter session max lob
                    //command.CommandText = "ALTER SESSION SET FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
                    //command.ExecuteNonQuery();
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
                    // Drop temp table
                    command.CommandText = $"DROP TABLE IF EXISTS {t_tableName}";
                    command.ExecuteNonQuery();
                }
            }
        }

        [Test, TestCaseSource(nameof(LobSizeTestCases))]
        public void TestSelectOnSpecifiedSize(int size)
        {
            // arrange
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"SELECT RANDSTR({size}, 124)";
                    string row = (string)command.ExecuteScalar();

                    // assert
                    Assert.AreEqual(size, row.Length);
                }
            }
        }

        [Test, TestCaseSource(nameof(LobSizeTestCases))]
        public void TestLiteralInsert(int lobSize)
        {
            // arrange
            string C1 = GenerateRandomString(lobSize);
            string C2 = GenerateRandomString(lobSize);
            int C3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_insertQuery} ('{C1}', '{C2}', '{C3}')";
                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(C1, reader.GetString(0));
                    Assert.AreEqual(C2, reader.GetString(1));
                    Assert.AreEqual(C3, reader.GetInt64(2));
                }
            }
        }

        [Test, TestCaseSource(nameof(LobSizeTestCases))]
        public void TestPositionalInsert(int lobSize)
        {
            // arrange
            string C1 = GenerateRandomString(lobSize);
            string C2 = GenerateRandomString(lobSize);
            int C3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_positionalBindingInsertQuery}";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.String;
                    p1.Value = C1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = C2;
                    command.Parameters.Add(p2);

                    var p3 = command.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.UInt32;
                    p3.Value = C3;
                    command.Parameters.Add(p3);

                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(C1, reader.GetString(0));
                    Assert.AreEqual(C2, reader.GetString(1));
                    Assert.AreEqual(C3, reader.GetInt64(2));
                }
            }
        }


        [Test, TestCaseSource(nameof(LobSizeTestCases))]
        public void TestNamedInsert(int lobSize)
        {
            // arrange
            string C1 = GenerateRandomString(lobSize);
            string C2 = GenerateRandomString(lobSize);
            int C3 = new Random().Next(LobRandomRange);

            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                using (var command = conn.CreateCommand())
                {
                    // act
                    command.CommandText = $"{t_namedBindingInsertQuery}";

                    var p1 = command.CreateParameter();
                    p1.ParameterName = "1";
                    p1.DbType = DbType.String;
                    p1.Value = C1;
                    command.Parameters.Add(p1);

                    var p2 = command.CreateParameter();
                    p2.ParameterName = "2";
                    p2.DbType = DbType.String;
                    p2.Value = C2;
                    command.Parameters.Add(p2);

                    var p3 = command.CreateParameter();
                    p3.ParameterName = "3";
                    p3.DbType = DbType.UInt32;
                    p3.Value = C3;
                    command.Parameters.Add(p3);

                    command.ExecuteNonQuery();

                    command.CommandText = t_selectQuery;
                    var reader = command.ExecuteReader();

                    // assert
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(C1, reader.GetString(0));
                    Assert.AreEqual(C2, reader.GetString(1));
                    Assert.AreEqual(C3, reader.GetInt64(2));
                }
            }
        }

        static IEnumerable<int> LobSizeTestCases = new[]
        {
            SmallSize,
            OriginSize,
            MediumSize,
            LargeSize,
            MaxLobSize
        };

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
