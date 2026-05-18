using System;
using System.Data;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests;

[TestFixture(ResultFormat.JSON)]
[TestFixture(ResultFormat.ARROW)]
public sealed class NumberOverflowAsStringIT : SFBaseTest
{
    private readonly ResultFormat _resultFormat;

    public NumberOverflowAsStringIT(ResultFormat resultFormat)
    {
        _resultFormat = resultFormat;
    }

    // A 32-digit integer that exceeds both Int64.MaxValue (19 digits, JSON path)
    // and System.Decimal.MaxValue (29 digits, Arrow Decimal128 path).
    // Snowflake stores it exactly in a NUMBER(38,0) column.
    private const string BigNumber = "99999999999999999999999999999999";
    private const string BigNonIntNumber = "99999999999999999999.9999999999";

    [TestCase(BigNumber, 38, 0)]
    [TestCase(BigNonIntNumber, 28, 10)]
    public void TestGetValueReturnsStringForOverflowWhenFlagIsSet(string insert, int precision, int scale)
    {
        using (var conn = OpenConnectionWithFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { $"cola NUMBER({precision},{scale})" });
            var cmd1 = conn.CreateCommand();
            cmd1.CommandText = $"INSERT INTO {TableName} VALUES ('{insert}'::NUMBER({precision},{scale}))";
            cmd1.ExecuteNonQuery();

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT cola FROM {TableName}";
            using (var reader = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                var value = reader.GetValue(0);
                Assert.IsInstanceOf<string>(value, "Expected string when overflow flag is set");
                Assert.AreEqual(insert, (string)value);
            }
        }
    }

    [Test]
    public void TestGetValueThrowsOverflowWhenFlagIsNotSet()
    {
        using (var conn = OpenConnectionWithoutFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER(38,0)" });
            var cmd1 = conn.CreateCommand();
            cmd1.CommandText = $"INSERT INTO {TableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
            cmd1.ExecuteNonQuery();

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT cola FROM {TableName}";
            using (var reader = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                Assert.Throws<OverflowException>(() => reader.GetValue(0));
            }
        }
    }

    [Test]
    public void TestGetStringAlwaysReturnsRawString()
    {
        using (var conn = OpenConnectionWithoutFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER(38,0)" });
            var cmd1 = conn.CreateCommand();
            cmd1.CommandText = $"INSERT INTO {TableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
            cmd1.ExecuteNonQuery();

            var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT cola FROM {TableName}";
            using (var reader = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(BigNumber, reader.GetString(0));
            }
        }
    }

    [Test]
    public void TestDbDataAdapterFillThrowsOverflowWhenFlagIsNotSet()
    {
        using (var conn = OpenConnectionWithoutFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER(38,0)" });
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"INSERT INTO {TableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
            cmd.ExecuteNonQuery();

            var adapter = new SnowflakeDbDataAdapter($"SELECT cola FROM {TableName}", conn);
            var dataTable = new DataTable();
            Assert.Throws<OverflowException>(() => adapter.Fill(dataTable));
        }
    }

    [Test]
    public void TestDbDataAdapterFillWithPreTypedStringColumnStoresOverflowAsString()
    {
        using (var conn = OpenConnectionWithFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER(38,0)" });
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"INSERT INTO {TableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
            cmd.ExecuteNonQuery();

            // Pre-declare the column as string so the adapter can store the
            // string returned by GetValue() without an Int64 coercion attempt.
            var dataTable = new DataTable();
            dataTable.Columns.Add("COLA", typeof(string));

            var adapter = new SnowflakeDbDataAdapter($"SELECT cola FROM {TableName}", conn);
            adapter.Fill(dataTable);

            Assert.AreEqual(1, dataTable.Rows.Count);
            Assert.AreEqual(BigNumber, (string)dataTable.Rows[0]["COLA"]);
        }
    }


    [Test]
    public void TestMixedRowsViaReader()
    {
        const string SmallNumber = "42";

        using (var conn = OpenConnectionWithFlag())
        {
            CreateOrReplaceTable(conn, TableName, new[] { "cola NUMBER(38,0)" });
            var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"INSERT INTO {TableName} VALUES ('{SmallNumber}'::NUMBER(38,0)), ('{BigNumber}'::NUMBER(38,0))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = $"SELECT cola FROM {TableName} ORDER BY cola";
            using (var reader = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                // Small value fits — JSON returns Int64, Arrow returns decimal
                // (NUMBER(38,0) columns always use Decimal128 in Arrow).
                var smallVal = reader.GetValue(0);
                if (_resultFormat == ResultFormat.JSON)
                    Assert.AreEqual(42L, smallVal);
                else
                    Assert.AreEqual(42m, smallVal);

                Assert.IsTrue(reader.Read());
                // Large value overflows — returned as string
                Assert.IsInstanceOf<string>(reader.GetValue(0));
                Assert.AreEqual(BigNumber, (string)reader.GetValue(0));

                Assert.IsFalse(reader.Read());
            }
        }
    }

    private SnowflakeDbConnection OpenConnectionWithFlag()
    {
        var conn = new SnowflakeDbConnection(ConnectionString + "ALLOW_NUMBER_OVERFLOW_AS_STRING=true;");
        conn.Open();
        SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
        return conn;
    }

    private SnowflakeDbConnection OpenConnectionWithoutFlag()
    {
        var conn = new SnowflakeDbConnection(ConnectionString);
        conn.Open();
        SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
        return conn;
    }
}
