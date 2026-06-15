using System;
using System.Data;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class NumberOverflowAsStringITJson : NumberOverflowAsStringIT
{
    public NumberOverflowAsStringITJson(SFBaseTestAsyncFixture fixture) : base(ResultFormat.JSON, fixture)
    {
    }
}

public sealed class NumberOverflowAsStringITArrow : NumberOverflowAsStringIT
{
    public NumberOverflowAsStringITArrow(SFBaseTestAsyncFixture fixture) : base(ResultFormat.ARROW, fixture)
    {
    }
}

public abstract class NumberOverflowAsStringIT : SFBaseTestAsync
{
    private readonly ResultFormat _resultFormat;
    private readonly SFBaseTestAsyncFixture _fixture;

    protected NumberOverflowAsStringIT(ResultFormat resultFormat, SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _resultFormat = resultFormat;
        _fixture = fixture;
    }

    // A 32-digit integer that exceeds both Int64.MaxValue (19 digits, JSON path)
    // and System.Decimal.MaxValue (29 digits, Arrow Decimal128 path).
    // Snowflake stores it exactly in a NUMBER(38,0) column.
    private const string BigNumber = "99999999999999999999999999999999";
    private const string BigNonIntNumber = "99999999999999999999.9999999999";

    [SFTheory]
    [InlineData(BigNumber, 38, 0)]
    [InlineData(BigNonIntNumber, 28, 10)]
    public void TestGetValueReturnsStringForOverflowWhenFlagIsSet(string insert, int precision, int scale)
    {
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        using var conn = OpenConnectionWithFlag();
        _fixture.CreateOrReplaceTable(conn, tableName, [$"cola NUMBER({precision},{scale})"]);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{insert}'::NUMBER({precision},{scale}))";
        cmd1.ExecuteNonQuery();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var value = reader.GetValue(0);
        Assert.IsType<string>(value);
        Assert.Equal(insert, (string)value);
    }

    [SFFact]
    public void TestGetValueThrowsOverflowWhenFlagIsNotSet()
    {
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        using var conn = OpenConnectionWithoutFlag();
        _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        cmd1.ExecuteNonQuery();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Throws<OverflowException>(() => reader.GetValue(0));
    }

    [SFFact]
    public void TestGetStringAlwaysReturnsRawString()
    {
        using var conn = OpenConnectionWithoutFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        cmd1.ExecuteNonQuery();

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(BigNumber, reader.GetString(0));
    }

    [SFFact]
    public void TestDbDataAdapterFillThrowsOverflowWhenFlagIsNotSet()
    {
        using var conn = OpenConnectionWithoutFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]);
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        cmd.ExecuteNonQuery();

        var adapter = new SnowflakeDbDataAdapter($"SELECT cola FROM {tableName}", conn);
        var dataTable = new DataTable();
        Assert.Throws<OverflowException>(() => adapter.Fill(dataTable));
    }

    [SFFact]
    public void TestDbDataAdapterFillWithPreTypedStringColumnStoresOverflowAsString()
    {
        using var conn = OpenConnectionWithFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]);
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        cmd.ExecuteNonQuery();

        // Pre-declare the column as string so the adapter can store the
        // string returned by GetValue() without an Int64 coercion attempt.
        var dataTable = new DataTable();
        dataTable.Columns.Add("COLA", typeof(string));

        var adapter = new SnowflakeDbDataAdapter($"SELECT cola FROM {tableName}", conn);
        adapter.Fill(dataTable);

        Assert.Equal(1, dataTable.Rows.Count);
        Assert.Equal(BigNumber, (string)dataTable.Rows[0]["COLA"]);
    }


    [SFFact]
    public void TestMixedRowsViaReader()
    {
        const string SmallNumber = "42";
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;

        using var conn = OpenConnectionWithFlag();
        _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]);
        var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO {tableName} VALUES ('{SmallNumber}'::NUMBER(38,0)), ('{BigNumber}'::NUMBER(38,0))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = $"SELECT cola FROM {tableName} ORDER BY cola";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        // Small value fits — JSON returns Int64, Arrow returns decimal
        // (NUMBER(38,0) columns always use Decimal128 in Arrow).
        var smallVal = reader.GetValue(0);
        if (_resultFormat == ResultFormat.JSON)
            Assert.Equal(42L, smallVal);
        else
            Assert.Equal(42m, smallVal);

        Assert.True(reader.Read());
        // Large value overflows — returned as string
        Assert.IsType<string>(reader.GetValue(0));
        Assert.Equal(BigNumber, (string)reader.GetValue(0));

        Assert.False(reader.Read());
    }

    private SnowflakeDbConnection OpenConnectionWithFlag()
    {
        var conn = new SnowflakeDbConnection(_fixture.ConnectionString + "ALLOW_NUMBER_OVERFLOW_AS_STRING=true;");
        conn.Open();
        SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
        return conn;
    }

    private SnowflakeDbConnection OpenConnectionWithoutFlag()
    {
        var conn = new SnowflakeDbConnection(_fixture.ConnectionString);
        conn.Open();
        SessionParameterAlterer.SetResultFormat(conn, _resultFormat);
        return conn;
    }
}
