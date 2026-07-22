using System;
using System.Data;
using System.Threading.Tasks;
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
    private const string BigNonIntNumber = "9999999999999999999999999999999.9";

    [SFTheory]
    [InlineData(BigNumber, 38, 0)]
    [InlineData(BigNonIntNumber, 37, 1)]
    public async Task TestGetValueReturnsStringForOverflowWhenFlagIsSet(string insert, int precision, int scale)
    {
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        using var conn = OpenConnectionWithFlag();
        await _fixture.CreateOrReplaceTable(conn, tableName, [$"cola NUMBER({precision},{scale})"]).ConfigureAwait(false);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{insert}'::NUMBER({precision},{scale}))";
        await cmd1.ExecuteNonQueryAsync().ConfigureAwait(false);

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        Assert.True(await reader.ReadAsync().ConfigureAwait(false));
        var value = reader.GetValue(0);
        Assert.IsType<string>(value);
        Assert.Equal(insert, (string)value);
    }

    [SFFact]
    public async Task TestGetValueThrowsOverflowWhenFlagIsNotSet()
    {
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        using var conn = OpenConnectionWithoutFlag();
        await _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]).ConfigureAwait(false);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        await cmd1.ExecuteNonQueryAsync().ConfigureAwait(false);

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        Assert.True(await reader.ReadAsync().ConfigureAwait(false));
        Assert.Throws<OverflowException>(() => reader.GetValue(0));
    }

    [SFFact]
    public async Task TestGetStringAlwaysReturnsRawString()
    {
        using var conn = OpenConnectionWithoutFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        await _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]).ConfigureAwait(false);
        var cmd1 = conn.CreateCommand();
        cmd1.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        await cmd1.ExecuteNonQueryAsync().ConfigureAwait(false);

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT cola FROM {tableName}";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        Assert.True(await reader.ReadAsync().ConfigureAwait(false));
        Assert.Equal(BigNumber, reader.GetString(0));
    }

    [SFFact]
    public async Task TestDbDataAdapterFillThrowsOverflowWhenFlagIsNotSet()
    {
        using var conn = OpenConnectionWithoutFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        await _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]).ConfigureAwait(false);
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        var adapter = new SnowflakeDbDataAdapter($"SELECT cola FROM {tableName}", conn);
        var dataTable = new DataTable();
        Assert.Throws<OverflowException>(() => adapter.Fill(dataTable));
    }

    [SFFact]
    public async Task TestDbDataAdapterFillWithPreTypedStringColumnStoresOverflowAsString()
    {
        using var conn = OpenConnectionWithFlag();
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;
        await _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]).ConfigureAwait(false);
        var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {tableName} VALUES ('{BigNumber}'::NUMBER(38,0))";
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

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
    public async Task TestMixedRowsViaReader()
    {
        const string SmallNumber = "42";
        var tableNameSuffix = Guid.NewGuid().ToString("N");
        var tableName = _fixture.TableNameBaseName + "_" + tableNameSuffix;

        using var conn = OpenConnectionWithFlag();
        await _fixture.CreateOrReplaceTable(conn, tableName, ["cola NUMBER(38,0)"]).ConfigureAwait(false);
        var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"INSERT INTO {tableName} VALUES ('{SmallNumber}'::NUMBER(38,0)), ('{BigNumber}'::NUMBER(38,0))";
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

        cmd.CommandText = $"SELECT cola FROM {tableName} ORDER BY cola";
        using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
        Assert.True(await reader.ReadAsync().ConfigureAwait(false));
        // Small value fits — JSON returns Int64, Arrow returns decimal
        // (NUMBER(38,0) columns always use Decimal128 in Arrow).
        var smallVal = reader.GetValue(0);
        if (_resultFormat == ResultFormat.JSON)
            Assert.Equal(42L, smallVal);
        else
            Assert.Equal(42m, smallVal);

        Assert.True(await reader.ReadAsync().ConfigureAwait(false));
        // Large value overflows — returned as string
        Assert.IsType<string>(reader.GetValue(0));
        Assert.Equal(BigNumber, (string)reader.GetValue(0));

        Assert.False(await reader.ReadAsync().ConfigureAwait(false));
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
