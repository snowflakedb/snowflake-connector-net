using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class SFDbCommandBehaviorTest
{
    [SFFact]
    public void TestSchemaOnlySetsDescribeOnlyTrue()
    {
        // Arrange
        var restRequester = new MockRestRequesterForDescribeOnly();
        var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
        session.Open();

        var conn = new SnowflakeDbConnection();
        conn.SfSession = session;

        var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "select 1";

        // Act
        cmd.ExecuteReader(CommandBehavior.SchemaOnly);

        // Assert
        Assert.True(restRequester.CapturedDescribeOnly);
    }

    [SFFact]
    public async Task TestSchemaOnlySetsDescribeOnlyTrueAsync()
    {
        // Arrange
        var restRequester = new MockRestRequesterForDescribeOnly();
        var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
        await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        var conn = new SnowflakeDbConnection();
        conn.SfSession = session;

        var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "select 1";

        // Act
        await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly).ConfigureAwait(false);

        // Assert
        Assert.True(restRequester.CapturedDescribeOnly);
    }

    [SFFact]
    public void TestDefaultBehaviorSetsDescribeOnlyFalse()
    {
        // Arrange
        var restRequester = new MockRestRequesterForDescribeOnly();
        var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
        session.Open();

        var conn = new SnowflakeDbConnection();
        conn.SfSession = session;

        var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "select 1";

        // Act
        cmd.ExecuteReader(CommandBehavior.Default);

        // Assert
        Assert.False(restRequester.CapturedDescribeOnly);
    }

    [SFFact]
    public async Task TestDefaultBehaviorSetsDescribeOnlyFalseAsync()
    {
        // Arrange
        var restRequester = new MockRestRequesterForDescribeOnly();
        var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
        session.Open();

        var conn = new SnowflakeDbConnection();
        conn.SfSession = session;

        var cmd = (SnowflakeDbCommand)conn.CreateCommand();
        cmd.CommandText = "select 1";

        // Act
        await cmd.ExecuteReaderAsync(CommandBehavior.Default).ConfigureAwait(false);

        // Assert
        Assert.False(restRequester.CapturedDescribeOnly);
    }

    [SFFact]
    public void TestSchemaOnlyReaderReportsRecordsAffectedAsMinusOne()
    {
        // Arrange
        var putGetData = new PutGetResponseData
        {
            rowType = new System.Collections.Generic.List<ExecResponseRowType>(),
            statementTypeId = 0
        };
        var metadata = new SFResultSetMetaData(putGetData);
        var mockResultSet = new Mock<SFBaseResultSet>
        {
            Object =
            {
                sfResultSetMetaData = metadata,
                columnCount = 0
            }
        }.Object;
        var command = new SnowflakeDbCommand();
        var reader = new SnowflakeDbDataReader(command, mockResultSet, schemaOnly: true);

        // Assert
        Assert.Equal(-1, reader.RecordsAffected);
    }

    [SFFact]
    public void TestDefaultReaderCalculatesRecordsAffected()
    {
        // Arrange
        var putGetData = new PutGetResponseData
        {
            rowType = new System.Collections.Generic.List<ExecResponseRowType>(),
            statementTypeId = 0
        };
        var metadata = new SFResultSetMetaData(putGetData);
        var mockResultSet = new Mock<SFBaseResultSet>
        {
            Object =
            {
                sfResultSetMetaData = metadata,
                columnCount = 0
            }
        }.Object;
        var command = new SnowflakeDbCommand();
        var reader = new SnowflakeDbDataReader(command, mockResultSet, schemaOnly: false);

        Assert.Equal(0, reader.RecordsAffected);
    }
}
