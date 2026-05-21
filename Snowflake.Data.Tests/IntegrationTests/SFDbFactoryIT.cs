using System;
using System.Threading.Tasks;
using Xunit;
using System.Data;
using System.Data.Common;
using System.Threading;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFDbFactoryIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        DbProviderFactory _factory;
        DbCommand _command;
        DbConnection _connection;

        public SFDbFactoryIT(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixture = fixture;
            _factory = Snowflake.Data.Client.SnowflakeDbFactory.Instance;
            _command = _factory.CreateCommand();
            _connection = _factory.CreateConnection();
            _connection.ConnectionString = _fixture.ConnectionString;
            _connection.Open();
        }

        public void Dispose()
        {
            _connection.Close();
        }

        [SFFact]
        public async Task TestSimpleDbFactory()
        {
            // set commnad's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            object res = await _command.ExecuteScalarAsync();

            Assert.Equal(1L, res);
        }

        [SFFact]
        public async Task TestDbFactoryWithParameter()
        {
            int expectedIntValue = 1;

            DbParameter parameter = _factory.CreateParameter();
            parameter.ParameterName = "1";
            parameter.Value = expectedIntValue;
            parameter.DbType = DbType.Int16;
            _command.Parameters.Add(parameter);

            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select ?";

            var result = await _command.ExecuteScalarAsync();

            Assert.Equal((long)expectedIntValue, result);
        }

        [SFFact]
        public async Task TestDbFactoryWithConnectionStringBuilder()
        {
            DbConnectionStringBuilder builder = _factory.CreateConnectionStringBuilder();
            builder.ConnectionString = _fixture.ConnectionString;

            _connection.ConnectionString = builder.ConnectionString;
            await _connection.OpenAsync(CancellationToken.None);

            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            var result = await _command.ExecuteScalarAsync();

            Assert.Equal(1L, result);
        }

        [SFFact]
        public async Task TestDbFactoryWithCommandBuilderAndAdapter()
        {
            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            DbDataAdapter adapter = _factory.CreateDataAdapter();
            adapter.SelectCommand = _command;
            DbCommandBuilder builder = _factory.CreateCommandBuilder();
            builder.DataAdapter = adapter;
            DataSet ds = new DataSet("ds");

            adapter.Fill(ds);

            Assert.Equal(1L, ds.Tables[0].Rows[0].ItemArray[0]);
        }
    }
}
