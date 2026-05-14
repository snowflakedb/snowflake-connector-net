using System;
using System.Threading.Tasks;
using Xunit;
using System.Data;
using System.Data.Common;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFDbFactoryIT : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        DbProviderFactory _factory;
        DbCommand _command;
        DbConnection _connection;

        public SFDbFactoryIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture)
        {
            _fixture = fixture;
#if NETFRAMEWORK
            _factory = DbProviderFactories.GetFactory("Snowflake.Data");
#else
            // In .NET Standard, DbProviderFactories is gone.
            // Reference https://weblog.west-wind.com/posts/2017/Nov/27/Working-around-the-lack-of-dynamic-DbProviderFactory-loading-in-NET-Core
            // for more details
            _factory = Snowflake.Data.Client.SnowflakeDbFactory.Instance;
#endif

            _command = _factory.CreateCommand();
            _connection = _factory.CreateConnection();

            _connection.ConnectionString = _fixture.ConnectionString;
            _connection.Open();
        }

        public void Dispose()
        {
            _connection.Close();
        }

        [Fact]
        public async Task TestSimpleDbFactory()
        {
            // set commnad's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            object res = await _command.ExecuteScalarAsync();

            Assert.Equal(1, res);
        }

        [Fact]
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

            Assert.Equal(expectedIntValue, result);
        }

        [Fact]
        public async Task TestDbFactoryWithConnectionStringBuilder()
        {
            DbConnectionStringBuilder builder = _factory.CreateConnectionStringBuilder();
            builder.ConnectionString = _fixture.ConnectionString;

            _connection.ConnectionString = builder.ConnectionString;
            await _connection.OpenAsync();

            // set command's connection object
            _command.Connection = _connection;
            _command.CommandText = "select 1";

            var result = await _command.ExecuteScalarAsync();

            Assert.Equal(1, result);
        }

        [Fact]
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

            Assert.Equal(1, ds.Tables[0].Rows[0].ItemArray[0]);
        }
    }
}
