using System;
using Snowflake.Data.Core;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbCommand : DbCommand
    {
        private SnowflakeDbConnection connection;

        private SFStatement sfStatement;

        private SnowflakeDbParameterCollection parameterCollection;

        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbCommand>();

        private readonly QueryResultsAwaiter _queryResultsAwaiter = QueryResultsAwaiter.Instance;

        public SnowflakeDbCommand()
        {
            logger.Debug("Constructing SnowflakeDbCommand class");
            // by default, no query timeout
            this.CommandTimeout = 0;
            parameterCollection = new SnowflakeDbParameterCollection();
        }

        public SnowflakeDbCommand(SnowflakeDbConnection connection) : this()
        {
            this.connection = connection;
        }

        public SnowflakeDbCommand(SnowflakeDbConnection connection, string cmdText) : this(connection)
        {
            this.CommandText = cmdText;
        }

        public override string CommandText
        {
            get; set;
        }

        public override int CommandTimeout
        {
            get; set;
        }

        public string QueryTag
        {
            get; set;
        }

        public override CommandType CommandType
        {
            get
            {
                return CommandType.Text;
            }

            set
            {
                if (value != CommandType.Text)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return false;
            }

            set
            {
                if (value)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => UpdateRowSource.None;

            set
            {
                if (value != UpdateRowSource.None)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        protected override DbConnection DbConnection
        {
            get => connection;

            set
            {
                if (value == null)
                {
                    if (connection == null)
                    {
                        return;
                    }

                    // Unsetting connection not supported.
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }

                if (!(value is SnowflakeDbConnection))
                {
                    // Must be of type SnowflakeDbConnection.
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }

                var sfc = (SnowflakeDbConnection)value;
                if (connection != null && connection != sfc)
                {
                    // Connection already set.
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }

                connection = sfc;
                if (sfc.SfSession != null)
                {
                    sfStatement = new SFStatement(sfc.SfSession, QueryTag);
                }
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return this.parameterCollection;
            }
        }

        protected override DbTransaction DbTransaction
        {
            get;

            set;
        }

        public override void Cancel()
        {
            // doesn't throw exception when sfStatement is null
            sfStatement?.Cancel();
        }

        public override int ExecuteNonQuery()
        {
            logger.Debug($"ExecuteNonQuery");
            SFBaseResultSet resultSet = ExecuteInternal();
            long total = 0;
            do
            {
                if (resultSet.IsDQL()) continue;
                int count = resultSet.CalculateUpdateCount();
                if (count < 0)
                {
                    // exceeded max int, return -1
                    return -1;
                }
                total += count;
                if (total > int.MaxValue)
                {
                    return -1;
                }
            }
            while (resultSet.NextResult());

            return (int)total;
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteNonQueryAsync");
            cancellationToken.ThrowIfCancellationRequested();

            var resultSet = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);
            long total = 0;
            do
            {
                if (resultSet.IsDQL()) continue;
                int count = resultSet.CalculateUpdateCount();
                if (count < 0)
                {
                    // exceeded max int, return -1
                    return -1;
                }
                total += count;
                if (total > int.MaxValue)
                {
                    return -1;
                }
            }
            while (await resultSet.NextResultAsync(cancellationToken).ConfigureAwait(false));

            return (int)total;
        }

        public override object ExecuteScalar()
        {
            logger.Debug($"ExecuteScalar");
            SFBaseResultSet resultSet = ExecuteInternal();

            if (resultSet.Next())
                return resultSet.GetValue(0);
            else
                return DBNull.Value;
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteScalarAsync");
            cancellationToken.ThrowIfCancellationRequested();

            var result = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);

            if (await result.NextAsync().ConfigureAwait(false))
                return result.GetValue(0);
            else
                return DBNull.Value;
        }

        /// <summary>
        /// Prepares the command for execution.
        /// This method is currently not implemented and acts as a no-operation (Noop).
        /// </summary>
        public override void Prepare()
        {
        }

        public string GetQueryId()
        {
            if (sfStatement != null)
            {
                return sfStatement.GetQueryId();
            }
            return null;
        }

        protected override DbParameter CreateDbParameter()
        {
            return new SnowflakeDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            logger.Debug($"ExecuteDbDataReader");
            SFBaseResultSet resultSet = ExecuteInternal();
            return new SnowflakeDbDataReader(this, resultSet);
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteDbDataReaderAsync");
            try
            {
                var result = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);
                return new SnowflakeDbDataReader(this, result);
            }
            catch (Exception ex)
            {
                logger.Error("The command failed to execute.", ex);
                throw;
            }
        }

        /// <summary>
        /// Execute a query in async mode.
        /// Async mode means the server will respond immediately with the query ID and execute the query asynchronously
        /// </summary>
        /// <returns>The query id.</returns>
        public string ExecuteInAsyncMode()
        {
            logger.Debug($"ExecuteInAsyncMode");
            SFBaseResultSet resultSet = ExecuteInternal(asyncExec: true);
            return resultSet.queryId;
        }

        /// <summary>
        /// Executes an asynchronous query in async mode.
        /// Async mode means the server will respond immediately with the query ID and execute the query asynchronously
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>The query id.</returns>
        public async Task<string> ExecuteAsyncInAsyncMode(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteAsyncInAsyncMode");
            var resultSet = await ExecuteInternalAsync(cancellationToken, asyncExec: true).ConfigureAwait(false);
            return resultSet.queryId;
        }

        /// <summary>
        /// Gets the query status based on query ID.
        /// </summary>
        /// <param name="queryId"></param>
        /// <returns>The query status.</returns>
        public QueryStatus GetQueryStatus(string queryId)
        {
            logger.Debug($"GetQueryStatus");
            return _queryResultsAwaiter.GetQueryStatus(connection, queryId);
        }

        /// <summary>
        /// Gets the query status based on query ID.
        /// </summary>
        /// <param name="queryId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The query status.</returns>
        public async Task<QueryStatus> GetQueryStatusAsync(string queryId, CancellationToken cancellationToken)
        {
            logger.Debug($"GetQueryStatusAsync");
            return await _queryResultsAwaiter.GetQueryStatusAsync(connection, queryId, cancellationToken);
        }

        /// <summary>
        /// Gets the query results based on query ID.
        /// </summary>
        /// <param name="queryId"></param>
        /// <returns>The query results.</returns>
        public DbDataReader GetResultsFromQueryId(string queryId)
        {
            logger.Debug($"GetResultsFromQueryId");

            Task task = _queryResultsAwaiter.RetryUntilQueryResultIsAvailable(connection, queryId, CancellationToken.None, false);
            task.Wait();

            SFBaseResultSet resultSet = sfStatement.GetResultWithId(queryId);

            return new SnowflakeDbDataReader(this, resultSet);
        }

        /// <summary>
        /// Gets the query results based on query ID.
        /// </summary>
        /// <param name="queryId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The query results.</returns>
        public async Task<DbDataReader> GetResultsFromQueryIdAsync(string queryId, CancellationToken cancellationToken)
        {
            logger.Debug($"GetResultsFromQueryIdAsync");

            await _queryResultsAwaiter.RetryUntilQueryResultIsAvailable(connection, queryId, cancellationToken, true);

            SFBaseResultSet resultSet = await sfStatement.GetResultWithIdAsync(queryId, cancellationToken).ConfigureAwait(false);

            return new SnowflakeDbDataReader(this, resultSet);
        }

        private static Dictionary<string, BindingDTO> convertToBindList(List<SnowflakeDbParameter> parameters)
        {
            if (parameters == null || parameters.Count == 0)
            {
                return null;
            }
            else
            {
                Dictionary<string, BindingDTO> binding = new Dictionary<string, BindingDTO>();
                foreach (SnowflakeDbParameter parameter in parameters)
                {
                    string bindingType = "";
                    object bindingVal;

                    if (parameter.Value == null)
                    {
                        parameter.Value = DBNull.Value;
                    }

                    // byte array and char array will not be treated as array binding
                    if (parameter.Value.GetType().IsArray &&
                        parameter.Value.GetType().GetElementType() != typeof(char) &&
                        parameter.Value.GetType().GetElementType() != typeof(byte))
                    {
                        List<object> vals = new List<object>();
                        foreach (object val in (Array)parameter.Value)
                        {
                            // if the user is using interface, SFDataType will be None and there will
                            // a conversion from DbType to SFDataType
                            // if the user is using concrete class, they should specify SFDataType.
                            if (parameter.SFDataType == SFDataType.None)
                            {
                                Tuple<string, string> typeAndVal = SFDataConverter
                                    .CSharpTypeValToSfTypeVal(parameter.DbType, val);

                                bindingType = typeAndVal.Item1;
                                vals.Add(typeAndVal.Item2);
                            }
                            else
                            {
                                bindingType = parameter.SFDataType.ToString();
                                vals.Add(SFDataConverter.CSharpValToSfVal(parameter.SFDataType, val));
                            }
                        }
                        bindingVal = vals;
                    }
                    else
                    {
                        if (parameter.SFDataType == SFDataType.None)
                        {
                            Tuple<string, string> typeAndVal = SFDataConverter
                                .CSharpTypeValToSfTypeVal(parameter.DbType, parameter.Value);
                            bindingType = typeAndVal.Item1;
                            bindingVal = typeAndVal.Item2;
                        }
                        else
                        {
                            bindingType = parameter.SFDataType.ToString();
                            bindingVal = SFDataConverter.CSharpValToSfVal(parameter.SFDataType, parameter.Value);
                        }
                    }

                    binding[parameter.ParameterName] = new BindingDTO(bindingType, bindingVal);
                }
                return binding;
            }
        }

        private void SetStatement()
        {
            if (connection == null)
            {
                throw new SnowflakeDbException(SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);
            }

            var session = (connection as SnowflakeDbConnection).SfSession;

            // SetStatement is called when executing a command. If SfSession is null
            // the connection has never been opened. Exception might be a bit vague.
            if (session == null)
                throw new SnowflakeDbException(SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);

            this.sfStatement = new SFStatement(session, QueryTag);
        }

        private SFBaseResultSet ExecuteInternal(bool describeOnly = false, bool asyncExec = false)
        {
            CheckIfCommandTextIsSet();
            SetStatement();
            return sfStatement.Execute(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly, asyncExec);
        }

        private Task<SFBaseResultSet> ExecuteInternalAsync(CancellationToken cancellationToken, bool describeOnly = false, bool asyncExec = false)
        {
            CheckIfCommandTextIsSet();
            SetStatement();
            return sfStatement.ExecuteAsync(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly, asyncExec, cancellationToken);
        }

        private void CheckIfCommandTextIsSet()
        {
            if (string.IsNullOrEmpty(CommandText))
            {
                var errorMessage = "Unable to execute command due to command text not being set";
                logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        internal string GetBindStage() => sfStatement?.GetBindStage();
    }
}
