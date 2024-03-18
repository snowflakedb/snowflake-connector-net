﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Snowflake.Data.Core;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Snowflake.Data.Log;
using System.Text.RegularExpressions;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbCommand : DbCommand
    {
        private SnowflakeDbConnection connection;

        private SFStatement sfStatement;

        private SnowflakeDbParameterCollection parameterCollection;

        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbCommand>();

        // Async max retry and retry pattern
        private const int AsyncNoDataMaxRetry = 24;
        private readonly int[] _asyncRetryPattern = { 1, 1, 2, 3, 4, 8, 10 };

        private static readonly Regex UuidRegex = new Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

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

                var sfc = (SnowflakeDbConnection) value;
                if (connection != null && connection != sfc)
                {
                    // Connection already set.
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }

                connection = sfc;
                if (sfc.SfSession != null)
                {
                    sfStatement = new SFStatement(sfc.SfSession);
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
                if (resultSet.HasResultSet()) continue;
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
                if (resultSet.HasResultSet()) continue;
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

            if(resultSet.Next())
                return resultSet.GetValue(0);
            else
                return DBNull.Value;
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteScalarAsync");
            cancellationToken.ThrowIfCancellationRequested();

            var result = await ExecuteInternalAsync(cancellationToken).ConfigureAwait(false);

            if(await result.NextAsync().ConfigureAwait(false))
                return result.GetValue(0);
            else
                return DBNull.Value;
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
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

            if (UuidRegex.IsMatch(queryId))
            {
                var sfStatement = new SFStatement(connection.SfSession);
                return sfStatement.GetQueryStatus(queryId);
            }
            else
            {
                var errorMessage = $"The given query id {queryId} is not valid uuid";
                logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
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

            // Check if queryId is valid uuid
            if (UuidRegex.IsMatch(queryId))
            {
                var sfStatement = new SFStatement(connection.SfSession);
                return await sfStatement.GetQueryStatusAsync(queryId, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var errorMessage = $"The given query id {queryId} is not valid uuid";
                logger.Error(errorMessage);
                throw new Exception(errorMessage);
            }
        }

        /// <summary>
        /// Checks query status until it is done executing.
        /// </summary>
        /// <param name="queryId"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="isAsync"></param>
        internal async Task RetryUntilQueryResultIsAvailable(string queryId, CancellationToken cancellationToken, bool isAsync)
        {
            int retryPatternPos = 0;
            int noDataCounter = 0;

            QueryStatus status;
            while (true)
            {
                status = isAsync ? await GetQueryStatusAsync(queryId, cancellationToken) : GetQueryStatus(queryId);

                if (!QueryStatuses.IsStillRunning(status))
                {
                    return;
                }

                // Timeout based on query status retry rules
                if (isAsync)
                {
                    await Task.Delay(TimeSpan.FromSeconds(_asyncRetryPattern[retryPatternPos]), cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Thread.Sleep(TimeSpan.FromSeconds(_asyncRetryPattern[retryPatternPos]));
                }

                // If no data, increment the no data counter
                if (status == QueryStatus.NO_DATA)
                {
                    noDataCounter++;

                    // Check if retry for no data is exceeded
                    if (noDataCounter > AsyncNoDataMaxRetry)
                    {
                        var errorMessage = "Max retry for no data is reached";
                        logger.Error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                }

                if (retryPatternPos < _asyncRetryPattern.Length - 1)
                {
                    retryPatternPos++;
                }
            }
        }

        /// <summary>
        /// Gets the query results based on query ID.
        /// </summary>
        /// <param name="queryId"></param>
        /// <returns>The query results.</returns>
        public DbDataReader GetResultsFromQueryId(string queryId)
        {
            logger.Debug($"GetResultsFromQueryId");

            Task task = RetryUntilQueryResultIsAvailable(queryId, CancellationToken.None, false);
            task.Wait();

            connection.SfSession.AsyncQueries.Remove(queryId);
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

            await RetryUntilQueryResultIsAvailable(queryId, cancellationToken, true);

            connection.SfSession.AsyncQueries.Remove(queryId);
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
                foreach(SnowflakeDbParameter parameter in parameters)
                {
                    string bindingType = "";
                    object bindingVal;

                    if (parameter.Value.GetType().IsArray &&
                        // byte array and char array will not be treated as array binding
                        parameter.Value.GetType().GetElementType() != typeof(char) && 
                        parameter.Value.GetType().GetElementType() != typeof(byte))
                    {
                        List<object> vals = new List<object>();
                        foreach(object val in (Array)parameter.Value)
                        {
                            // if the user is using interface, SFDataType will be None and there will 
                            // a conversion from DbType to SFDataType
                            // if the user is using concrete class, they should specify SFDataType. 
                            if (parameter.SFDataType == SFDataType.None)
                            {
                                Tuple<string, string> typeAndVal = SFDataConverter
                                    .csharpTypeValToSfTypeVal(parameter.DbType, val);

                                bindingType = typeAndVal.Item1;
                                vals.Add(typeAndVal.Item2);
                            }
                            else
                            {
                                bindingType = parameter.SFDataType.ToString(); 
                                vals.Add(SFDataConverter.csharpValToSfVal(parameter.SFDataType, val));
                            }
                        }
                        bindingVal = vals;
                    }
                    else
                    {
                        if (parameter.SFDataType == SFDataType.None)
                        {
                            Tuple<string, string> typeAndVal = SFDataConverter
                                .csharpTypeValToSfTypeVal(parameter.DbType, parameter.Value);
                            bindingType = typeAndVal.Item1;
                            bindingVal = typeAndVal.Item2;
                        }
                        else
                        {
                            bindingType = parameter.SFDataType.ToString();
                            bindingVal = SFDataConverter.csharpValToSfVal(parameter.SFDataType, parameter.Value);
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

            this.sfStatement = new SFStatement(session);
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
    }
}
