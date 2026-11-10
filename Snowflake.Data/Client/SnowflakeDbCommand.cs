using System;
using Snowflake.Data.Core;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;
using Snowflake.Data.Telemetry;

namespace Snowflake.Data.Client;

[System.ComponentModel.DesignerCategory("Code")]
public class SnowflakeDbCommand : DbCommand
{
    private SnowflakeDbConnection _connection;

    private SFStatement _sfStatement;

    private readonly SnowflakeDbParameterCollection _parameterCollection;

    private readonly SFLogger _logger = SFLoggerFactory.GetLogger<SnowflakeDbCommand>();

    public SnowflakeDbCommand()
    {
        _logger.Debug("Constructing SnowflakeDbCommand class");
        _parameterCollection = new SnowflakeDbParameterCollection();
    }

    public SnowflakeDbCommand(SnowflakeDbConnection connection) : this()
    {
        this._connection = connection;
    }

    public SnowflakeDbCommand(SnowflakeDbConnection connection, string cmdText) : this(connection)
    {
        CommandText = cmdText;
    }

    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }

    public string QueryTag { get; set; }

    public override CommandType CommandType
    {
        get => CommandType.Text;

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
        get => false;
        set
        {
            if (value)
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
        }
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => UpdateRowSource.None;

        set
        {
            if (value != UpdateRowSource.None)
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
        }
    }

    protected override DbConnection DbConnection
    {
        get => _connection;

        set
        {
            if (value == null)
            {
                if (_connection == null)
                {
                    return;
                }

                // Unsetting connection not supported.
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            if (value is not SnowflakeDbConnection sfc)
            {
                // Must be of type SnowflakeDbConnection.
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            if (_connection != null && _connection != sfc)
            {
                // Connection already set.
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            _connection = sfc;
            if (sfc.SfSession != null)
            {
                _sfStatement = new SFStatement(sfc.SfSession, QueryTag);
            }
        }
    }

    protected override DbParameterCollection DbParameterCollection => _parameterCollection;

    protected override DbTransaction DbTransaction { get; set; }

    public override void Cancel()
    {
        // doesn't throw exception when sfStatement is null
        _sfStatement?.Cancel();
    }

    public override int ExecuteNonQuery() => ExecuteNonQuery(GetStatementContext());

    private int ExecuteNonQuery(StatementContext statementContext)
    {
        _logger.Debug("ExecuteNonQuery");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteNonQuery);
        try
        {
            var resultSet = ExecuteInternal(statementContext);
            long total = 0;
            do
            {
                if (resultSet.IsDQL())
                {
                    activity?.AddTelemetryEvent(TelemetryEvents.DqlResultSetSkipped);
                    continue;
                }
                var count = resultSet.CalculateUpdateCount();
                if (count < 0)
                {
                    // exceeded max int, return -1
                    activity?.AddTelemetryEvent(TelemetryEvents.RowCountNegative);
                    return -1;
                }
                total += count;
                if (total > int.MaxValue)
                {
                    activity?.AddTelemetryEvent(TelemetryEvents.RowCountOverflow);
                    return -1;
                }
            }
            while (resultSet.NextResult());

            activity?.SetSuccess();
            return (int)total;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        _logger.Debug("ExecuteNonQueryAsync");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteNonQueryAsync);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var statementContext = GetStatementContext();
            var resultSet = await ExecuteInternalAsync(statementContext, cancellationToken).ConfigureAwait(false);
            long total = 0;
            do
            {
                if (resultSet.IsDQL())
                {
                    activity?.AddTelemetryEvent(TelemetryEvents.DqlResultSetSkipped);
                    continue;
                }
                var count = resultSet.CalculateUpdateCount();
                if (count < 0)
                {
                    activity?.AddTelemetryEvent(TelemetryEvents.RowCountNegative);
                    return -1;
                }
                total += count;
                if (total > int.MaxValue)
                {
                    activity?.AddTelemetryEvent(TelemetryEvents.RowCountOverflow);
                    return -1;
                }
            }
            while (await resultSet.NextResultAsync(cancellationToken).ConfigureAwait(false));

            activity?.SetSuccess();
            return (int)total;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    public override object ExecuteScalar()
    {
        _logger.Debug("ExecuteScalar");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteScalar);
        try
        {
            var statementContext = GetStatementContext();
            var resultSet = ExecuteInternal(statementContext);

            object result;
            if (resultSet.Next())
                result = resultSet.GetValue(0);
            else
                result = DBNull.Value;

            activity?.SetSuccess();
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        _logger.Debug("ExecuteScalarAsync");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteScalarAsync);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var statementContext = GetStatementContext();
            var resultSet = await ExecuteInternalAsync(statementContext, cancellationToken).ConfigureAwait(false);

            object result;
            if (await resultSet.NextAsync().ConfigureAwait(false))
                result = resultSet.GetValue(0);
            else
                result = DBNull.Value;

            activity?.SetSuccess();
            return result;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
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
        if (_sfStatement != null)
        {
            return _sfStatement.GetQueryId();
        }
        return null;
    }

    protected override DbParameter CreateDbParameter()
    {
        return new SnowflakeDbParameter();
    }

    private DbDataReader ExecuteDbDataReader(StatementContext statementContext)
    {
        _logger.Debug("ExecuteDbDataReader");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteDbDataReader);
        try
        {
            var resultSet = ExecuteInternal(statementContext);
            activity?.SetSuccess();
            return new SnowflakeDbDataReader(this, resultSet, statementContext.DescribeOnly);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var schemaOnly = behavior.HasFlag(CommandBehavior.SchemaOnly);
        var statementContext = GetStatementContext() with { DescribeOnly = schemaOnly };
        return ExecuteDbDataReader(statementContext);
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        _logger.Debug("ExecuteDbDataReaderAsync");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteDbDataReaderAsync);
        try
        {
            var schemaOnly = behavior.HasFlag(CommandBehavior.SchemaOnly);
            var statementContext = GetStatementContext() with { DescribeOnly = schemaOnly };
            var result = await ExecuteInternalAsync(statementContext, cancellationToken).ConfigureAwait(false);
            activity?.SetSuccess();
            return new SnowflakeDbDataReader(this, result, schemaOnly);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            _logger.Error("The command failed to execute.", ex);
            throw;
        }
    }

    /// <summary>
    /// Executes a PUT command uploading data from the provided in-memory stream instead of a local file.
    /// The caller is responsible for disposing the stream and ensuring thread-safe access to it.
    /// </summary>
    /// <returns>A <see cref="DbDataReader"/> containing the upload result metadata.</returns>
    public DbDataReader ExecuteDbDataReaderWithMemoryStream(MemoryStream stream)
    {
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));
        if (!stream.CanRead)
            throw new ObjectDisposedException(nameof(stream), "The stream has been disposed or is not readable.");
        if (stream.Position != 0)
            throw new InvalidOperationException($"{nameof(stream)} needs to be positioned before reading.");

        _logger.Debug($"{nameof(ExecuteDbDataReaderWithMemoryStream)}");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteWithCustomUploadStream);
        try
        {
            var statementContext = GetStatementContext() with { Stream = stream };
            return ExecuteDbDataReader(statementContext);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
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
        _logger.Debug("ExecuteInAsyncMode");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteInAsyncMode);
        try
        {
            var statementContext = GetStatementContext() with { AsyncExec = true };
            var resultSet = ExecuteInternal(statementContext);
            activity?.SetSuccess();
            return resultSet.queryId;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    /// <summary>
    /// Executes an asynchronous query in async mode.
    /// Async mode means the server will respond immediately with the query ID and execute the query asynchronously
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns>The query id.</returns>
    public async Task<string> ExecuteAsyncInAsyncMode(CancellationToken cancellationToken)
    {
        _logger.Debug("ExecuteAsyncInAsyncMode");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.ExecuteAsyncInAsyncMode);
        try
        {
            var statementContext = GetStatementContext() with { AsyncExec = true };
            var resultSet = await ExecuteInternalAsync(statementContext, cancellationToken).ConfigureAwait(false);
            activity?.SetSuccess();
            return resultSet.queryId;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    /// <summary>
    /// Gets the query status based on query ID.
    /// </summary>
    /// <param name="queryId"></param>
    /// <returns>The query status.</returns>
    public QueryStatus GetQueryStatus(string queryId)
    {
        _logger.Debug("GetQueryStatus");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.GetQueryStatus);
        try
        {
            var status = QueryResultsAwaiter.GetQueryStatus(_connection, queryId);
            activity?.SetSuccess();
            return status;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
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
        _logger.Debug("GetQueryStatusAsync");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.GetQueryStatusAsync);
        try
        {
            var status = await QueryResultsAwaiter.GetQueryStatusAsync(_connection, queryId, cancellationToken).ConfigureAwait(false);
            activity?.SetSuccess();
            return status;
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    /// <summary>
    /// Gets the query results based on query ID.
    /// </summary>
    /// <param name="queryId"></param>
    /// <returns>The query results.</returns>
    public DbDataReader GetResultsFromQueryId(string queryId)
    {
        _logger.Debug("GetResultsFromQueryId");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.GetResultsFromQueryId);

        try
        {
            var task = QueryResultsAwaiter.Instance.RetryUntilQueryResultIsAvailable(_connection, queryId, CancellationToken.None, false);
            task.Wait();

            var resultSet = _sfStatement.GetResultWithId(queryId);

            activity?.SetSuccess();
            return new SnowflakeDbDataReader(this, resultSet, false);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    /// <summary>
    /// Gets the query results based on query ID.
    /// </summary>
    /// <param name="queryId"></param>
    /// <param name="cancellationToken"></param>
    /// <returns>The query results.</returns>
    public async Task<DbDataReader> GetResultsFromQueryIdAsync(string queryId, CancellationToken cancellationToken)
    {
        _logger.Debug("GetResultsFromQueryIdAsync");
        using var activity = _connection?.SfSession?.StartActivity(TelemetryActivities.GetResultsFromQueryIdAsync);

        try
        {
            await QueryResultsAwaiter.Instance.RetryUntilQueryResultIsAvailable(_connection, queryId, cancellationToken, true).ConfigureAwait(false);

            var resultSet = await _sfStatement.GetResultWithIdAsync(queryId, cancellationToken).ConfigureAwait(false);

            activity?.SetSuccess();
            return new SnowflakeDbDataReader(this, resultSet, false);
        }
        catch (Exception ex)
        {
            activity?.SetException(ex);
            throw;
        }
    }

    private static Dictionary<string, BindingDTO> ConvertToBindList(List<SnowflakeDbParameter> parameters)
    {
        if (parameters == null || parameters.Count == 0)
        {
            return null;
        }
        else
        {
            var binding = new Dictionary<string, BindingDTO>();
            foreach (var parameter in parameters)
            {
                var bindingType = "";
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
                    var vals = new List<object>();
                    foreach (var val in (Array)parameter.Value)
                    {
                        // if the user is using interface, SFDataType will be None and there will
                        // a conversion from DbType to SFDataType
                        // if the user is using concrete class, they should specify SFDataType.
                        if (parameter.SFDataType == SFDataType.None)
                        {
                            var typeAndVal = SFDataConverter
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
                        var typeAndVal = SFDataConverter
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
        if (_connection == null)
        {
            throw new SnowflakeDbException(SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);
        }

        var session = _connection.SfSession;

        // SetStatement is called when executing a command. If SfSession is null
        // the connection has never been opened. Exception might be a bit vague.
        if (session == null)
            throw new SnowflakeDbException(SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);

        _sfStatement = new SFStatement(session, QueryTag);
    }

    private SFBaseResultSet ExecuteInternal(StatementContext statementContext)
    {
        CheckIfCommandTextIsSet();
        SetStatement();
        return _sfStatement.Execute(statementContext, ConvertToBindList(_parameterCollection.parameterList));
    }

    private Task<SFBaseResultSet> ExecuteInternalAsync(StatementContext statementContext, CancellationToken cancellationToken)
    {
        CheckIfCommandTextIsSet();
        SetStatement();
        return _sfStatement.ExecuteAsync(statementContext, ConvertToBindList(_parameterCollection.parameterList), cancellationToken);
    }

    private void CheckIfCommandTextIsSet()
    {
        if (!string.IsNullOrEmpty(CommandText))
            return;

        var errorMessage = "Unable to execute command due to command text not being set";
        _logger.Error(errorMessage);
        throw new Exception(errorMessage);
    }

    internal string GetBindStage() => _sfStatement?.GetBindStage();

    private StatementContext GetStatementContext() => new(null, false, false, CommandText, CommandTimeout, QueryTag);
}
