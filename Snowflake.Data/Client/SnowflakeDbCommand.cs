/*
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

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbCommand : DbCommand
    {
        private DbConnection connection;

        private SFStatement sfStatement;

        private SnowflakeDbParameterCollection parameterCollection;

        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbCommand>();

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
            var session = (connection as SnowflakeDbConnection).SfSession;

            // SetStatement is called when executing a command. If SfSession is null
            // the connection has never been opened. Exception might be a bit vague.
            if (session == null)
                throw new Exception("Can't execute command when connection has never been opened");

            this.sfStatement = new SFStatement(session);
        }

        private SFBaseResultSet ExecuteInternal(bool describeOnly = false)
        {
            SetStatement();
            return sfStatement.Execute(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly);
        }

        private Task<SFBaseResultSet> ExecuteInternalAsync(CancellationToken cancellationToken, bool describeOnly = false)
        {
            SetStatement();
            return sfStatement.ExecuteAsync(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly, cancellationToken);
        }
    }
}
