/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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

        public SnowflakeDbCommand(SnowflakeDbConnection connection)
        {
            logger.Debug("Constucting SnowflakeDbCommand class");
            this.connection = connection;
            this.sfStatement = new SFStatement(connection.SfSession);
            // by default, no query timeout
            this.CommandTimeout = 0;
            parameterCollection = new SnowflakeDbParameterCollection();
        }

        public override string CommandText
        {
            get;  set;
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
            get
            {
                return UpdateRowSource.None;
            }

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
            get
            {
                return connection;
            }

            set
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
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
            sfStatement.Cancel();
        }
        
        public override int ExecuteNonQuery()
        {
            logger.Debug($"ExecuteNonQuery, command: {CommandText}");
            SFBaseResultSet resultSet = ExecuteInternal();
            resultSet.Next();
            return resultSet.CalculateUpdateCount();
        }

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteNonQueryAsync, command: {CommandText}");
            if (cancellationToken.IsCancellationRequested)
                throw new TaskCanceledException();

            var resultSet = await ExecuteInternalAsync(cancellationToken);
            await resultSet.NextAsync();
            return resultSet.CalculateUpdateCount();
        }

        public override object ExecuteScalar()
        {
            logger.Debug($"ExecuteScalar, command: {CommandText}");
            SFBaseResultSet resultSet = ExecuteInternal();

            if(resultSet.Next())
                return resultSet.GetValue(0);

            return DBNull.Value;
        }

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteScalarAsync, command: {CommandText}");
            if (cancellationToken.IsCancellationRequested)
                throw new TaskCanceledException();

            var result = await ExecuteInternalAsync(cancellationToken);

            if(await result.NextAsync())
                return result.GetValue(0);

            return DBNull.Value;
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            return new SnowflakeDbParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            logger.Debug($"ExecuteDbDataReader, command: {CommandText}");
            SFBaseResultSet resultSet = ExecuteInternal();
            return new SnowflakeDbDataReader(this, resultSet);
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            logger.Debug($"ExecuteDbDataReaderAsync, command: {CommandText}");
            var result = await ExecuteInternalAsync(cancellationToken);
            return new SnowflakeDbDataReader(this, result);
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

        private SFBaseResultSet ExecuteInternal(bool describeOnly = false)
        {
            return sfStatement.Execute(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly);
        }

        private Task<SFBaseResultSet> ExecuteInternalAsync(CancellationToken cancellationToken, bool describeOnly = false)
        {
            return sfStatement.ExecuteAsync(CommandTimeout, CommandText, convertToBindList(parameterCollection.parameterList), describeOnly, cancellationToken);
        }
    }
}
