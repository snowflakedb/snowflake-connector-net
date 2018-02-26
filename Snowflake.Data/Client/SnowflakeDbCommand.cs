/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using Snowflake.Data.Core;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbCommand : DbCommand
    {
        private DbConnection connection;

        private SFStatement sfStatement;

        private SnowflakeDbParameterCollection parameterCollection;

        public SnowflakeDbCommand(SnowflakeDbConnection connection)
        {
            this.connection = connection;
            this.sfStatement = new SFStatement(connection.sfSession);
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
            sfStatement.cancel();
        }

        public override int ExecuteNonQuery()
        {
            SFBaseResultSet resultSet = executeInternal(CommandText, 
                convertToBindList(parameterCollection.parameterList), false);
            resultSet.next();
            return ResultSetUtil.calculateUpdateCount(resultSet);
        }

        public override object ExecuteScalar()
        {
            SFBaseResultSet resultSet = executeInternal(CommandText, null, false);
            resultSet.next();
            return resultSet.GetValue(0);
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
            SFBaseResultSet resultSet = executeInternal(CommandText, null, false);
            return new SnowflakeDbDataReader(this, resultSet);
        }

        private Dictionary<string, BindingDTO> convertToBindList(List<SnowflakeDbParameter> parameters)
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

        private SFBaseResultSet executeInternal(string sql, 
            Dictionary<string, BindingDTO> bindings, bool describeOnly)
        {
            if (CommandTimeout != 0)
            {
                sfStatement.setQueryTimeoutBomb(CommandTimeout);
            }

            SFBaseResultSet resultSet = sfStatement.execute(sql, bindings, describeOnly);

            return resultSet;
        }
    }
}
