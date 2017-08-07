using System;
using Snowflake.Data.Core;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbCommand : DbCommand
    {
        private SnowflakeDbConnection connection;

        private SFStatement sfStatement;

        private SnowflakeDbParameterCollection parameterCollection;

        public SnowflakeDbCommand(SnowflakeDbConnection connection)
        {
            this.connection = connection;
            this.sfStatement = new SFStatement(connection.sfSession);
            this.CommandTimeout = 0;

            // by default, no query timeout
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
                if (CommandType != CommandType.Text)
                {
                    throw new NotImplementedException();
                }
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                return UpdateRowSource.FirstReturnedRecord;
            }

            set
            {
                if (UpdatedRowSource != UpdateRowSource.FirstReturnedRecord)
                {
                    throw new NotImplementedException();
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
                throw new NotImplementedException();
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
            return resultSet.getObject(0);
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
                    // if the user is using interface, SFDataType will be None and there will a conversion from DbType to SFDataType
                    // if the user is using concrete class, they should specify SFDataType. 
                    if (parameter.SFDataType == SFDataType.None)
                    {
                        Tuple<string, string> typeAndVal = SFDataConverter.csharpTypeValToSfTypeVal(parameter.DbType, parameter.Value);

                        BindingDTO bindingDto = new BindingDTO(typeAndVal.Item1, typeAndVal.Item2);
                        binding[parameter.ParameterName] = bindingDto;
                    }
                    else
                    {
                        // for now just support TIMESTAMP_LTZ
                        string val = SFDataConverter.csharpValToSfVal(parameter.SFDataType, parameter.Value);
                        BindingDTO bindingDto = new BindingDTO(parameter.SFDataType.ToString(), val);
                        binding[parameter.ParameterName] = bindingDto;
                    }
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
