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
            parameterCollection = new SnowflakeDbParameterCollection();
        }

        public override string CommandText
        {
            get;  set;
        }

        public override int CommandTimeout
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
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        public override int ExecuteNonQuery()
        {
            SFBaseResultSet resultSet = sfStatement.execute(CommandText, 
                convertToBindList(parameterCollection.parameterList), false);
            resultSet.next();
            return ResultSetUtil.calculateUpdateCount(resultSet);
        }

        public override object ExecuteScalar()
        {
            SFBaseResultSet resultSet = sfStatement.execute(CommandText, null, false);
            resultSet.next();
            return resultSet.getObject(1);
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
            SFBaseResultSet resultSet = sfStatement.execute(CommandText, null, false);
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
                    Tuple<string, string> typeAndVal = SFDataConverter.csharpTypeValToSfTypeVal(parameter.DbType, parameter.Value);

                    BindingDTO bindingDto = new BindingDTO(typeAndVal.Item1, typeAndVal.Item2);
                    binding[parameter.ParameterName] = bindingDto;
                }
                return binding;
            }
        }
    }
}
