/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using System.Collections;
using Snowflake.Data.Core;
using System.Data;

using Common.Logging;

namespace Snowflake.Data.Client
{
    class SnowflakeDbDataReader : DbDataReader
    {
        static private readonly ILog logger = LogManager.GetLogger<SnowflakeDbDataReader>();

        private SnowflakeDbCommand dbCommand;

        private SFBaseResultSet resultSet;
        public SnowflakeDbDataReader(SnowflakeDbCommand command, SFBaseResultSet resultSet)
        {
            this.dbCommand = command;
            this.resultSet = resultSet;
        }
        public override object this[string name]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override object this[int ordinal]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override int FieldCount
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool HasRows
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsClosed
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override int RecordsAffected
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool GetBoolean(int ordinal)
        {
            return resultSet.getBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return resultSet.getDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return resultSet.getDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return resultSet.getDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            return resultSet.getFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return resultSet.getGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return resultSet.getInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return resultSet.getInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return resultSet.getInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            return resultSet.getString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return resultSet.getValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            return resultSet.next();
        }

        public override DataTable GetSchemaTable()
        {
            return resultSet.sfResultSetMetaData.toDataTable();
        }
    }
}
