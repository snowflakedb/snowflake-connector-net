﻿/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using System.Collections;
using Snowflake.Data.Core;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbDataReader : DbDataReader
    {
        static private readonly ILog logger = LogManager.GetLogger<SnowflakeDbDataReader>();

        private SnowflakeDbCommand dbCommand;

        private SFBaseResultSet resultSet;

        private bool isClosed;

        internal SnowflakeDbDataReader(SnowflakeDbCommand command, SFBaseResultSet resultSet)
        {
            this.dbCommand = command;
            this.resultSet = resultSet;
            this.isClosed = false;
        }
        public override object this[string name]
        {
            get
            {
                return resultSet.getValue(GetOrdinal(name));
            }
        }

        public override object this[int ordinal]
        {
            get
            {
                return resultSet.getValue(ordinal);
            }
        }

        public override int Depth
        {
            get
            {
                return 0;
            }
        }

        public override int FieldCount
        {
            get
            {
                return resultSet.columnCount;
            }
        }

        public override bool HasRows
        {
            get
            {
                // return true for now since every query returned from server
                // will have at least one row
                return true;
            }
        }

        public override bool IsClosed
        {
            get
            {
                return this.isClosed;
            }
        }

        public override int RecordsAffected => resultSet.CalculateUpdateCount();

        public override bool GetBoolean(int ordinal)
        {
            return resultSet.getBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            byte[] bytes = resultSet.getBytes(ordinal);
            return bytes[0];
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            string val = resultSet.getString(ordinal);
            return val[0];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return resultSet.sfResultSetMetaData.getColumnTypeByIndex(ordinal).ToString();
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
            return resultSet.sfResultSetMetaData.getCSharpTypeByIndex(ordinal);
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
            return resultSet.sfResultSetMetaData.getColumnNameByIndex(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return resultSet.sfResultSetMetaData.getColumnIndexByName(name);
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
            int count = Math.Min(FieldCount, values.Length);
            for (int i=0; i< count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return resultSet.getValue(ordinal) == null;
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            return resultSet.Next();
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                throw new TaskCanceledException();

            return resultSet.NextAsync();
        }

        public override void Close()
        {
            base.Close();
            resultSet.close();
            isClosed = true;
        }
    }
}
