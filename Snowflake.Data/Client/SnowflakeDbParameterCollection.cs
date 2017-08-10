/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Collections;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    class SnowflakeDbParameterCollection : DbParameterCollection
    {
        internal List<SnowflakeDbParameter> parameterList;
        
        internal SnowflakeDbParameterCollection()
        {
            parameterList = new List<SnowflakeDbParameter>();
        }

        public override int Count
        {
            get
            {
                return parameterList.Count;
            }
        }

        public override object SyncRoot
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override int Add(object value)
        {
            if ( value.GetType() != typeof(SnowflakeDbParameter))
            {
                throw new NotImplementedException();
            }
            else
            {
                parameterList.Add((SnowflakeDbParameter)value);
                return parameterList.Count - 1;
            }
        }

        public SnowflakeDbParameter Add(string parameterName, SFDataType dataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(parameterName, dataType);
            parameterList.Add(parameter);
            return parameter;   
        }

        public SnowflakeDbParameter Add(int parameterIndex, SFDataType dataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(parameterIndex, dataType);
            parameterList.Add(parameter);
            return parameter;   
        }

        public override void AddRange(Array values)
        {
            throw new NotImplementedException();
        }

        public override void Clear()
        {
            throw new NotImplementedException();
        }

        public override bool Contains(string value)
        {
            throw new NotImplementedException();
        }

        public override bool Contains(object value)
        {
            throw new NotImplementedException();
        }

        public override void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override int IndexOf(string parameterName)
        {
            throw new NotImplementedException();
        }

        public override int IndexOf(object value)
        {
            throw new NotImplementedException();
        }

        public override void Insert(int index, object value)
        {
            throw new NotImplementedException();
        }

        public override void Remove(object value)
        {
            throw new NotImplementedException();
        }

        public override void RemoveAt(string parameterName)
        {
            throw new NotImplementedException();
        }

        public override void RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            throw new NotImplementedException();
        }

        protected override DbParameter GetParameter(int index)
        {
            throw new NotImplementedException();
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            throw new NotImplementedException();
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            throw new NotImplementedException();
        }
    }
}
