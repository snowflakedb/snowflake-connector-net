using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Collections;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbParameterCollection : DbParameterCollection
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
            parameterList.Add(tryCastThrow(value));
            return parameterList.Count - 1;
        }

        public SnowflakeDbParameter Add(string parameterName, SFDataType dataType)
        {
            SnowflakeDbParameter parameter = new SnowflakeDbParameter(parameterName, dataType);
            parameterList.Add(parameter);
            return parameter;
        }

        public override void AddRange(Array values)
        {
            IEnumerator e = values.GetEnumerator();
            while (e.MoveNext())
            {
                parameterList.Add(tryCastThrow(e.Current));
            }
        }

        public override void Clear()
        {
            parameterList.Clear();
        }

        public override bool Contains(string value)
        {
            return IndexOf(value) != -1;
        }

        public override bool Contains(object value)
        {
            SnowflakeDbParameter parameter = tryCast(value);
            return parameter != null && parameterList.Contains(parameter);
        }

        public override void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            return parameterList.GetEnumerator();
        }

        public override int IndexOf(string parameterName)
        {
            int index = 0;
            foreach (SnowflakeDbParameter parameter in parameterList)
            {
                if (String.Compare(parameterName, parameter.ParameterName) == 0)
                {
                    return index;
                }
                index++;
            }
            return -1;
        }

        public override int IndexOf(object value)
        {
            SnowflakeDbParameter parameter = tryCast(value);
            return parameter == null ? -1 : parameterList.IndexOf(parameter);
        }

        public override void Insert(int index, object value)
        {
            parameterList.Insert(index, tryCastThrow(value));
        }

        public override void Remove(object value)
        {
            parameterList.Remove(tryCastThrow(value));
        }

        public override void RemoveAt(string parameterName)
        {
            int index = IndexOf(parameterName);
            parameterList.RemoveAt(index);
        }

        public override void RemoveAt(int index)
        {
            parameterList.RemoveAt(index);
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            int index = IndexOf(parameterName);
            return parameterList[index];
        }

        protected override DbParameter GetParameter(int index)
        {
            return parameterList[index];
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf(parameterName);
            parameterList[index] = tryCastThrow(value);
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            parameterList[index] = tryCastThrow(value);
        }

        private SnowflakeDbParameter tryCast(object parameter)
        {
            if (parameter.GetType() != typeof(SnowflakeDbParameter))
            {
                return null;
            }
            else
            {
                return (SnowflakeDbParameter)parameter;
            }
        }

        private SnowflakeDbParameter tryCastThrow(object parameter)
        {
            if (parameter.GetType() != typeof(SnowflakeDbParameter))
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
            else
            {
                return (SnowflakeDbParameter)parameter;
            }
        }
    }
}
