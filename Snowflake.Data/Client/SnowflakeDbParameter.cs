/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Data;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbParameter : DbParameter
    {
        public SFDataType SFDataType { get; set; }

        public SnowflakeDbParameter()
        {
            SFDataType = SFDataType.None;
        }

        public SnowflakeDbParameter(string ParameterName, SFDataType SFDataType)
        {
            this.ParameterName = ParameterName;
            this.SFDataType = SFDataType;
        }

        public SnowflakeDbParameter(int ParameterIndex, SFDataType SFDataType)
        {
            this.ParameterName = ParameterIndex.ToString();
            this.SFDataType = SFDataType;
        }

        public override DbType DbType { get; set; }

        public override ParameterDirection Direction
        {
            get
            {
                return ParameterDirection.Input;
            }

            set
            {
                if (value != ParameterDirection.Input)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override bool IsNullable
        {
            get { return false; }

            set
            {
                if (value != false)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override string ParameterName
        {
            get;
            set;
        }

        public override int Size
        {
            get { return 0; }

            set
            {
                if (value != 0)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override string SourceColumn
        {
            get { return null; }

            set
            {
                if (value != null)
                {
                    throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override bool SourceColumnNullMapping
        {
            get { return false; }

            set
            {
                // ReSharper disable once RedundantBoolCompare (compare to false for clarity)
                if (value != false)
                {
                    throw  new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
                }
            }
        }

        public override object Value
        {
            get;

            set;
        }

        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }
    }
}
