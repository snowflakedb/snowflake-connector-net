/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsNullable
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

        public override string ParameterName
        {
            get;
            set;
        }

        public override int Size
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

        public override string SourceColumn
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

        public override bool SourceColumnNullMapping
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
