using System;
using System.Data.Common;
using Snowflake.Data.Core;
using System.Data;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbDataAdapter : DbDataAdapter, IDbDataAdapter
    {
        private static readonly object EventRowUpdated = new object();
        private static readonly object EventRowUpdating = new object();

        private SnowflakeDbCommand _selectCommand;

        public SnowflakeDbDataAdapter() : base()
        {
            GC.SuppressFinalize(this);
        }

        public SnowflakeDbDataAdapter(SnowflakeDbCommand selectCommand) : this()
        {
            SelectCommand = selectCommand;
        }

        public SnowflakeDbDataAdapter(string selectCommandText, SnowflakeDbConnection selectConnection) : this()
        {
            SelectCommand = new SnowflakeDbCommand(selectConnection);
            SelectCommand.CommandText = selectCommandText;
        }

        private SnowflakeDbDataAdapter(SnowflakeDbDataAdapter from) : base(from)
        {
            GC.SuppressFinalize(this);
        }

        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
            set
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
        }

        IDbCommand IDbDataAdapter.InsertCommand
        {
            get
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
            set
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
        }

        new public SnowflakeDbCommand SelectCommand
        {
            get { return _selectCommand; }
            set { _selectCommand = value; }
        }

        IDbCommand IDbDataAdapter.SelectCommand
        {
            get { return _selectCommand; }
            set { _selectCommand = (SnowflakeDbCommand)value; }
        }

        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
            set
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
        }
    }
}
