using System;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbTransaction : DbTransaction
    {
        private IsolationLevel isolationLevel;

        private SnowflakeDbConnection connection;

        public SnowflakeDbTransaction(IsolationLevel isolationLevel, SnowflakeDbConnection connection)
        {
            if (isolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new NotImplementedException(); 
            }

            this.isolationLevel = IsolationLevel;
            this.connection = connection;

            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "BEGIN";
                command.ExecuteNonQuery();
            }
        }

        public override IsolationLevel IsolationLevel
        {
            get
            {
                return isolationLevel;
            }
        }

        protected override DbConnection DbConnection
        {
            get
            {
                return connection;
            }
        }

        public override void Commit()
        {
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();
            }
        }

        public override void Rollback()
        {
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
            }
        }
    }
}
