/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbTransaction : DbTransaction
    {
        private ILogger logger = SFLoggerFactory.GetCustomLogger<SnowflakeDbTransaction>();

        private IsolationLevel isolationLevel;

        private SnowflakeDbConnection connection;

        private bool disposed = false;
        private bool isCommittedOrRollbacked = false;

        internal bool IsActive => !disposed && !isCommittedOrRollbacked;
        
        public SnowflakeDbTransaction(IsolationLevel isolationLevel, SnowflakeDbConnection connection)
        {
            logger.LogDebug("Begin transaction.");
            if (isolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }
            if (connection == null)
            {
                logger.LogError("Transaction cannot be started for an unknown connection");
                throw new SnowflakeDbException(SFError.MISSING_CONNECTION_PROPERTY);
            }
            if (!connection.IsOpen())
            {
                logger.LogError("Transaction cannot be started for a closed connection");
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            this.isolationLevel = isolationLevel;
            this.connection = connection;

            using (IDbCommand command = connection.CreateCommand())
            {
                isCommittedOrRollbacked = false;
                command.CommandText = "BEGIN";
                command.ExecuteNonQuery();
            }
            connection.ExplicitTransaction = this;
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
            logger.LogDebug("Commit transaction.");
            if (!isCommittedOrRollbacked)
            {
                using (IDbCommand command = connection.CreateCommand())
                {
                    isCommittedOrRollbacked = true;
                    command.CommandText = "COMMIT";
                    command.ExecuteNonQuery();
                }
            }
        }

        public override void Rollback()
        {
            logger.LogDebug("Rollback transaction.");
            if (!isCommittedOrRollbacked)
            {
                using (IDbCommand command = connection.CreateCommand())
                {
                    isCommittedOrRollbacked = true;
                    command.CommandText = "ROLLBACK";
                    command.ExecuteNonQuery();
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposed)
                return;
            // Rollback the uncommitted transaction when the connection is open
            if (connection != null && connection.IsOpen())
            {
                // When there is no uncommitted transaction, Snowflake would just ignore the rollback request;
                if (!isCommittedOrRollbacked)
                {
                    this.Rollback();
                }
                isCommittedOrRollbacked = true;
                connection.ExplicitTransaction = null; // let GC clean it
            }
            disposed = true;

            base.Dispose(disposing);
        }

        ~SnowflakeDbTransaction()
        {
            Dispose(false);
        }
    }
}
