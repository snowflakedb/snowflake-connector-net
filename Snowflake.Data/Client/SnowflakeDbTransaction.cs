/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using Snowflake.Data.Util;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbTransaction : DbTransaction, ISnowflakeResource
    {
        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbTransaction>();

        public event SnowflakeResourceEventHandler Disposed;

        public string ResourceID { get; } = Guid.NewGuid().ToString();

        private IsolationLevel isolationLevel;

        private SnowflakeDbConnection connection;

        private bool disposed = false;
        private bool isCommittedOrRollbacked = false;

        public SnowflakeDbTransaction(IsolationLevel isolationLevel, SnowflakeDbConnection connection)
        {
            logger.Debug("Begin transaction.");
            if (isolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            this.isolationLevel = isolationLevel;
            this.connection = connection;
            this.connection.transactions.Add(this); // Consider the case where "BEGIN" was successful but reception of the response packet failed.

            using (IDbCommand command = connection.CreateCommand())
            {
                isCommittedOrRollbacked = false;
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
            logger.Debug("Commit transaction.");
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
            logger.Debug("Rollback transaction.");
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
            }

            // Notify to references.
            this.Disposed(this, EventArgs.Empty);

            disposed = true;

            base.Dispose(disposing);
        }

        ~SnowflakeDbTransaction()
        {
            Dispose(false);
        }
    }
}
