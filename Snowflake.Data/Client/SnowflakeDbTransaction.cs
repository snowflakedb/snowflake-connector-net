﻿/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Core;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbTransaction : DbTransaction
    {
        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbTransaction>();

        private IsolationLevel isolationLevel;

        private SnowflakeDbConnection connection;

        public SnowflakeDbTransaction(IsolationLevel isolationLevel, SnowflakeDbConnection connection)
        {
            logger.Debug("Begin transaction.");
            if (isolationLevel != IsolationLevel.ReadCommitted)
            {
                throw new SnowflakeDbException(SFError.UNSUPPORTED_FEATURE);
            }

            this.isolationLevel = isolationLevel;
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
            logger.Debug("Commit transaction.");
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();
            }
        }

        public override void Rollback()
        {
            logger.Debug("Rollback transaction.");
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
            }
        }
    }
}
