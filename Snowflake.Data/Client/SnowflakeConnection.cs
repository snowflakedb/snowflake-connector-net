using System;
using System.Data.Common;
using Snowflake.Data.Core;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeConnection : DbConnection
    {
        internal SFSession sfSession { get; set; } 

        private ConnectionState connectionState;

        public SnowflakeConnection()
        {
            connectionState = ConnectionState.Closed;
        }

        public override string ConnectionString
        {
            get; set;
        }

        public override string Database
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override string DataSource
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override string ServerVersion
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override ConnectionState State
        {
            get
            {
                return connectionState;
            }
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            if (connectionState != ConnectionState.Closed && sfSession != null)
            {
                sfSession.close();
            }
        }

        public override void Open()
        {
            sfSession = new SFSession(ConnectionString);
            sfSession.open();
            connectionState = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            SFStatement sfStatement = new SFStatement(sfSession);
            return new SnowflakeDbCommand(this);
        }
    }
}
