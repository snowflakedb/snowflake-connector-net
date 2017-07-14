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
    public class SnowflakeDbConnection : DbConnection
    {
        internal SFSession sfSession { get; set; } 

        private ConnectionState connectionState;

        public SnowflakeDbConnection()
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
                return connectionState == ConnectionState.Open ? sfSession.database : "";
            }
        }

        /// <summary>
        ///     If the connection to the database is closed, the DataSource returns whatever is contained 
        ///     in the ConnectionString for the DataSource keyword. If the connection is open and the 
        ///     ConnectionString data source keyword's value starts with "|datadirectory|", the property 
        ///     returns whatever is contained in the ConnectionString for the DataSource keyword only. If 
        ///     the connection to the database is open, the property returns what the native provider 
        ///     returns for the DBPROP_INIT_DATASOURCE, and if that is empty, the native provider's 
        ///     DBPROP_DATASOURCENAME is returned.
        /// </summary>
        public override string DataSource
        {
            get
            {
                return "";
            }
        }

        public override string ServerVersion
        {
            get
            {
                return connectionState == ConnectionState.Open ? sfSession.serverVersion : "";
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
            string alterDbCommand = String.Format("use database {0}", databaseName);

            using (IDbCommand cmd = this.CreateCommand())
            {
                cmd.CommandText = alterDbCommand;
                cmd.ExecuteNonQuery();
            }
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
