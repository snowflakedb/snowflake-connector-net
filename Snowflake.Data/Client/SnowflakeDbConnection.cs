/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using Snowflake.Data.Core;
using System.Security;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Threading;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbConnection : DbConnection
    {
        internal SFSession SfSession { get; private set; } 

        private ConnectionState _connectionState;

        private int _connectionTimeout;

        public SnowflakeDbConnection()
        {
            _connectionState = ConnectionState.Closed;
            _connectionTimeout = 0;
        }

        public override string ConnectionString
        {
            get; set;
        }

        public SecureString Password
        {
            get; set;
        }

        public override string Database => _connectionState == ConnectionState.Open ? SfSession.database : string.Empty;

        public override int ConnectionTimeout => this._connectionTimeout;

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

        public override string ServerVersion => _connectionState == ConnectionState.Open ? SfSession.serverVersion : "";

        public override ConnectionState State => _connectionState;

        public override void ChangeDatabase(string databaseName)
        {
            string alterDbCommand = $"use database {databaseName}";

            using (IDbCommand cmd = this.CreateCommand())
            {
                cmd.CommandText = alterDbCommand;
                cmd.ExecuteNonQuery();
            }
        }

        public override void Close()
        {
            if (_connectionState != ConnectionState.Closed && SfSession != null)
            {
                SfSession.close();
            }
        }

        public override void Open()
        {
            SetSession();
            SfSession.Open();
            OnSessionEstablished();
        }
        
        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);

            SetSession();
            //TODO: Respect the cancellation tokens...
            return SfSession.OpenAsync().ContinueWith(t => OnSessionEstablished(), cancellationToken);
        }

        private void SetSession()
        {
            SfSession = new SFSession(ConnectionString, Password);
            _connectionTimeout = SfSession.connectionTimeout;
            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            _connectionState = ConnectionState.Open;
        }




        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new SnowflakeDbTransaction(isolationLevel, this);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new SnowflakeDbCommand(this);
        }
    }
}
