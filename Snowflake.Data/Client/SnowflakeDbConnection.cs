/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Data.Common;
using Snowflake.Data.Core;
using System.Security;
using System.Threading.Tasks;
using System.Data;
using System.Threading;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbConnection : DbConnection
    {
        private SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        internal SFSession SfSession { get; set; } 

        internal ConnectionState _connectionState;

        internal int _connectionTimeout;

        private bool disposed = false;

        private static Mutex _arraybindingMutex = new Mutex();

        private static Boolean _isArrayBindStageCreated;

        public SnowflakeDbConnection()
        {
            _connectionState = ConnectionState.Closed;
            _connectionTimeout = 
                int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().
                    defaultValue);
            _isArrayBindStageCreated = false;
        }

        public override string ConnectionString
        {
            get; set;
        }

        public SecureString Password
        {
            get; set;
        }

        public bool IsOpen()
        {
            return _connectionState == ConnectionState.Open;
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
        ///     Note: not yet implemented
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
            logger.Debug($"ChangeDatabase to:{databaseName}");

            string alterDbCommand = $"use database {databaseName}";

            using (IDbCommand cmd = this.CreateCommand())
            {
                cmd.CommandText = alterDbCommand;
                cmd.ExecuteNonQuery();
            }
        }

        public override void Close()
        {
            logger.Debug("Close Connection.");

            if (_connectionState != ConnectionState.Closed && SfSession != null)
            {
                SfSession.close();
            }
            _connectionState = ConnectionState.Closed;
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Close Connection.");
            TaskCompletionSource<object> taskCompletionSource = new TaskCompletionSource<object>();

            if (cancellationToken.IsCancellationRequested)
            {
                taskCompletionSource.SetCanceled();
            }
            else
            {
                if (_connectionState != ConnectionState.Closed && SfSession != null)
                {
                    SfSession.CloseAsync(cancellationToken).ContinueWith(
                        previousTask =>
                        {
                            if (previousTask.IsFaulted)
                            {
                                // Exception from SfSession.CloseAsync
                                logger.Error("Error closing the session", previousTask.Exception);
                                taskCompletionSource.SetException(previousTask.Exception);
                            }
                            else if (previousTask.IsCanceled)
                            {
                                _connectionState = ConnectionState.Closed;
                                logger.Debug("Session close canceled");
                                taskCompletionSource.SetCanceled();
                            }
                            else
                            {
                                logger.Debug("Session closed successfully");
                                taskCompletionSource.SetResult(null);
                                _connectionState = ConnectionState.Closed;
                            }
                        }, cancellationToken);
                }
                else
                {
                    logger.Debug("Session not opened. Nothing to do.");
                    taskCompletionSource.SetResult(null);
                }
            }
            return taskCompletionSource.Task;
        }

        public override void Open()
        {
            logger.Debug("Open Connection.");
            SetSession();
            try
            {
                SfSession.Open();
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                _connectionState = ConnectionState.Closed;
                logger.Error("Unable to connect", e);
                if (!(e.GetType() == typeof(SnowflakeDbException)))
                {
                    throw
                       new SnowflakeDbException(
                           e,
                           SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                           SFError.INTERNAL_ERROR,
                           "Unable to connect. " + e.Message);
                }
                else
                {
                    throw;
                }
            }
            OnSessionEstablished();
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Connection.");
            registerConnectionCancellationCallback(cancellationToken);
            SetSession();

            return SfSession.OpenAsync(cancellationToken).ContinueWith(
                previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                        // Exception from SfSession.OpenAsync
                        Exception sfSessionEx = previousTask.Exception;
                        _connectionState = ConnectionState.Closed;
                        logger.Error("Unable to connect", sfSessionEx);
                        throw new SnowflakeDbException(
                           sfSessionEx,
                           SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                           SFError.INTERNAL_ERROR,
                           "Unable to connect");
                    }
                    else if (previousTask.IsCanceled)
                    {
                        _connectionState = ConnectionState.Closed;
                        logger.Debug("Connection canceled");
                    }
                    else
                    {
                        logger.Debug("All good");
                        // Only continue if the session was opened successfully
                        OnSessionEstablished();
                    }
                },
                cancellationToken);
        }

        public Mutex GetArrayBindingMutex()
        {
            return _arraybindingMutex;
        }

        public bool IsArrayBindStageCreated()
        {
            return _isArrayBindStageCreated;
        }

        public void SetArrayBindStageCreated()
        {
            _isArrayBindStageCreated = true;
        }

        /// <summary>
        /// Create a new SFsession with the connection string settings.
        /// </summary>
        /// <exception cref="SnowflakeDbException">If the connection string can't be processed</exception>
        private void SetSession()
        {
            SfSession = new SFSession(ConnectionString, Password);
            _connectionTimeout = (int)SfSession.connectionTimeout.TotalSeconds;
            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            _connectionState = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            // Parameterless BeginTransaction() method of the super class calls this method with IsolationLevel.Unspecified,
            // Change the isolation level to ReadCommitted
            if (isolationLevel == IsolationLevel.Unspecified)
            {
                isolationLevel = IsolationLevel.ReadCommitted;
            }

            return new SnowflakeDbTransaction(isolationLevel, this);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new SnowflakeDbCommand(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposed)
                return;

            try
            {
                this.Close();
            } 
            catch (Exception ex)
            {
                // Prevent an exception from being thrown when disposing of this object
                logger.Error("Unable to close connection", ex);
            }
            
            disposed = true;

            base.Dispose(disposing);
        }


        /// <summary>
        ///     Register cancel callback. Two factors: either external cancellation token passed down from upper
        ///     layer or timeout reached. Whichever comes first would trigger query cancellation.
        /// </summary>
        /// <param name="externalCancellationToken">cancellation token from upper layer</param>
        internal void registerConnectionCancellationCallback(CancellationToken externalCancellationToken)
        {
            if (!externalCancellationToken.IsCancellationRequested)
            {
                externalCancellationToken.Register(() => { _connectionState = ConnectionState.Closed; });
            }
        }

        ~SnowflakeDbConnection()
        {
            Dispose(false);
        }
    }
}
