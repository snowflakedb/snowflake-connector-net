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

        protected override DbProviderFactory DbProviderFactory => new SnowflakeDbFactory();
        
        internal int _connectionTimeout;

        private bool _disposed = false;

        private static Mutex _arraybindingMutex = new Mutex();

        // TBD this doesn't make sense to have a static flag while reset it
        // in each instance.
        // Likely this should be a non-static one and the Mutex as well (if
        // it's needed) since the stage is created per session
        // Will fix that in a separated PR though as it's a different issue
        private static Boolean _isArrayBindStageCreated;

        private enum TransactionRollbackStatus
        {
            Undefined, // used to indicate ignored transaction status when pool disabled
            Success,
            Failure
        }

        public SnowflakeDbConnection()
        {
            _connectionState = ConnectionState.Closed;
            _connectionTimeout = 
                int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().
                    defaultValue);
            _isArrayBindStageCreated = false;
            ExplicitTransaction = null;
        }

        public SnowflakeDbConnection(string connectionString) : this()
        {
            ConnectionString = connectionString;
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
            return _connectionState == ConnectionState.Open && SfSession != null;
        }

        private bool IsNonClosedWithSession()
        {
            return _connectionState != ConnectionState.Closed && SfSession != null;
        }

        public override string Database => IsOpen() ? SfSession.database : string.Empty;

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

        public override string ServerVersion => IsOpen() ? SfSession.serverVersion : String.Empty;

        public override ConnectionState State => _connectionState;
        internal SnowflakeDbTransaction ExplicitTransaction { get; set; } // tracks only explicit transaction operations
        
        internal bool HasActiveExplicitTransaction() => ExplicitTransaction != null && ExplicitTransaction.IsActive;

        private TransactionRollbackStatus TerminateTransactionForDirtyConnectionReturningToPool()
        {
            if (!HasActiveExplicitTransaction())
                return TransactionRollbackStatus.Success;
            try
            {
                logger.Debug("Closing dirty connection: an active transaction exists in session: " + SfSession.sessionId);
                using (IDbCommand command = CreateCommand())
                {
                    command.CommandText = "ROLLBACK";
                    command.ExecuteNonQuery();
                    // error to indicate a problem within application code that a connection was closed while still having a pending transaction
                    logger.Error("Closing dirty connection: rollback transaction in session " + SfSession.sessionId + " succeeded.");
                    ExplicitTransaction = null;
                    return TransactionRollbackStatus.Success; 
                }
            }
            catch (SnowflakeDbException exception)
            {
                // error to indicate a problem with rollback of an active transaction and inability to return dirty connection to the pool 
                logger.Error("Closing dirty connection: rollback transaction in session: " + SfSession.sessionId + " failed, exception: " + exception.Message);
                return TransactionRollbackStatus.Failure; // connection won't be pooled
            }
        }

        public override void ChangeDatabase(string databaseName)
        {
            logger.Debug($"ChangeDatabase to:{databaseName}");

            string alterDbCommand = $"use database {databaseName}";

            using (IDbCommand cmd = CreateCommand())
            {
                cmd.CommandText = alterDbCommand;
                cmd.ExecuteNonQuery();
            }
        }

        public override void Close()
        {
            logger.Debug("Close Connection.");
            if (IsNonClosedWithSession())
            {
                var transactionRollbackStatus = GetPooling() ? TerminateTransactionForDirtyConnectionReturningToPool() : TransactionRollbackStatus.Undefined;
                
                if (CanReuseSession(transactionRollbackStatus) && SnowflakeDbConnectionPool.AddSession(ConnectionString, Password, SfSession))
                {
                    logger.Debug($"Session pooled: {SfSession.sessionId}");
                }
                else
                {
                    SfSession.close();
                }
                SfSession = null;
            }

            _connectionState = ConnectionState.Closed;
        }

#if NETCOREAPP3_0_OR_GREATER
        // CloseAsync was added to IDbConnection as part of .NET Standard 2.1, first supported by .NET Core 3.0.
        // Adding an override for CloseAsync will prevent the need for casting to SnowflakeDbConnection to call CloseAsync(CancellationToken).
        public override async Task CloseAsync()
        {
            await CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
#endif

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
                if (IsNonClosedWithSession())
                {
                    var transactionRollbackStatus = GetPooling() ? TerminateTransactionForDirtyConnectionReturningToPool() : TransactionRollbackStatus.Undefined;

                    if (CanReuseSession(transactionRollbackStatus) && SnowflakeDbConnectionPool.AddSession(ConnectionString, Password, SfSession))
                    {
                        logger.Debug($"Session pooled: {SfSession.sessionId}");
                        _connectionState = ConnectionState.Closed;
                        taskCompletionSource.SetResult(null);
                    }
                    else
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
                                    _connectionState = ConnectionState.Closed;
                                    taskCompletionSource.SetResult(null);
                                }
                            }, cancellationToken);
                    }
                }
                else
                {
                    logger.Debug("Session not opened. Nothing to do.");
                    taskCompletionSource.SetResult(null);
                }
            }
            return taskCompletionSource.Task;
        }

        private bool CanReuseSession(TransactionRollbackStatus transactionRollbackStatus)
        {
            return GetPooling() && 
                   transactionRollbackStatus == TransactionRollbackStatus.Success;
        }
        
        private bool GetPooling()
        {
            return SnowflakeDbConnectionPool.GetPool(ConnectionString, Password).GetPooling();
        }

        public override void Open()
        {
            logger.Debug("Open Connection.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Warn($"Opening a connection already opened: {_connectionState}");
                return;
            }

            try
            {
                OnSessionOpen();
                SfSession = SnowflakeDbConnectionPool.GetSession(ConnectionString, Password);
                OnSessionEstablished();
            }
            catch (Exception e)
            {
                RethrowOnSessionOpenFailure(e);
            }
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Connection Async.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Warn($"Opening a connection already opened: {_connectionState}");
                return Task.CompletedTask;
            }

            OnSessionOpen();
            return SnowflakeDbConnectionPool.GetSessionAsync(ConnectionString, Password, cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                        // Exception from SfSession.OpenAsync
                        RethrowOnSessionOpenFailure(previousTask.Exception);
                    }
                    else if (previousTask.IsCanceled)
                    {
                        _connectionState = ConnectionState.Closed;
                        logger.Debug("Connection canceled");
                    }
                    else
                    {
                        logger.Debug($"Connection open with pooled session: {SfSession.sessionId}");
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
        
        private void OnSessionOpen()
        {
            logger.Debug("Opening session");
            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            if (SfSession == null)
            {
                logger.Error("Error during opening session");
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Unable to establish a session");
            }
            logger.Debug("Session established");
            _connectionState = ConnectionState.Open;
            _connectionTimeout = (int)SfSession.connectionTimeout.TotalSeconds;
            logger.Debug($"Connection open with pooled session: {SfSession.sessionId}");
        }

        private void RethrowOnSessionOpenFailure(Exception exception)
        {
            // Otherwise when Dispose() is called, the close request would timeout.
            _connectionState = ConnectionState.Closed;
            logger.Error("Unable to connect: ", exception);
            if (exception != null && exception is SnowflakeDbException dbException)
                throw dbException;

            var errorMessage = "Unable to connect. " + (exception != null ? exception.Message : "");
            throw new SnowflakeDbException(
                exception,
                SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                SFError.INTERNAL_ERROR,
                errorMessage);
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
            var command = DbProviderFactory.CreateCommand();
            command.Connection = this;
            return command;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            try
            {
                Close();
            } 
            catch (Exception ex)
            {
                // Prevent an exception from being thrown when disposing of this object
                logger.Error("Unable to close connection", ex);
            }

            _disposed = true;

            base.Dispose(disposing);
        }


        /// <summary>
        ///     Register cancel callback. Two factors: either external cancellation token passed down from upper
        ///     layer or timeout reached. Whichever comes first would trigger query cancellation.
        /// </summary>
        /// <param name="externalCancellationToken">cancellation token from upper layer</param>
        internal void RegisterConnectionCancellationCallback(CancellationToken externalCancellationToken)
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
