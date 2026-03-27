using System;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    [System.ComponentModel.DesignerCategory("Code")]
    public class SnowflakeDbConnection : DbConnection
    {
        static SnowflakeDbConnection()
        {
            SFEnvironment.StartMinicoreLoading();
        }

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

        private readonly TomlConnectionBuilder _tomlConnectionBuilder;

        protected enum TransactionRollbackStatus
        {
            Undefined, // used to indicate ignored transaction status when pool disabled
            Success,
            Failure
        }

        public SnowflakeDbConnection() : this(TomlConnectionBuilder.Instance)
        {
        }

        public SnowflakeDbConnection(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }

        internal SnowflakeDbConnection(TomlConnectionBuilder tomlConnectionBuilder)
        {
            _tomlConnectionBuilder = tomlConnectionBuilder;
            _connectionState = ConnectionState.Closed;
            _connectionTimeout =
                int.Parse(SFSessionProperty.CONNECTION_TIMEOUT.GetAttribute<SFSessionPropertyAttr>().
                    defaultValue);
            _isArrayBindStageCreated = false;
            ExplicitTransaction = null;
        }

        public Func<HttpMessageHandler> HttpMessageHandlerFactory { get; set; }

        public override string ConnectionString
        {
            get; set;
        }

        public SecureString Password
        {
            get; set;
        }

        public SecureString Passcode { get; set; }

        public SecureString OAuthClientSecret { get; set; }

        public SecureString Token { get; set; }

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

        public void PreventPooling()
        {
            if (SfSession == null)
            {
                throw new Exception("Session not yet created for this connection. Unable to prevent the session from pooling");
            }
            SfSession.SetPooling(false);
            logger.Debug($"Session {SfSession.sessionId} marked not to be pooled any more");
        }

        internal bool HasActiveExplicitTransaction() => ExplicitTransaction != null && ExplicitTransaction.IsActive;

        private bool TryToReturnSessionToPool()
        {
            var pooling = SnowflakeDbConnectionPool.GetPooling() && SfSession.GetPooling();
            var transactionRollbackStatus = pooling ? TerminateTransactionForDirtyConnectionReturningToPool() : TransactionRollbackStatus.Undefined;
            var canReuseSession = CanReuseSession(transactionRollbackStatus);
            if (!canReuseSession)
            {
                SnowflakeDbConnectionPool.ReleaseBusySession(SfSession);
                return false;
            }
            var sessionReturnedToPool = SnowflakeDbConnectionPool.AddSession(SfSession);
            if (sessionReturnedToPool)
            {
                logger.Debug($"Session pooled: {SfSession.sessionId}");
            }
            return sessionReturnedToPool;
        }

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
            catch (Exception exception)
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
                var returnedToPool = TryToReturnSessionToPool();
                if (!returnedToPool)
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
            await CloseAsync(CancellationToken.None);
        }
#endif

        public virtual async Task CloseAsync(CancellationToken cancellationToken)
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
                    var returnedToPool = TryToReturnSessionToPool();
                    if (returnedToPool)
                    {
                        _connectionState = ConnectionState.Closed;
                        taskCompletionSource.SetResult(null);
                    }
                    else
                    {
                        await SfSession.CloseAsync(cancellationToken).ContinueWith(
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
                            }, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    logger.Debug("Session not opened. Nothing to do.");
                    taskCompletionSource.SetResult(null);
                }
            }
            await taskCompletionSource.Task;
        }

        protected virtual bool CanReuseSession(TransactionRollbackStatus transactionRollbackStatus)
        {
            return SnowflakeDbConnectionPool.GetPooling() &&
                   transactionRollbackStatus == TransactionRollbackStatus.Success;
        }

        public override void Open()
        {
            logger.Debug("Open Connection.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Debug($"Open with a connection already opened: {_connectionState}");
                return;
            }
            try
            {
                FillConnectionStringFromTomlConfigIfNotSet();
                OnSessionConnecting();
                var sessionContext = new SessionPropertiesContext
                {
                    Password = Password,
                    Passcode = Passcode,
                    OAuthClientSecret = OAuthClientSecret,
                    HttpMessageHandlerFactory = HttpMessageHandlerFactory
                };
                
                SfSession = SnowflakeDbConnectionPool.GetSession(ConnectionString, sessionContext);
                if (SfSession == null)
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Could not open session");
                logger.Debug($"Connection open with pooled session: {SfSession.sessionId}");
                OnSessionEstablished();
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                _connectionState = ConnectionState.Closed;
                logger.Error("Unable to connect: ", e);
                if (e is SnowflakeDbException)
                {
                    throw;
                }
                throw new SnowflakeDbException(
                        e,
                        SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                        SFError.INTERNAL_ERROR,
                        "Unable to connect. " + e.Message);
            }
        }

        internal void FillConnectionStringFromTomlConfigIfNotSet()
        {
            if (string.IsNullOrEmpty(ConnectionString))
            {
                ConnectionString = _tomlConnectionBuilder.GetConnectionStringFromToml();
            }
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Connection Async.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Debug($"Open with a connection already opened: {_connectionState}");
                return Task.CompletedTask;
            }
            OnSessionConnecting();
            FillConnectionStringFromTomlConfigIfNotSet();
            var sessionContext = new SessionPropertiesContext
            {
                Password = Password,
                Passcode = Passcode,
                OAuthClientSecret = OAuthClientSecret,
                HttpMessageHandlerFactory = HttpMessageHandlerFactory
            };
            
            return SnowflakeDbConnectionPool
                .GetSessionAsync(ConnectionString, sessionContext, cancellationToken)
                .ContinueWith(previousTask =>
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
                        throw new TaskCanceledException("Connecting was cancelled");
                    }
                    else
                    {
                        // Only continue if the session was opened successfully
                        SfSession = previousTask.Result;
                        logger.Debug($"Connection open with pooled session: {SfSession.sessionId}");
                        OnSessionEstablished();
                    }
                }, TaskContinuationOptions.None); // this continuation should be executed always (even if the whole operation was canceled) because it sets the proper state of the connection
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

        private void OnSessionConnecting()
        {
            _connectionState = ConnectionState.Connecting;
        }

        private void OnSessionEstablished()
        {
            _connectionTimeout = (int)SfSession.connectionTimeout.TotalSeconds;
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
            var command = DbProviderFactory.CreateCommand();
            command.Connection = this;
            return command;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    try
                    {
                        Close();
                    }
                    catch (Exception ex)
                    {
                        // Prevent an exception from being thrown when disposing of this object
                        logger.Error("Unable to close connection", ex);
                    }
                }
                else
                {
                    SfSession?.CloseNonBlocking();
                    SfSession = null;
                    _connectionState = ConnectionState.Closed;
                }

                _disposed = true;
            }

            base.Dispose(disposing);
        }

        public bool IsStillRunning(QueryStatus status)
        {
            return QueryStatusExtensions.IsStillRunning(status);
        }

        public bool IsAnError(QueryStatus status)
        {
            return QueryStatusExtensions.IsAnError(status);
        }

        ~SnowflakeDbConnection()
        {
            Dispose(false);
        }
    }
}
