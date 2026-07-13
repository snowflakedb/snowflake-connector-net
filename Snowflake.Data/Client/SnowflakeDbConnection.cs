using System;
using System.Data;
using System.Data.Common;
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

        /// <summary>
        /// Initializes a new instance of <see cref="SnowflakeDbConnection"/> with default settings.
        /// </summary>
        public SnowflakeDbConnection() : this(TomlConnectionBuilder.Instance)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="SnowflakeDbConnection"/> with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string used to connect to Snowflake.</param>
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

        /// <summary>
        /// Gets or sets the connection string used to connect to Snowflake.
        /// </summary>
        public override string ConnectionString
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the password as a <see cref="SecureString"/> for authentication.
        /// </summary>
        public SecureString Password
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the MFA passcode as a <see cref="SecureString"/> for multi-factor authentication.
        /// </summary>
        public SecureString Passcode { get; set; }

        /// <summary>
        /// Gets or sets the OAuth client secret as a <see cref="SecureString"/> for OAuth authentication.
        /// </summary>
        public SecureString OAuthClientSecret { get; set; }

        /// <summary>
        /// Gets or sets the OAuth token as a <see cref="SecureString"/> for token-based authentication.
        /// </summary>
        public SecureString Token { get; set; }

        /// <summary>
        /// Returns whether the connection is currently open and has an active session.
        /// </summary>
        /// <returns><c>true</c> if the connection state is open and a session exists; otherwise, <c>false</c>.</returns>
        public bool IsOpen()
        {
            return _connectionState == ConnectionState.Open && SfSession != null;
        }

        /// <summary>
        /// Gets the name of the current database if the connection is open; otherwise, returns an empty string.
        /// </summary>
        public override string Database => IsOpen() ? SfSession.database : string.Empty;

        /// <summary>
        /// Gets the connection timeout in seconds.
        /// </summary>
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

        /// <summary>
        /// Gets the Snowflake server version if the connection is open; otherwise, returns an empty string.
        /// </summary>
        public override string ServerVersion => IsOpen() ? SfSession.serverVersion : String.Empty;

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public override ConnectionState State => _connectionState;
        internal SnowflakeDbTransaction ExplicitTransaction { get; set; } // tracks only explicit transaction operations

        /// <summary>
        /// Marks the underlying session to not be returned to the connection pool when the connection is closed.
        /// </summary>
        /// <exception cref="Exception">Thrown when no session has been created for this connection.</exception>
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

        /// <summary>
        /// Changes the current database for an open connection by executing a USE DATABASE command.
        /// </summary>
        /// <param name="databaseName">The name of the database to switch to.</param>
        public override void ChangeDatabase(string databaseName)
        {
            logger.Debug($"ChangeDatabase to:{databaseName}");

            using (var cmd = CreateCommand())
            {
                cmd.CommandText = "use database identifier(?)";
                var dbParam = cmd.CreateParameter();
                dbParam.ParameterName = "1";
                dbParam.DbType = DbType.String;
                dbParam.Value = databaseName;
                cmd.Parameters.Add(dbParam);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Closes the connection to the database. If connection pooling is enabled, the session may be returned to the pool.
        /// </summary>
        public override void Close()
        {
            logger.Debug("Close Connection.");

            if (SfSession == null || _connectionState is not ConnectionState.Open and not ConnectionState.Broken)
            {
                logger.Debug("Session not opened. Nothing to do.");
                return;
            }

            try
            {
                var returnedToPool = TryToReturnSessionToPool();
                if (!returnedToPool)
                    SfSession.close();

                logger.Debug("Session closed successfully");
                SfSession = null;
                _connectionState = ConnectionState.Closed;
            }
            catch (Exception exception)
            {
                logger.Error($"Close Connection failed: {exception.Message}");
                _connectionState = ConnectionState.Broken;
                throw;
            }
        }

#if NETCOREAPP3_0_OR_GREATER
        /// <summary>
        /// Asynchronously closes the connection to the database.
        /// </summary>
        /// <returns>A task representing the asynchronous close operation.</returns>
        // CloseAsync was added to IDbConnection as part of .NET Standard 2.1, first supported by .NET Core 3.0.
        // Adding an override for CloseAsync will prevent the need for casting to SnowflakeDbConnection to call CloseAsync(CancellationToken).
        public override async Task CloseAsync()
        {
            await CloseAsync(CancellationToken.None);
        }
#endif

        /// <summary>
        /// Asynchronously closes the connection to the database with cancellation support.
        /// If connection pooling is enabled, the session may be returned to the pool.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the asynchronous operation. If the operation is successfully canceled, <see cref="ConnectionState"/> of this connection will remain unchanged.</param>
        /// <returns>A task representing the asynchronous close operation.</returns>
        public virtual async Task CloseAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Close Connection.");

            if (cancellationToken.IsCancellationRequested)
                return;

            if (SfSession == null || _connectionState is not ConnectionState.Open and not ConnectionState.Broken)
            {
                logger.Debug("Session not opened. Nothing to do.");
                return;
            }

            try
            {
                var returnedToPool = TryToReturnSessionToPool();
                if (!returnedToPool)
                    await SfSession.CloseAsync(cancellationToken).ConfigureAwait(false);

                logger.Debug("Session closed successfully");
                SfSession = null;
                _connectionState = ConnectionState.Closed;
            }
            catch (OperationCanceledException)
            {
                logger.Debug("Session close canceled");
                throw;
            }
            catch (Exception exception)
            {
                _connectionState = ConnectionState.Broken;
                logger.Error("Error closing the session", exception);
                throw;
            }
        }

        protected virtual bool CanReuseSession(TransactionRollbackStatus transactionRollbackStatus)
        {
            return SnowflakeDbConnectionPool.GetPooling() &&
                   transactionRollbackStatus == TransactionRollbackStatus.Success;
        }

        /// <summary>
        /// Opens a connection to the Snowflake.
        /// </summary>
        /// <exception cref="SnowflakeDbException">Thrown when the connection cannot be established.</exception>
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
                OnSessionConnecting();
                FillConnectionStringFromTomlConfigIfNotSet();
                var sessionContext = new SessionPropertiesContext
                {
                    Password = Password,
                    Passcode = Passcode,
                    OAuthClientSecret = OAuthClientSecret,
                    Token = Token
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
                    throw;

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
                ConnectionString = _tomlConnectionBuilder.GetConnectionStringFromToml();
        }

        /// <summary>
        /// Asynchronously opens a connection to the Snowflake.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
        /// <returns>A task representing the asynchronous open operation.</returns>
        /// <exception cref="SnowflakeDbException">Thrown when the connection cannot be established.</exception>
        /// <exception cref="TaskCanceledException">Thrown when the operation is canceled.</exception>
        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            logger.Debug("Open Connection Async.");
            if (_connectionState != ConnectionState.Closed)
            {
                logger.Debug($"Open with a connection already opened: {_connectionState}");
                return;
            }
            var sessionContext = new SessionPropertiesContext
            {
                Password = Password,
                Passcode = Passcode,
                OAuthClientSecret = OAuthClientSecret,
                Token = Token
            };

            try
            {
                OnSessionConnecting();
                FillConnectionStringFromTomlConfigIfNotSet();
                var session = await SnowflakeDbConnectionPool.GetSessionAsync(ConnectionString, sessionContext, cancellationToken).ConfigureAwait(false);
                SfSession = session;
                logger.Debug($"Connection open with pooled session: {SfSession.sessionId}");
                OnSessionEstablished();
            }
            catch (OperationCanceledException)
            {
                _connectionState = ConnectionState.Closed;
                logger.Debug("Connection canceled");
                throw new TaskCanceledException("Connecting was cancelled");
            }
            catch (Exception ex)
            {
                _connectionState = ConnectionState.Closed;
                logger.Error("Unable to connect", ex);

                if (ex is SnowflakeDbException)
                    throw;

                throw new SnowflakeDbException(ex, SnowflakeDbException.CONNECTION_FAILURE_SSTATE, SFError.INTERNAL_ERROR, "Unable to connect");
            }
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

        /// <summary>
        /// Determines whether the given query status indicates the query is still running.
        /// </summary>
        /// <param name="status">The query status to check.</param>
        /// <returns><c>true</c> if the query is still running; otherwise, <c>false</c>.</returns>
        public bool IsStillRunning(QueryStatus status)
        {
            return QueryStatusExtensions.IsStillRunning(status);
        }

        /// <summary>
        /// Determines whether the given query status indicates an error.
        /// </summary>
        /// <param name="status">The query status to check.</param>
        /// <returns><c>true</c> if the query status represents an error; otherwise, <c>false</c>.</returns>
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
