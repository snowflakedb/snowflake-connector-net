using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{

    sealed class SessionPool : IDisposable
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SessionPool>();
        private readonly object _sessionPoolLock = new object();
        private static ISessionFactory s_sessionFactory = new SessionFactory();

        private readonly Guid _id = Guid.NewGuid();
        internal readonly List<SFSession> _idleSessions;
        private readonly IWaitingQueue _waitingForIdleSessionQueue;
        private readonly ISessionCreationTokenCounter _sessionCreationTokenCounter;
        private readonly ISessionCreationTokenCounter _noPoolingSessionCreationTokenCounter = new NonCountingSessionCreationTokenCounter();
        internal string ConnectionString { get; }
        internal SecureString Password { get; }
        internal SecureString OAuthClientSecret { get; }
        internal SecureString Token { get; }
        private readonly string _connectionStringWithoutSecrets;
        private readonly ICounter _busySessionsCounter;
        private ISessionPoolEventHandler _sessionPoolEventHandler = new SessionPoolEventHandler(); // a way to inject some additional behaviour after certain events. Can be used for example to measure time of given steps.
        private readonly ConnectionPoolConfig _poolConfig;
        private bool _configOverriden = false;
        private bool _underDestruction = false;

        private static readonly InvalidOperationException s_notSupportedInCachePoolException = new InvalidOperationException("Feature not supported in a Connection Cache");

        private SessionPool()
        {
            // acquiring a lock not needed because one is already acquired in SnowflakeDbConnectionPool
            _idleSessions = new List<SFSession>();
            _busySessionsCounter = new FixedZeroCounter();
            _waitingForIdleSessionQueue = new NonWaitingQueue();
            _sessionCreationTokenCounter = new NonCountingSessionCreationTokenCounter();
            _poolConfig = new ConnectionPoolConfig();
        }

        private SessionPool(string connectionString, SecureString password, SecureString oauthClientSecret, SecureString token, ConnectionPoolConfig poolConfig, string connectionStringWithoutSecrets)
        {
            // acquiring a lock not needed because one is already acquired in ConnectionPoolManager
            _idleSessions = new List<SFSession>();
            _busySessionsCounter = new NonNegativeCounter();
            ConnectionString = connectionString;
            Password = password;
            OAuthClientSecret = oauthClientSecret;
            Token = token;
            _connectionStringWithoutSecrets = connectionStringWithoutSecrets;
            _waitingForIdleSessionQueue = new WaitingQueue();
            _poolConfig = poolConfig;
            _sessionCreationTokenCounter = new SessionCreationTokenCounter(_poolConfig.ConnectionTimeout);
        }

        internal static SessionPool CreateSessionCache() => new SessionPool();

        internal static SessionPool CreateSessionPool(string connectionString, SecureString password, SecureString oauthClientSecret, SecureString token)
        {
            s_logger.Debug("Creating a connection pool");
            var propertiesContext = new SessionPropertiesContext { Password = password, OAuthClientSecret = oauthClientSecret, Token = token };
            var extracted = ExtractConfig(connectionString, propertiesContext);
            s_logger.Debug("Creating a connection pool identified by: " + extracted.Item2);
            return new SessionPool(connectionString, password, oauthClientSecret, token, extracted.Item1, extracted.Item2);
        }

        ~SessionPool()
        {
            // Use async for the finalizer due to possible deadlock
            // when waiting for the CloseResponse task while closing the session
            DestroyPoolAsync();
        }

        public void Dispose()
        {
            DestroyPool();
        }

        internal static ISessionFactory SessionFactory
        {
            set => s_sessionFactory = value;
        }

        private void CleanExpiredSessions()
        {
            s_logger.Debug("SessionPool::CleanExpiredSessions" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                var timeNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                foreach (var item in _idleSessions.ToList())
                {
                    if (item.IsExpired(_poolConfig.ExpirationTimeout, timeNow))
                    {
                        Task.Run(() => item.close());
                        _idleSessions.Remove(item);
                    }
                }
            }
        }

        internal static Tuple<ConnectionPoolConfig, string> ExtractConfig(string connectionString, SessionPropertiesContext propertiesContext)
        {
            try
            {
                var properties = SFSessionProperties.ParseConnectionString(connectionString, propertiesContext);
                var extractedProperties = SFSessionHttpClientProperties.ExtractAndValidate(properties);
                extractedProperties.DisablePoolingDefaultIfSecretsProvidedExternally(properties);
                return Tuple.Create(extractedProperties.BuildConnectionPoolConfig(), properties.ConnectionStringWithoutSecrets);
            }
            catch (Exception exception)
            {
                s_logger.Error("Failed to extract pool configuration", exception);
                throw;
            }
        }

        internal void ValidateSecurePassword(SecureString password)
        {
            if (!ExtractSecureString(Password).Equals(ExtractSecureString(password)))
            {
                var errorMessage = "Could not get a pool because of password mismatch";
                s_logger.Error(errorMessage + PoolIdentification());
                throw new Exception(errorMessage);
            }
        }

        internal void ValidateSecureOAuthClientSecret(SecureString oauthClientSecret)
        {
            if (!ExtractSecureString(OAuthClientSecret).Equals(ExtractSecureString(oauthClientSecret)))
            {
                var errorMessage = "Could not get a pool because of oauth client secret mismatch";
                s_logger.Error(errorMessage + PoolIdentification());
                throw new Exception(errorMessage);
            }
        }

        internal void ValidateSecureToken(SecureString token)
        {
            if (!ExtractSecureString(Token).Equals(ExtractSecureString(token)))
            {
                var errorMessage = "Could not get a pool because of token mismatch";
                s_logger.Error(errorMessage + PoolIdentification());
                throw new Exception(errorMessage);
            }
        }

        private string ExtractSecureString(SecureString secret) =>
            secret == null ? string.Empty : SecureStringHelper.Decode(secret);

        internal SFSession GetSession(string connStr, SessionPropertiesContext sessionContext)
        {
            s_logger.Debug("SessionPool::GetSession" + PoolIdentification());
            var sessionProperties = SFSessionProperties.ParseConnectionString(connStr, sessionContext);
            ValidateMinPoolSizeWithPasscode(sessionProperties, sessionContext.Passcode);
            if (!GetPooling())
                return NewNonPoolingSession(connStr, sessionContext);
            var isMfaAuthentication = sessionProperties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator) &&
                                      MFACacheAuthenticator.IsMfaCacheAuthenticator(authenticator);
            var sessionOrCreateTokens = GetIdleSession(connStr, isMfaAuthentication ? 1 : int.MaxValue);
            if (sessionOrCreateTokens.Session != null)
            {
                _sessionPoolEventHandler.OnSessionProvided(this);
            }
            ScheduleNewIdleSessions(connStr, sessionContext, sessionOrCreateTokens.BackgroundSessionCreationTokens());
            WarnAboutOverridenConfig();
            var session = sessionOrCreateTokens.Session ?? NewSession(connStr, sessionContext, sessionOrCreateTokens.SessionCreationToken());
            if (isMfaAuthentication)
            {
                ScheduleNewIdleSessions(connStr, sessionContext, RegisterSessionCreationsToEnsureMinPoolSize());
            }
            return session;
        }

        private void ValidateMinPoolSizeWithPasscode(SFSessionProperties sessionProperties, SecureString passcode)
        {
            if (!GetPooling() || !IsMultiplePoolsVersion() || _poolConfig.MinPoolSize == 0) return;
            var isUsingPasscode = (passcode != null && passcode.Length > 0) || (sessionProperties.IsNonEmptyValueProvided(SFSessionProperty.PASSCODE) ||
                                                                                (sessionProperties.TryGetValue(SFSessionProperty.PASSCODEINPASSWORD, out var passcodeInPasswordValue) &&
                                                                                 bool.TryParse(passcodeInPasswordValue, out var isPasscodeinPassword) && isPasscodeinPassword));
            var isMfaAuthenticator = sessionProperties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator) &&
                                     MFACacheAuthenticator.IsMfaCacheAuthenticator(authenticator);
            if (isUsingPasscode && !isMfaAuthenticator)
            {
                const string ErrorMessage = "Passcode with MinPoolSize feature of connection pool allowed only for username_password_mfa authentication";
                s_logger.Error(ErrorMessage + PoolIdentification());
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, ErrorMessage);
            }
        }

        internal async Task<SFSession> GetSessionAsync(string connStr, SessionPropertiesContext sessionContext, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::GetSessionAsync" + PoolIdentification());
            var sessionProperties = SFSessionProperties.ParseConnectionString(connStr, sessionContext);
            ValidateMinPoolSizeWithPasscode(sessionProperties, sessionContext.Passcode);
            if (!GetPooling())
                return await NewNonPoolingSessionAsync(connStr, sessionContext, cancellationToken).ConfigureAwait(false);
            var isMfaAuthentication = sessionProperties.TryGetValue(SFSessionProperty.AUTHENTICATOR, out var authenticator) &&
                                      MFACacheAuthenticator.IsMfaCacheAuthenticator(authenticator);
            var sessionOrCreateTokens = GetIdleSession(connStr, isMfaAuthentication ? 1 : int.MaxValue);
            WarnAboutOverridenConfig();

            if (sessionOrCreateTokens.Session != null)
            {
                _sessionPoolEventHandler.OnSessionProvided(this);
            }
            ScheduleNewIdleSessions(connStr, sessionContext, sessionOrCreateTokens.BackgroundSessionCreationTokens());
            WarnAboutOverridenConfig();
            var session = sessionOrCreateTokens.Session ??
                          await NewSessionAsync(connStr, sessionContext, sessionOrCreateTokens.SessionCreationToken(), cancellationToken)
                              .ConfigureAwait(false);
            if (isMfaAuthentication)
            {
                ScheduleNewIdleSessions(connStr, sessionContext, RegisterSessionCreationsToEnsureMinPoolSize());
            }
            return session;

        }

        private void ScheduleNewIdleSessions(string connStr, SessionPropertiesContext sessionContext, List<SessionCreationToken> tokens)
        {
            tokens.ForEach(token => ScheduleNewIdleSession(connStr, sessionContext, token));
        }

        private void ScheduleNewIdleSession(string connStr, SessionPropertiesContext sessionContext, SessionCreationToken token)
        {
            Task.Run(() =>
            {
                var session = NewSession(connStr, sessionContext, token);
                AddSession(session, false); // we don't want to ensure min pool size here because we could get into infinite recursion if expirationTimeout would be very low
            });
        }

        private void WarnAboutOverridenConfig()
        {
            if (IsConfigOverridden() && GetPooling() && IsMultiplePoolsVersion())
            {
                s_logger.Warn("Providing a connection from a pool for which technical configuration has been overriden by the user");
            }
        }

        internal bool IsConfigOverridden() => _configOverriden;

        internal void SetSessionPoolEventHandler(ISessionPoolEventHandler sessionPoolEventHandler)
        {
            _sessionPoolEventHandler = sessionPoolEventHandler;
        }

        private SessionOrCreationTokens GetIdleSession(string connStr, int maxSessions)
        {
            s_logger.Debug("SessionPool::GetIdleSession" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                if (_waitingForIdleSessionQueue.IsAnyoneWaiting())
                {
                    s_logger.Debug("SessionPool::GetIdleSession - someone is already waiting for a session, request is going to be queued" + PoolIdentification());
                }
                else
                {
                    var session = ExtractIdleSession(connStr);
                    if (session != null)
                    {
                        s_logger.Debug("SessionPool::GetIdleSession - no thread was waiting for a session, an idle session was retrieved from the pool" + PoolIdentification());
                        return new SessionOrCreationTokens(session);
                    }
                    s_logger.Debug("SessionPool::GetIdleSession - no thread was waiting for a session, but could not find any idle session available in the pool" + PoolIdentification());
                    var sessionsCount = AllowedNumberOfNewSessionCreations(1, maxSessions);
                    if (sessionsCount > 0)
                    {
                        // there is no need to wait for a session since we can create new ones
                        return new SessionOrCreationTokens(RegisterSessionCreations(sessionsCount));
                    }
                }
            }
            return new SessionOrCreationTokens(WaitForSession(connStr));
        }

        private List<SessionCreationToken> RegisterSessionCreationsToEnsureMinPoolSize()
        {
            var count = AllowedNumberOfNewSessionCreations(0);
            return RegisterSessionCreations(count);
        }

        private List<SessionCreationToken> RegisterSessionCreations(int sessionsCount) =>
            Enumerable.Range(1, sessionsCount)
                .Select(_ => _sessionCreationTokenCounter.NewToken())
                .ToList();

        private int AllowedNumberOfNewSessionCreations(int atLeastCount, int maxSessionsLimit = int.MaxValue)
        {
            // we are expecting to create atLeast 1 session in case of opening a connection (atLeastCount = 1)
            // but we have no expectations when closing a connection (atLeastCount = 0)
            if (!IsMultiplePoolsVersion())
            {
                if (atLeastCount > 0)
                    s_logger.Debug($"SessionPool - creating of new sessions is not limited");
                return atLeastCount; // we are either in old pool or there is no pooling
            }
            var currentSize = GetCurrentPoolSize();
            if (currentSize < _poolConfig.MaxPoolSize)
            {
                var maxSessionsToCreate = _poolConfig.MaxPoolSize - currentSize;
                var sessionsNeeded = Math.Max(_poolConfig.MinPoolSize - currentSize, atLeastCount);
                var sessionsToCreate = Math.Min(maxSessionsLimit, Math.Min(sessionsNeeded, maxSessionsToCreate));
                s_logger.Debug($"SessionPool - allowed to create {sessionsToCreate} sessions, current pool size is {currentSize} out of {_poolConfig.MaxPoolSize}" + PoolIdentification());
                return sessionsToCreate;
            }
            s_logger.Debug($"SessionPool - not allowed to create a session, current pool size is {currentSize} out of {_poolConfig.MaxPoolSize}" + PoolIdentification());
            return 0;
        }

        private bool IsMultiplePoolsVersion() => _waitingForIdleSessionQueue.IsWaitingEnabled();

        private SFSession WaitForSession(string connStr)
        {
            if (TimeoutHelper.IsInfinite(_poolConfig.WaitingForIdleSessionTimeout))
                throw new Exception("WaitingForIdleSessionTimeout cannot be infinite");
            s_logger.Info($"SessionPool::WaitForSession for {(long)_poolConfig.WaitingForIdleSessionTimeout.TotalMilliseconds} ms timeout" + PoolIdentification());
            _sessionPoolEventHandler.OnWaitingForSessionStarted(this);
            var beforeWaitingTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long nowTimeMillis = beforeWaitingTimeMillis;
            while (GetPooling() && !_underDestruction && !TimeoutHelper.IsExpired(beforeWaitingTimeMillis, nowTimeMillis, _poolConfig.WaitingForIdleSessionTimeout)) // we loop to handle the case if someone overtook us after being woken or session which we were promised has just expired
            {
                var timeoutLeftMillis = TimeoutHelper.FiniteTimeoutLeftMillis(beforeWaitingTimeMillis, nowTimeMillis, _poolConfig.WaitingForIdleSessionTimeout);
                _sessionPoolEventHandler.OnWaitingForSessionStarted(this, timeoutLeftMillis);
                var successful = _waitingForIdleSessionQueue.Wait((int)timeoutLeftMillis, CancellationToken.None);
                if (successful)
                {
                    s_logger.Debug($"SessionPool::WaitForSession - woken with a session granted" + PoolIdentification());
                    _sessionPoolEventHandler.OnWaitingForSessionSuccessful(this);
                    lock (_sessionPoolLock)
                    {
                        var session = ExtractIdleSession(connStr);
                        if (session != null)
                        {
                            s_logger.Debug("SessionPool::WaitForSession - provided an idle session" + PoolIdentification());
                            return session;
                        }
                    }
                }
                else
                {
                    s_logger.Debug("SessionPool::WaitForSession - woken without a session granted" + PoolIdentification());
                }
                nowTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }
            s_logger.Info("SessionPool::WaitForSession - could not find any idle session available withing a given timeout" + PoolIdentification());
            throw WaitingFailedException();
        }

        private static Exception WaitingFailedException() => new Exception("Could not obtain a connection from the pool within a given timeout");

        internal SFSession ExtractIdleSession(string connStr)
        {
            for (int i = 0; i < _idleSessions.Count; i++)
            {
                if (_idleSessions[i].ConnectionString.Equals(connStr))
                {
                    SFSession session = _idleSessions[i];
                    _idleSessions.RemoveAt(i);
                    var timeNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (session.IsExpired(_poolConfig.ExpirationTimeout, timeNow))
                    {
                        s_logger.Debug($"session {session.sessionId}{PoolIdentification()} has expired");
                        if (session.isHeartBeatEnabled)
                        {
                            s_logger.Debug($"keep alive enabled: renewing session {session.sessionId}{PoolIdentification()}");
                            try
                            {
                                session.renewSession();
                                _busySessionsCounter.Increase();
                                return session;
                            }
                            catch (Exception e)
                            {
                                s_logger.Error($"failed to renew session {session.sessionId}{PoolIdentification()} due to error: {e.Message}");
                                Task.Run(() => session.close());
                                i--;
                            }
                        }
                        else
                        {
                            s_logger.Debug($"keep alive disabled: closing the expired session {session.sessionId}{PoolIdentification()}");
                            Task.Run(() => session.close());
                            i--;
                        }
                    }
                    else
                    {
                        s_logger.Debug($"reuse pooled session with sid {session.sessionId}" + PoolIdentification());
                        _busySessionsCounter.Increase();
                        return session;
                    }
                }
            }
            return null;
        }

        private SFSession NewNonPoolingSession(String connectionString, SessionPropertiesContext sessionContext) =>
            NewSession(connectionString, sessionContext, _noPoolingSessionCreationTokenCounter.NewToken());

        private SFSession NewSession(String connectionString, SessionPropertiesContext sessionContext, SessionCreationToken sessionCreationToken)
        {
            s_logger.Debug("SessionPool::NewSession" + PoolIdentification());
            try
            {
                var session = s_sessionFactory.NewSession(connectionString, sessionContext);
                session.Open();
                s_logger.Debug("SessionPool::NewSession - opened" + PoolIdentification());
                if (GetPooling() && !_underDestruction)
                {
                    lock (_sessionPoolLock)
                    {
                        _sessionCreationTokenCounter.RemoveToken(sessionCreationToken);
                        _busySessionsCounter.Increase();
                        s_logger.Debug($"Pool state after creating a session {GetCurrentState()}" + PoolIdentification());
                    }
                }
                _sessionPoolEventHandler.OnNewSessionCreated(this);
                _sessionPoolEventHandler.OnSessionProvided(this);
                return session;
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                _sessionCreationTokenCounter.RemoveToken(sessionCreationToken);
                if (GetPooling())
                {
                    lock (_sessionPoolLock)
                    {
                        s_logger.Debug($"Failed to create a new session {GetCurrentState()}" + PoolIdentification());
                    }
                }
                if (e is SnowflakeDbException)
                    throw;
                throw new SnowflakeDbException(
                    e,
                    SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    SFError.INTERNAL_ERROR,
                    "Unable to connect. " + e.Message);
            }
        }

        private Task<SFSession> NewNonPoolingSessionAsync(
            String connectionString,
            SessionPropertiesContext sessionContext,
            CancellationToken cancellationToken) =>
            NewSessionAsync(connectionString, sessionContext, _noPoolingSessionCreationTokenCounter.NewToken(), cancellationToken);

        private Task<SFSession> NewSessionAsync(String connectionString, SessionPropertiesContext sessionContext, SessionCreationToken sessionCreationToken, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::NewSessionAsync" + PoolIdentification());
            var session = s_sessionFactory.NewSession(connectionString, sessionContext);
            return session
                .OpenAsync(cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted || previousTask.IsCanceled)
                    {
                        _sessionCreationTokenCounter.RemoveToken(sessionCreationToken);
                        if (GetPooling())
                        {
                            lock (_sessionPoolLock)
                            {
                                s_logger.Debug($"Failed to create a new session {GetCurrentState()}" + PoolIdentification());
                            }
                        }
                    }

                    if (previousTask.IsFaulted && previousTask.Exception != null)
                        throw previousTask.Exception;

                    if (previousTask.IsFaulted)
                        throw new SnowflakeDbException(
                            SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                            SFError.INTERNAL_ERROR,
                            "Failure while opening session async");

                    if (!previousTask.IsCanceled)
                    {
                        if (GetPooling() && !_underDestruction)
                        {
                            lock (_sessionPoolLock)
                            {
                                _sessionCreationTokenCounter.RemoveToken(sessionCreationToken);
                                _busySessionsCounter.Increase();
                                s_logger.Debug($"Pool state after creating a session {GetCurrentState()}" + PoolIdentification());
                            }
                        }

                        _sessionPoolEventHandler.OnNewSessionCreated(this);
                        _sessionPoolEventHandler.OnSessionProvided(this);
                    }
                    return session;
                }, TaskContinuationOptions.NotOnCanceled);
        }

        internal void ReleaseBusySession(SFSession session)
        {
            s_logger.Debug("SessionPool::ReleaseBusySession" + PoolIdentification());
            SessionPoolState poolState;
            lock (_sessionPoolLock)
            {
                _busySessionsCounter.Decrease();
                poolState = GetCurrentState();
            }
            s_logger.Debug($"After releasing a busy session from the pool {poolState}" + PoolIdentification());
        }

        internal bool AddSession(SFSession session, bool ensureMinPoolSize)
        {
            s_logger.Debug("SessionPool::AddSession" + PoolIdentification());

            if (!GetPooling() || _underDestruction)
                return false;

            var isMultiplePoolsVersion = IsMultiplePoolsVersion();
            if (isMultiplePoolsVersion &&
                session.SessionPropertiesChanged &&
                _poolConfig.ChangedSession == ChangedSessionBehavior.Destroy)
            {
                s_logger.Debug($"Session returning to pool was changed. Destroying the session: {session.sessionId}.");
                session.SetPooling(false);
            }

            if (!isMultiplePoolsVersion && !session.IsPoolingEnabledForConnectionCache())
            {
                s_logger.Debug($"Session not allowed to be used in connection cache. Destroying the session: {session.sessionId}.");
                session.SetPooling(false);
            }

            var sessionContext = new SessionPropertiesContext { Password = Password, OAuthClientSecret = OAuthClientSecret, Token = Token };
            if (!session.GetPooling())
            {
                ReleaseBusySession(session);
                if (ensureMinPoolSize)
                {
                    ScheduleNewIdleSessions(ConnectionString, sessionContext, RegisterSessionCreationsToEnsureMinPoolSize());
                }
                return false;
            }

            session.ClearQueryContextCache();
            var result = ReturnSessionToPool(session, ensureMinPoolSize);
            var wasSessionReturnedToPool = result.Item1;
            var sessionCreationTokens = result.Item2;
            ScheduleNewIdleSessions(ConnectionString, sessionContext, sessionCreationTokens);
            return wasSessionReturnedToPool;
        }

        private Tuple<bool, List<SessionCreationToken>> ReturnSessionToPool(SFSession session, bool ensureMinPoolSize)
        {
            long timeNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (session.IsNotOpen() || session.IsExpired(_poolConfig.ExpirationTimeout, timeNow))
            {
                lock (_sessionPoolLock)
                {
                    _busySessionsCounter.Decrease();
                    var sessionCreationTokens = ensureMinPoolSize
                        ? RegisterSessionCreationsToEnsureMinPoolSize()
                        : SessionOrCreationTokens.s_emptySessionCreationTokenList;
                    var poolState = GetCurrentState();
                    s_logger.Debug($"Could not return session to pool {poolState}" + PoolIdentification());
                    return Tuple.Create(false, sessionCreationTokens);
                }
            }

            lock (_sessionPoolLock)
            {
                _busySessionsCounter.Decrease();
                CleanExpiredSessions();
                if (session.IsExpired(_poolConfig.ExpirationTimeout, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())) // checking again because we could have spent some time waiting for a lock
                {
                    var sessionCreationTokens = ensureMinPoolSize
                        ? RegisterSessionCreationsToEnsureMinPoolSize()
                        : SessionOrCreationTokens.s_emptySessionCreationTokenList;
                    var poolState = GetCurrentState();
                    s_logger.Debug($"Could not return session to pool {poolState}" + PoolIdentification());
                    return Tuple.Create(false, sessionCreationTokens);
                }
                var poolStateBeforeReturningToPool = GetCurrentState();
                if (poolStateBeforeReturningToPool.Count() >= _poolConfig.MaxPoolSize)
                {
                    s_logger.Warn($"Pool is full - unable to add session with sid {session.sessionId} {poolStateBeforeReturningToPool}");
                    return Tuple.Create(false, SessionOrCreationTokens.s_emptySessionCreationTokenList);
                }
                _idleSessions.Add(session);
                _waitingForIdleSessionQueue.OnResourceIncrease();
                var sessionCreationTokensAfterReturningToPool = ensureMinPoolSize
                    ? RegisterSessionCreationsToEnsureMinPoolSize()
                    : SessionOrCreationTokens.s_emptySessionCreationTokenList;
                var poolStateAfterReturningToPool = GetCurrentState();
                s_logger.Debug($"returned session with sid {session.sessionId} to pool {poolStateAfterReturningToPool}" + PoolIdentification());
                return Tuple.Create(true, sessionCreationTokensAfterReturningToPool);
            }
        }

        internal void DestroyPool()
        {
            s_logger.Debug("SessionPool::DestroyPool" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                _underDestruction = true;
                ClearIdleSessions();
                _busySessionsCounter.Reset();
                _waitingForIdleSessionQueue.Reset();
                _sessionCreationTokenCounter.Reset();
            }
        }

        internal void DestroyPoolAsync()
        {
            s_logger.Debug("SessionPool::DestroyPoolAsync" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                _underDestruction = true;
                ClearIdleSessionsAsync();
                _busySessionsCounter.Reset();
                _waitingForIdleSessionQueue.Reset();
                _sessionCreationTokenCounter.Reset();
            }
        }

        internal void ClearSessions()
        {
            s_logger.Debug($"SessionPool::ClearSessions" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                _busySessionsCounter.Reset();
                ClearIdleSessions();
                _waitingForIdleSessionQueue.Reset();
            }
        }

        internal void ClearIdleSessions()
        {
            s_logger.Debug("SessionPool::ClearIdleSessions" + PoolIdentification());
            lock (_sessionPoolLock)
            {
                foreach (SFSession session in _idleSessions)
                {
                    session.close(); // it is left synchronously here because too much async tasks slows down testing
                }
                _idleSessions.Clear();
            }
        }

        internal async void ClearIdleSessionsAsync()
        {
            s_logger.Debug("SessionPool::ClearIdleSessionsAsync" + PoolIdentification());
            IEnumerable<SFSession> idleSessionsCopy;
            lock (_sessionPoolLock)
            {
                idleSessionsCopy = _idleSessions.Select(session => session);
                _idleSessions.Clear();
            }
            foreach (SFSession session in idleSessionsCopy)
            {
                await session.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        public void SetMaxPoolSize(int size)
        {
            s_logger.Debug($"SessionPool::SetMaxPoolSize({size})" + PoolIdentification());
            _poolConfig.MaxPoolSize = size;
            _configOverriden = true;
        }

        public int GetMaxPoolSize() => _poolConfig.MaxPoolSize;

        public int GetMinPoolSize()
        {
            return IsMultiplePoolsVersion()
                ? _poolConfig.MinPoolSize
                : throw s_notSupportedInCachePoolException;
        }

        public ChangedSessionBehavior GetChangedSession() =>
            IsMultiplePoolsVersion()
                ? _poolConfig.ChangedSession
                : throw s_notSupportedInCachePoolException;

        public long GetWaitForIdleSessionTimeout() =>
            IsMultiplePoolsVersion()
                ? (long)_poolConfig.WaitingForIdleSessionTimeout.TotalSeconds
                : throw s_notSupportedInCachePoolException;

        public long GetConnectionTimeout()
        {
            return TimeoutHelper.IsInfinite(_poolConfig.ConnectionTimeout) ? -1 : (long)_poolConfig.ConnectionTimeout.TotalSeconds;
        }

        public void SetTimeout(long seconds)
        {
            s_logger.Debug($"SessionPool::SetTimeout({seconds})" + PoolIdentification());
            var timeout = seconds < 0 ? TimeoutHelper.Infinity() : TimeSpan.FromSeconds(seconds);
            _poolConfig.ExpirationTimeout = timeout;
            _configOverriden = true;
        }

        public long GetTimeout()
        {
            return TimeoutHelper.IsInfinite(_poolConfig.ExpirationTimeout) ? -1 : (long)_poolConfig.ExpirationTimeout.TotalSeconds;
        }

        public int GetCurrentPoolSize()
        {
            return _idleSessions.Count + _busySessionsCounter.Count() + _sessionCreationTokenCounter.Count();
        }

        public SessionPoolState GetCurrentState()
        {
            return new SessionPoolState(
                _idleSessions.Count,
                _busySessionsCounter.Count(),
                _sessionCreationTokenCounter.Count(),
                _waitingForIdleSessionQueue.WaitingCount(),
                IsMultiplePoolsVersion()
            );
        }

        public bool SetPooling(bool isEnable)
        {
            s_logger.Info($"SessionPool::SetPooling({isEnable})" + PoolIdentification());
            if (_poolConfig.PoolingEnabled == isEnable)
                return false;
            _poolConfig.PoolingEnabled = isEnable;
            if (!_poolConfig.PoolingEnabled)
            {
                ClearSessions();
            }
            _configOverriden = true;
            return true;
        }

        public bool GetPooling() => _poolConfig.PoolingEnabled;

        internal int OngoingSessionCreationsCount()
        {
            lock (_sessionPoolLock)
            {
                return _sessionCreationTokenCounter.Count();
            }
        }

        internal List<long> GetIdleSessionsStartTimes()
        {
            lock (_sessionPoolLock)
            {
                return _idleSessions.Select(s => s.GetStartTime()).ToList();
            }
        }

        internal string PoolIdentification()
        {
            if (!IsMultiplePoolsVersion())
                return "";
            return
#if SF_PUBLIC_ENVIRONMENT
                PoolIdentificationBasedOnInternalId;
#else
                PoolIdentificationBasedOnConnectionString;
#endif
        }

        internal string PoolIdentificationBasedOnConnectionString => " [pool: " + _connectionStringWithoutSecrets + "]";

        internal string PoolIdentificationBasedOnInternalId => " [pool: " + _id + "]";
    }
}
