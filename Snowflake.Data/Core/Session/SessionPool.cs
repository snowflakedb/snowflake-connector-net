/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    sealed class SessionPool : IDisposable
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SessionPool>();
        private readonly object _sessionPoolLock = new object();
        private static ISessionFactory s_sessionFactory = new SessionFactory();

        private readonly List<SFSession> _idleSessions;
        private readonly IWaitingQueue _waitingQueue;
        private readonly ICreateSessionTokens _createSessionTokens;
        private readonly ICreateSessionTokens _noPoolingCreateSessionTokens = new NotCountingCreateSessionTokens();
        private int _maxPoolSize;
        private long _timeout;
        private const int MaxPoolSize = 10;
        private const long Timeout = 3600;
        internal string ConnectionString { get; }
        internal SecureString Password { get; }
        private bool _pooling = true;
        private readonly ICounter _busySessionsCounter;
        private ISessionPoolEventHandler _sessionPoolEventHandler = new SessionPoolEventHandler(); // a way to inject some additional behaviour after certain events. Can be used for example to measure time of given steps.

        private SessionPool()
        {
            // acquiring a lock not needed because one is already acquired in SnowflakeDbConnectionPool
            _idleSessions = new List<SFSession>();
            _maxPoolSize = MaxPoolSize;
            _timeout = Timeout;
            _busySessionsCounter = new FixedZeroCounter();
            _waitingQueue = new NoOneWaiting();
            _createSessionTokens = new NotCountingCreateSessionTokens();
        }

        private SessionPool(string connectionString, SecureString password)
        {
            // acquiring a lock not needed because one is already acquired in ConnectionPoolManager 
            _idleSessions = new List<SFSession>();
            _maxPoolSize = MaxPoolSize;
            _timeout = Timeout;
            _busySessionsCounter = new NonNegativeCounter();
            ConnectionString = connectionString;
            Password = password;
            _waitingQueue = new SemaphoreBasedQueue();
            _createSessionTokens = new CreateSessionTokens();
        }

        internal static SessionPool CreateSessionCache() => new SessionPool();

        internal static SessionPool CreateSessionPool(string connectionString, SecureString password) =>
            new SessionPool(connectionString, password);
        
        ~SessionPool()
        {
            // Use async for the finalizer due to possible deadlock
            // when waiting for the CloseResponse task while closing the session
            ClearAllPoolsAsync();
        }

        public void Dispose()
        {
            ClearIdleSessions();
        }

        internal static ISessionFactory SessionFactory
        {
            set => s_sessionFactory = value;
        }

        private void CleanExpiredSessions()
        {
            s_logger.Debug("SessionPool::CleanExpiredSessions");
            lock (_sessionPoolLock)
            {
                long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                foreach (var item in _idleSessions.ToList())
                {
                    if (item.IsExpired(_timeout, timeNow))
                    {
                        _idleSessions.Remove(item);
                        _waitingQueue.OnResourceDecrease();
                        item.close();
                    }
                }
            }
        }

        internal SFSession GetSession(string connStr, SecureString password)
        {
            s_logger.Debug("SessionPool::GetSession");
            if (!_pooling)
                return NewSession(connStr, password, _noPoolingCreateSessionTokens.BeginCreate());
            var sessionOrCreateToken = GetIdleSession(connStr);
            if (sessionOrCreateToken.Session != null)
            {
                _sessionPoolEventHandler.OnSessionProvided(this);
            }
            return sessionOrCreateToken.Session ?? NewSession(connStr, password, sessionOrCreateToken.CreateToken);
        }
        
        internal async Task<SFSession> GetSessionAsync(string connStr, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::GetSessionAsync");
            if (!_pooling)
                return await NewSessionAsync(connStr, password, _noPoolingCreateSessionTokens.BeginCreate(), cancellationToken).ConfigureAwait(false);
            var sessionOrCreateToken = GetIdleSession(connStr);
            if (sessionOrCreateToken.Session != null)
            {
                _sessionPoolEventHandler.OnSessionProvided(this);
            }
            return sessionOrCreateToken.Session ?? await NewSessionAsync(connStr, password, sessionOrCreateToken.CreateToken, cancellationToken).ConfigureAwait(false);
        }

        internal SFSession GetSession() => GetSession(ConnectionString, Password);

        internal Task<SFSession> GetSessionAsync(CancellationToken cancellationToken) =>
            GetSessionAsync(ConnectionString, Password, cancellationToken);

        internal void SetSessionPoolEventHandler(ISessionPoolEventHandler sessionPoolEventHandler)
        {
            _sessionPoolEventHandler = sessionPoolEventHandler;
        }
        
        private SessionOrCreateToken GetIdleSession(string connStr)
        {
            s_logger.Debug("SessionPool::GetIdleSession");
            lock (_sessionPoolLock)
            {
                if (_waitingQueue.IsAnyoneWaiting())
                {
                    s_logger.Debug("SessionPool::GetIdleSession - someone is already waiting for a session, request is going to be queued");
                }
                else
                {
                    var session = ExtractIdleSession(connStr);
                    if (session != null)
                    {
                        s_logger.Debug("SessionPool::GetIdleSession - no one was waiting for a session, a session was extracted from idle sessions");
                        _waitingQueue.OnResourceDecrease();
                        return new SessionOrCreateToken(session);
                    }
                    s_logger.Debug("SessionPool::GetIdleSession - no one was waiting for session, but could not find any idle session available");
                    if (IsAllowedToCreateNewSession())
                    {
                        // there is no need to wait for a session since we can create a new one
                        return new SessionOrCreateToken(_createSessionTokens.BeginCreate());
                    }
                }
            }
            return new SessionOrCreateToken(WaitForSession(connStr));
        }

        private bool IsAllowedToCreateNewSession()
        {
            if (!_waitingQueue.IsWaitingEnabled())
            {
                s_logger.Debug($"SessionPool - new session creation granted");
                return true;
            }
            var currentSize = GetCurrentPoolSize();
            if (currentSize < _maxPoolSize)
            {
                s_logger.Debug($"SessionPool - new session creation granted because current size is {currentSize} out of {_maxPoolSize}");
                return true;
            }
            s_logger.Debug($"SessionPool - could not grant new session creation because current size is {currentSize} out of {_maxPoolSize}");
            return false;
        }
        
        private SFSession WaitForSession(string connStr)
        {
            var timeout = _waitingQueue.GetWaitingTimeoutMillis();
            s_logger.Warn($"SessionPool::WaitForSession for {timeout} millis timeout");
            _sessionPoolEventHandler.OnWaitingForSessionStarted(this);
            var beforeWaitingTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            string debugApplicationName = "";
            if (connStr.EndsWith("application=TestWaitForMaxSize1"))
                debugApplicationName = "TestWaitForMaxSize1";
            if (connStr.EndsWith("application=TestWaitForMaxSize2"))
                debugApplicationName = "TestWaitForMaxSize2";
            if (connStr.EndsWith("application=TestWaitForMaxSize3"))
                debugApplicationName = "TestWaitForMaxSize3";
            s_logger.Warn($"Wait for session started at: {beforeWaitingTime} for: {debugApplicationName}");
            long nowTime = beforeWaitingTime;
            while (nowTime < beforeWaitingTime + timeout) // we loop to handle the case if someone overtook us after being woken or session which we were promised has just expired
            {
                var timeoutLeft = beforeWaitingTime + timeout - nowTime;
                var successful = _waitingQueue.Wait((int) timeoutLeft, CancellationToken.None);
                if (successful)
                {
                    s_logger.Warn($"SessionPool::WaitForSession - woken with a session granted for: {debugApplicationName} at: {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}");
                    lock (_sessionPoolLock)
                    {
                        var session = ExtractIdleSession(connStr);
                        if (session != null)
                        {
                            s_logger.Warn($"SessionPool::WaitForSession - a session was extracted from idle sessions for: {debugApplicationName}");
                            return session;
                        }
                    }
                }
                else 
                {
                    s_logger.Warn($"SessionPool::WaitForSession - woken without a session granted for: {debugApplicationName}");
                }
                nowTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }
            s_logger.Warn($"SessionPool::WaitForSession - could not find any idle session available withing a given timeout for: {debugApplicationName} at: {nowTime}");
            throw WaitingFailedException();
        }

        private static Exception WaitingFailedException() => new Exception("Could not obtain a connection from the pool within a given timeout");

        private SFSession ExtractIdleSession(string connStr)
        {
            for (int i = 0; i < _idleSessions.Count; i++)
            {
                if (_idleSessions[i].ConnectionString.Equals(connStr))
                {
                    SFSession session = _idleSessions[i];
                    _idleSessions.RemoveAt(i); // we don't do _waitingQueue.OnResourceDecrease() here because it happens in GetIdleSession()
                    long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    if (session.IsExpired(_timeout, timeNow))
                    {
                        session.close(); // TODO: change to session required
                        _waitingQueue.OnResourceDecrease();
                        i--;
                    }
                    else
                    {
                        s_logger.Debug($"reuse pooled session with sid {session.sessionId}");
                        _busySessionsCounter.Increase();
                        return session;
                    }
                }
            }
            return null;
        }

        private SFSession NewSession(String connectionString, SecureString password, CreateSessionToken createSessionToken)
        {
            s_logger.Warn("SessionPool::NewSession"); // TODO: change to debug !!!
            try
            {
                var session = s_sessionFactory.NewSession(connectionString, password);
                session.Open();
                s_logger.Warn("SessionPool::NewSession - opened"); // TODO: change to debug !!!
                if (_pooling)
                {
                    lock (_sessionPoolLock)
                    {
                        _createSessionTokens.EndCreate(createSessionToken);
                        _busySessionsCounter.Increase();
                    }
                }
                _sessionPoolEventHandler.OnNewSessionCreated(this);
                _sessionPoolEventHandler.OnSessionProvided(this);
                s_logger.Warn($"SessionPool::NewSession - finish: {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}"); // TODO: remove !!!
                return session;
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                _createSessionTokens.EndCreate(createSessionToken);
                if (e is SnowflakeDbException)
                    throw;
                throw new SnowflakeDbException(
                    e,
                    SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    SFError.INTERNAL_ERROR,
                    "Unable to connect. " + e.Message);
            }
        }

        private Task<SFSession> NewSessionAsync(String connectionString, SecureString password, CreateSessionToken createSessionToken, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::NewSessionAsync");
            var session = s_sessionFactory.NewSession(connectionString, password);
            return session
                .OpenAsync(cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                        _createSessionTokens.EndCreate(createSessionToken);
                    }
                        
                    if (previousTask.IsFaulted && previousTask.Exception != null)
                        throw previousTask.Exception;

                    if (previousTask.IsFaulted)
                        throw new SnowflakeDbException(
                            SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                            SFError.INTERNAL_ERROR,
                            "Failure while opening session async");

                    if (_pooling)
                    {
                        lock (_sessionPoolLock)
                        {
                            _createSessionTokens.EndCreate(createSessionToken);
                            _busySessionsCounter.Increase();
                        }
                    }
                    _sessionPoolEventHandler.OnNewSessionCreated(this);
                    _sessionPoolEventHandler.OnSessionProvided(this);
                    return session;
                }, TaskContinuationOptions.None); // previously it was NotOnCanceled but we would like to execute it even in case of cancellation to properly update counters 
        }

        internal bool AddSession(SFSession session)
        {
            s_logger.Debug("SessionPool::AddSession");
            if (!_pooling)
                return false;
            long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (session.IsNotOpen() || session.IsExpired(_timeout, timeNow))
            {
                lock (_sessionPoolLock)
                {
                    _busySessionsCounter.Decrease();
                }
                return false;
            }

            lock (_sessionPoolLock)
            {
                _busySessionsCounter.Decrease();
                CleanExpiredSessions();
                if (GetCurrentPoolSize() >= _maxPoolSize)
                {
                    s_logger.Warn($"Pool is full - unable to add session with sid {session.sessionId}");
                    return false;
                }

                s_logger.Debug($"pool connection with sid {session.sessionId}");
                _idleSessions.Add(session);
                _waitingQueue.OnResourceIncrease();
                return true;
            }
        }

        internal void ClearIdleSessions()
        {
            s_logger.Debug("SessionPool::ClearIdleSessions");
            lock (_sessionPoolLock)
            {
                foreach (SFSession session in _idleSessions)
                {
                    session.close();
                }
                _idleSessions.ForEach(session => _waitingQueue.OnResourceDecrease());
                _idleSessions.Clear();
            }
        }

        internal async void ClearAllPoolsAsync()
        {
            s_logger.Debug("SessionPool::ClearAllPoolsAsync");
            IEnumerable<SFSession> idleSessionsCopy;
            lock (_sessionPoolLock)
            {
                idleSessionsCopy = _idleSessions.Select(session => session);
                _idleSessions.ForEach(session => _waitingQueue.OnResourceDecrease());
                _idleSessions.Clear();
            }
            foreach (SFSession session in idleSessionsCopy)
            {
                await session.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }

        public void SetMaxPoolSize(int size)
        {
            _maxPoolSize = size;
        }

        public int GetMaxPoolSize()
        {
            return _maxPoolSize;
        }

        public void SetTimeout(long time)
        {
            _timeout = time;
        }

        public long GetTimeout()
        {
            return _timeout;
        }

        public int GetCurrentPoolSize()
        {
            return _idleSessions.Count + _busySessionsCounter.Count() + _createSessionTokens.Count();
        }

        public bool SetPooling(bool isEnable)
        {
            s_logger.Info($"SessionPool::SetPooling({isEnable})");
            if (_pooling == isEnable)
                return false;
            _pooling = isEnable;
            if (!_pooling)
            {
                ClearIdleSessions();
            }
            return true;
        }

        public bool GetPooling()
        {
            return _pooling;
        }

        public void SetWaitingTimeout(long timeoutMillis) => _waitingQueue.SetWaitingTimeout(timeoutMillis);
    }
}
