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
        private static readonly object s_sessionPoolLock = new object();
        private static ISessionFactory s_sessionFactory = new SessionFactory();

        private readonly List<SFSession> _idleSessions;
        private int _maxPoolSize;
        private long _timeout;
        private const int MaxPoolSize = 10;
        private const long Timeout = 3600;
        internal string ConnectionString { get; }
        internal SecureString Password { get; }
        private bool _pooling = true;
        private bool _allowExceedMaxPoolSize = true;

        private SessionPool()
        {
            lock (s_sessionPoolLock)
            {
                _idleSessions = new List<SFSession>();
                _maxPoolSize = MaxPoolSize;
                _timeout = Timeout;
            }
        }

        private SessionPool(string connectionString, SecureString password) : this()
        {
            ConnectionString = connectionString;
            Password = password;
            _allowExceedMaxPoolSize = false; // TODO: SNOW-937190
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
            ClearAllPools();
        }

        internal static ISessionFactory SessionFactory
        {
            set => s_sessionFactory = value;
        }

        private void CleanExpiredSessions()
        {
            s_logger.Debug("SessionPool::CleanExpiredSessions");
            lock (s_sessionPoolLock)
            {
                long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                foreach (var item in _idleSessions.ToList())
                {
                    if (item.IsExpired(_timeout, timeNow))
                    {
                        _idleSessions.Remove(item);
                        item.close();
                    }
                }
            }
        }

        internal SFSession GetSession(string connStr, SecureString password)
        {
            s_logger.Debug("SessionPool::GetSession");
            if (!_pooling)
                return NewSession(connStr, password);
            SFSession session = GetIdleSession(connStr);
            return session ?? NewSession(connStr, password);
        }
        
        internal Task<SFSession> GetSessionAsync(string connStr, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::GetSessionAsync");
            if (!_pooling)
                return NewSessionAsync(connStr, password, cancellationToken);
            SFSession session = GetIdleSession(connStr);
            return session != null ? Task.FromResult(session) : NewSessionAsync(connStr, password, cancellationToken);
        }

        internal SFSession GetSession() => GetSession(ConnectionString, Password);

        internal Task<SFSession> GetSessionAsync(CancellationToken cancellationToken) =>
            GetSessionAsync(ConnectionString, Password, cancellationToken);

        private SFSession GetIdleSession(string connStr)
        {
            s_logger.Debug("SessionPool::GetIdleSession");
            lock (s_sessionPoolLock)
            {
                for (int i = 0; i < _idleSessions.Count; i++)
                {
                    if (_idleSessions[i].ConnectionString.Equals(connStr))
                    {
                        SFSession session = _idleSessions[i];
                        _idleSessions.RemoveAt(i);
                        long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        if (session.IsExpired(_timeout, timeNow))
                        {
                            session.close();
                            i--;
                        }
                        else
                        {
                            s_logger.Debug($"reuse pooled session with sid {session.sessionId}");
                            return session;
                        }
                    }
                }
            }
            return null;
        }

        private SFSession NewSession(String connectionString, SecureString password)
        {
            s_logger.Debug("SessionPool::NewSession");
            try
            {
                var session = s_sessionFactory.NewSession(connectionString, password);
                session.Open();
                return session;
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                if (e is SnowflakeDbException)
                    throw;
                throw new SnowflakeDbException(
                    e,
                    SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                    SFError.INTERNAL_ERROR,
                    "Unable to connect. " + e.Message);
            }
        }

        private Task<SFSession> NewSessionAsync(String connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug("SessionPool::NewSessionAsync");
            var session = s_sessionFactory.NewSession(connectionString, password);
            return session
                .OpenAsync(cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted && previousTask.Exception != null)
                        throw previousTask.Exception;

                    if (previousTask.IsFaulted)
                        throw new SnowflakeDbException(
                            SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                            SFError.INTERNAL_ERROR,
                            "Failure while opening session async");

                    return session;
                }, TaskContinuationOptions.NotOnCanceled);
        }

        internal bool AddSession(SFSession session)
        {
            s_logger.Debug("SessionPool::AddSession");
            if (!_pooling)
                return false;
            long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (session.IsNotOpen() || session.IsExpired(_timeout, timeNow))
                return false;

            lock (s_sessionPoolLock)
            {
                if (_idleSessions.Count >= _maxPoolSize)
                {
                    CleanExpiredSessions();
                }
                if (_idleSessions.Count >= _maxPoolSize)
                {
                    s_logger.Warn($"Pool is full - unable to add session with sid {session.sessionId}");
                    return false;
                }

                s_logger.Debug($"pool connection with sid {session.sessionId}");
                _idleSessions.Add(session);
                return true;
            }
        }

        internal void ClearAllPools()
        {
            s_logger.Debug("SessionPool::ClearAllPools");
            lock (s_sessionPoolLock)
            {
                foreach (SFSession session in _idleSessions)
                {
                    session.close();
                }
                _idleSessions.Clear();
            }
        }

        internal async void ClearAllPoolsAsync()
        {
            s_logger.Debug("SessionPool::ClearAllPoolsAsync");
            foreach (SFSession session in _idleSessions)
            {
                await session.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
            _idleSessions.Clear();
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
            return _idleSessions.Count;
        }

        public bool SetPooling(bool isEnable)
        {
            s_logger.Info($"SessionPool::SetPooling({isEnable})");
            if (_pooling == isEnable)
                return false;
            _pooling = isEnable;
            if (!_pooling)
            {
                ClearAllPools();
            }
            return true;
        }

        public bool GetPooling()
        {
            return _pooling;
        }
    }
}
