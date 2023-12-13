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
        private readonly List<SFSession> _sessions;
        private int _maxPoolSize;
        private long _timeout;
        private const int MaxPoolSize = 10;
        private const long Timeout = 3600;
        private bool _pooling = true;

        internal SessionPool()
        {
            lock (s_sessionPoolLock)
            {
                _sessions = new List<SFSession>();
                _maxPoolSize = MaxPoolSize;
                _timeout = Timeout;
            }
        }
        
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

        private void CleanExpiredSessions()
        {
            s_logger.Debug("SessionPool::CleanExpiredSessions");
            lock (s_sessionPoolLock)
            {
                long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                foreach (var item in _sessions.ToList())
                {
                    if (item.IsExpired(_timeout, timeNow))
                    {
                        Task.Run(() => item.close());
                        _sessions.Remove(item);
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

        private SFSession GetIdleSession(string connStr)
        {
            s_logger.Debug("SessionPool::GetIdleSession");
            lock (s_sessionPoolLock)
            {
                for (int i = 0; i < _sessions.Count; i++)
                {
                    if (_sessions[i].connStr.Equals(connStr))
                    {
                        SFSession session = _sessions[i];
                        _sessions.RemoveAt(i);
                        long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        if (session.IsExpired(_timeout, timeNow))
                        {
                            Task.Run(() => session.close());
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
                var session = new SFSession(connectionString, password);
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
            var session = new SFSession(connectionString, password);
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
                if (_sessions.Count >= _maxPoolSize)
                {
                    CleanExpiredSessions();
                }
                if (_sessions.Count >= _maxPoolSize)
                {
                    // pool is full
                    return false;
                }

                s_logger.Debug($"pool connection with sid {session.sessionId}");
                _sessions.Add(session);
                return true;
            }
        }

        internal void ClearAllPools()
        {
            s_logger.Debug("SessionPool::ClearAllPools");
            lock (s_sessionPoolLock)
            {
                foreach (SFSession session in _sessions)
                {
                    session.close(); // it is left synchronously here because too much async tasks slows down testing
                }
                _sessions.Clear();
            }
        }

        internal async void ClearAllPoolsAsync()
        {
            s_logger.Debug("SessionPool::ClearAllPoolsAsync");
            foreach (SFSession session in _sessions)
            {
                await session.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            }
            _sessions.Clear();
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
            return _sessions.Count;
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
