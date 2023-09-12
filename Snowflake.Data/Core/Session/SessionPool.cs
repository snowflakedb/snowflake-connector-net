using System;
using System.Collections.Generic;
using System.Linq;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    sealed class SessionPoolSingleton : IDisposable
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SessionPoolSingleton>();
        private static SessionPoolSingleton s_instance = null;
        private static readonly object s_sessionPoolLock = new object();

        private readonly List<SFSession> _sessionPool;
        private int _maxPoolSize;
        private long _timeout;
        private const int MaxPoolSize = 10;
        private const long Timeout = 3600;
        private bool _pooling = true;

        SessionPoolSingleton()
        {
            lock (s_sessionPoolLock)
            {
                _sessionPool = new List<SFSession>();
                _maxPoolSize = MaxPoolSize;
                _timeout = Timeout;
            }
        }
        ~SessionPoolSingleton()
        {
            ClearAllPools();
        }

        public void Dispose()
        {
            ClearAllPools();
        }

        public static SessionPoolSingleton Instance
        {
            get
            {
                lock (s_sessionPoolLock)
                {
                    if(s_instance == null)
                    {
                        s_instance = new SessionPoolSingleton();
                    }
                    return s_instance;
                }
            }
        }

        private void CleanExpiredSessions()
        {
            s_logger.Debug("SessionPool::cleanExpiredSessions");
            lock (s_sessionPoolLock)
            {
                long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                foreach (var item in _sessionPool.ToList())
                {
                    if (item.IsExpired(_timeout, timeNow))
                    {
                        _sessionPool.Remove(item);
                        item.close();
                    }
                }
            }
        }

        internal SFSession GetSession(string connStr)
        {
            s_logger.Debug("SessionPool::GetSession");
            if (!_pooling)
                return null;
            lock (s_sessionPoolLock)
            {
                for (int i = 0; i < _sessionPool.Count; i++)
                {
                    if (_sessionPool[i].connStr.Equals(connStr))
                    {
                        SFSession session = _sessionPool[i];
                        _sessionPool.RemoveAt(i);
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
                if (_sessionPool.Count >= _maxPoolSize)
                {
                    CleanExpiredSessions();
                }
                if (_sessionPool.Count >= _maxPoolSize)
                {
                    // pool is full
                    return false;
                }

                s_logger.Debug($"pool connection with sid {session.sessionId}");
                _sessionPool.Add(session);
                return true;
            }
        }

        internal void ClearAllPools()
        {
            s_logger.Debug("SessionPool::ClearAllPools");
            lock (s_sessionPoolLock)
            {
                foreach (SFSession session in _sessionPool)
                {
                    session.close();
                }
                _sessionPool.Clear();
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
            return _sessionPool.Count;
        }

        public bool SetPooling(bool isEnable)
        {
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