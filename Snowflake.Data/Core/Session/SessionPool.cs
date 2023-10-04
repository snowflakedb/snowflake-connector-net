using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    public enum SessionPickAlgorithm
    {
        MatchConnectionString,
        PickOldest
    }
    
    public class SessionPool : IDisposable
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SessionPool>();
        private static readonly object s_sessionPoolLock = new object();

        // private const int MinPoolSize = 0; // TODO: 
        private const int MaxPoolSize = 10;
        private const long Timeout = 3600;
        private const long OpenSessionTimeout = 60 * 2;
        
        private readonly List<SFSession> _sessionPool;
        private int _busySessions; 
        // private int _minPoolSize; // TODO: 
        private int _maxPoolSize;
        private long _timeout;
        private long _openSessionTimeout;
        private int _openSessionSleepTimeout = 250;
        private bool _pooling = true; // TODO: Get rid of that!
        private string _connectionString;
        private SecureString _password;
        private bool _allowExceedMaxPoolSize; // backward compatibility flag
        private SessionPickAlgorithm _sessionPick; // backward compatibility flag

        internal SessionPool(string connectionString, SecureString password)
        {
            lock (s_sessionPoolLock)
            {
                _sessionPool = new List<SFSession>();
                _busySessions = 0;
                _maxPoolSize = MaxPoolSize;
                _timeout = Timeout;
                // _minPoolSize = MinPoolSize; // TODO:
                _openSessionTimeout = OpenSessionTimeout;
                _connectionString = connectionString;
                _password = password;
            }
        }

        ~SessionPool()
        {
            ClearAllPools();
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

        internal SFSession GetSession(string connectionString, SecureString password)
        {
            if (!_pooling)
                return OpenNewSession(connectionString, password);
            CleanExpiredSessions();
            // EnsureMinPoolSizeAsync();
            return ProvidePooledSession(connectionString, password);
        }

        internal Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            if (!_pooling)
                return OpenNewSessionAsync(connectionString, password, cancellationToken);
            CleanExpiredSessions();
            // EnsureMinPoolSizeAsync();
            return ProvidePooledSessionAsync(connectionString, password, cancellationToken);
        }
        
        private SFSession ProvidePooledSession(string connectionString, SecureString password)
        {
            if (!_pooling)
                return null;

            lock (s_sessionPoolLock)
            {
                if (GetIdleSessionsSize() > 0)
                {
                    var sessionFromPool = PickSession(connectionString); // oldest idle // TODO: 
                    if (sessionFromPool != null)
                    {
                        _sessionPool.Remove(sessionFromPool);
                        _busySessions++;
                        s_logger.Debug($"Reused pooled session with sid {sessionFromPool.sessionId}");
                        return sessionFromPool;
                    }
                }

                if (GetCurrentPoolSize() < MaxPoolSize)
                {
                    var newSession = OpenNewSession(connectionString, password);
                    _busySessions++;
                    s_logger.Info($"Created new pooled session with sid {newSession.sessionId}");
                    return newSession;
                }
                s_logger.Debug($"Pool size {_maxPoolSize} reached, no free idle connections");
            }

            if (_allowExceedMaxPoolSize) // backward compatibility
            {
                s_logger.Warn($"Exceeding Max Pool Size enabled, providing new session");
                return OpenNewSession(connectionString, password);
            }

            return WaitForPooledSession(CancellationToken.None);
        }
        
        private Task<SFSession> ProvidePooledSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            if (!_pooling)
                return null;

            lock (s_sessionPoolLock)
            {
                if (GetIdleSessionsSize() > 0)
                {
                    var sessionFromPool = PickSession(connectionString);
                    if (sessionFromPool != null)
                    {
                        _sessionPool.Remove(sessionFromPool);
                        _busySessions++;
                        s_logger.Debug($"Reused pooled session with sid {sessionFromPool.sessionId}");
                        return Task.FromResult(sessionFromPool);
                    }
                }

                if (GetCurrentPoolSize() < MaxPoolSize)
                {
                    var session = OpenNewSessionAsync(connectionString, password, cancellationToken);
                    _busySessions++;
                    s_logger.Info($"Creating new pooled session");
                    return session;
                }

                s_logger.Debug($"Pool size {_maxPoolSize} reached, no free idle connections");
            }

            if (_allowExceedMaxPoolSize) // backward compatibility
            {
                s_logger.Warn($"Exceeding Max Pool Size enabled, providing new session");
                return OpenNewSessionAsync(connectionString, password, cancellationToken);
            }

            return Task.Run(()=>WaitForPooledSession(cancellationToken));
        }

        private SFSession WaitForPooledSession(CancellationToken cancellationToken)
        {
            if (!_pooling)
                return null;

            SFSession session = null;
            long start = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            while (session == null)
            {
                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                lock (s_sessionPoolLock)
                {
                    session = PickSession(_connectionString);
                    if (session != null)
                    {
                        _sessionPool.Remove(session);
                        _busySessions++;
                        return session;
                    }
                }

                Thread.Sleep(_openSessionSleepTimeout);
                long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                if (start + _openSessionTimeout < now)
                    throw new SnowflakeDbException(
                        new TimeoutException("No free connections in the pool."), // TODO:
                        SnowflakeDbException.CONNECTION_FAILURE_SSTATE,
                        SFError.INTERNAL_ERROR, 
                        "Unable to connect.");
            }

            return session;
        }

        // TODO: inject 
        SFSession PickSession(string connectionString)
        {
            SFSession session = null;
            lock (s_sessionPoolLock)
            {
                switch (_sessionPick)
                {
                    case SessionPickAlgorithm.MatchConnectionString:
                        session =  _sessionPool.FirstOrDefault(it => it.connStr.Equals(connectionString));
                        break;
                    case SessionPickAlgorithm.PickOldest:
                        session = _sessionPool.Any() ? _sessionPool[0] : null;
                        break;
                }
                if (session != null)
                    _sessionPool.Remove(session);
            }

            return session;
        }

        // TODO:
        // private void EnsureMinPoolSize()
        // {
        //     if (!_pooling || _minPoolSize == 0)
        //         return;
        //     
        //     lock (s_sessionPoolLock)
        //     {
        //         s_logger.Debug($"Filling up connection pool to {_minPoolSize}");
        //
        //         while (GetCurrentPoolSize() < _minPoolSize)
        //         {
        //             var newSession = OpenNewSession();
        //             _sessionPool.Add(newSession);
        //         }
        //     }
        // }

        internal SFSession OpenNewSession(string connectionString, SecureString password)
        {
            SFSession session = new SFSession(connectionString, password);
            try
            {
                session.Open(); 
                s_logger.Debug($"session opened {session.sessionId}");
            }
            catch (Exception e)
            {
                // Otherwise when Dispose() is called, the close request would timeout.
                // _connectionState = ConnectionState.Closed; // TODO:
                s_logger.Error("Unable to connect", e);
                if (!(e is SnowflakeDbException))
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
            return session;
        }

        internal Task<SFSession> OpenNewSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            SFSession session = new SFSession(connectionString, password);
            return session
                .OpenAsync(cancellationToken)
                .ContinueWith(previousTask =>
                {
                    if (previousTask.IsFaulted)
                    {
                        Debug.Assert(previousTask.Exception != null, "previousTask.Exception != null");
                        throw previousTask.Exception;
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    
                    return session;
                }, cancellationToken);
        }

        internal bool AddSession(SFSession session)
        {
            s_logger.Debug("SessionPool::AddSession");
            if (!_pooling)
                return false;
            long timeNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (session.IsNotOpen() || session.IsExpired(_timeout, timeNow))
            {
                s_logger.Warn($"Session returning to the pool in an undesired state: {session.sessionId}");
                // TODO: fix because it is counted in the pool 
                // TODO: lock
                if (_busySessions > 0)
                    _busySessions--; 
                return false; 
            }

            if (_sessionPool.Count >= _maxPoolSize)
                CleanExpiredSessions();
            
            lock (s_sessionPoolLock)
            {
                if (_sessionPool.Count >= _maxPoolSize)
                {
                    s_logger.Warn($"Pool is full, cannot add session with sid {session.sessionId}"); 
                    return false;
                }

                if (_busySessions > 0)
                    _busySessions--;
                s_logger.Debug($"Connection returned to the pool with sid {session.sessionId}");
                _sessionPool.Add(session);
                return true;
                
                // s_logger.Warn($"Unexpected session with sid {session.sessionId} was not returned to the pool"); // or clear pool was called and session was created before
                return false;
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
                _busySessions = 0; // TODO: check test TestConnectionPoolIsFull
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
            switch (SnowflakeDbConnectionPool.GetVersion())
            {
                case PoolManagerVersion.Version1: return _sessionPool.Count;
                case PoolManagerVersion.Version2: return _sessionPool.Count + _busySessions;
            }
            throw new NotSupportedException("Unknown pool version");

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
        
        private int GetIdleSessionsSize()
        {
            return _sessionPool.Count;
        }
        
        public bool GetAllowExceedMaxPoolSize()
        {
            return _allowExceedMaxPoolSize;
        }

        public void SetAllowExceedMaxPoolSize(bool allowExceedMaxPoolSize)
        {
            _allowExceedMaxPoolSize = allowExceedMaxPoolSize;
        }

        public void SetSessionPickAlgorithm(SessionPickAlgorithm sessionPicking)
        {
            _sessionPick = sessionPicking;
        }
    }

}