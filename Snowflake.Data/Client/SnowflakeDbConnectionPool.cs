﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Security;
using Snowflake.Data.Log;
using System.Linq;
using Snowflake.Data.Core;

namespace Snowflake.Data.Client
{
    sealed class SessionPoolSingleton : IDisposable
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SessionPoolSingleton>();
        private static SessionPoolSingleton instance = null;
        private static readonly object _sessionPoolLock = new object();

        private List<SFSession> sessionPool;
        private int maxPoolSize;
        private long timeout;
        private int MAX_POOL_SIZE = 10;
        private const long TIMEOUT = 3600;
        private bool pooling = true;

        SessionPoolSingleton()
        {
            lock (_sessionPoolLock)
            {
                sessionPool = new List<SFSession>();
                maxPoolSize = MAX_POOL_SIZE;
                timeout = TIMEOUT;
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
                lock (_sessionPoolLock)
                {
                    if(instance == null)
                    {
                        instance = new SessionPoolSingleton();
                    }
                    return instance;
                }
            }
        }

        private void cleanExpiredSessions()
        {
            logger.Debug("SessionPool::cleanExpiredSessions");
            lock (_sessionPoolLock)
            {
                long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();

                foreach (var item in sessionPool.ToList())
                {
                    if ((item.startTime + timeout) <= timeNow)
                    {
                        sessionPool.Remove(item);
                        item.close();
                    }
                }
            }
        }

        internal SFSession getSession(string connStr)
        {
            logger.Debug("SessionPool::getSession");
            if (!pooling)
                return null;
            lock (_sessionPoolLock)
            {
                for (int i = 0; i < sessionPool.Count; i++)
                {
                    if (sessionPool[i].connStr.Equals(connStr))
                    {
                        SFSession session = sessionPool[i];
                        sessionPool.RemoveAt(i);
                        long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();
                        if ((session.startTime + timeout) <= timeNow)
                        {
                            session.close();
                            i--;
                        }
                        else
                        {
                            logger.Debug($"reuse pooled session with sid {session.sessionId}");
                            return session;
                        }
                    }
                }
            }
            return null;
        }
        internal bool addSession(SFSession session)
        {
            logger.Debug("SessionPool::addSession");
            if (!pooling)
                return false;
            long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();
            // Do not pool session if not open or already expired
            if ((session.startTime == 0) || ((session.startTime + timeout) <= timeNow))
                return false;

            lock (_sessionPoolLock)
            {
                if (sessionPool.Count >= maxPoolSize)
                {
                    cleanExpiredSessions();
                }
                if (sessionPool.Count >= maxPoolSize)
                {
                    // pool is full
                    return false;
                }

                logger.Debug($"pool connection with sid {session.sessionId}");
                sessionPool.Add(session);
                return true;
            }
        }

        internal void ClearAllPools()
        {
            logger.Debug("SessionPool::ClearAllPools");
            lock (_sessionPoolLock)
            {
                foreach (SFSession session in sessionPool)
                {
                    session.close();
                }
                sessionPool.Clear();
            }
        }

        public void SetMaxPoolSize(int size)
        {
            maxPoolSize = size;
        }

        public void SetTimeout(long time)
        {
            timeout = time;
        }

        public int GetCurrentPoolSize()
        {
            return sessionPool.Count;
        }

        public bool SetPooling(bool isEnable)
        {
            if (pooling == isEnable)
                return false;
            pooling = isEnable;
            if (!pooling)
            {
                ClearAllPools();
            }
            return true;
        }

        public bool GetPooling()
        {
            return pooling;
        }
    }
    public class SnowflakeDbConnectionPool
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        internal static SFSession getSession(string connStr)
        {
            logger.Debug("SnowflakeDbConnectionPool::getSession");
            return SessionPoolSingleton.Instance.getSession(connStr);
        }

        internal static bool addSession(SFSession session)
        {
            logger.Debug("SnowflakeDbConnectionPool::addSession");
            return SessionPoolSingleton.Instance.addSession(session);
        }

        public static void ClearAllPools()
        {
            logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            SessionPoolSingleton.Instance.ClearAllPools();
        }

        public static void SetMaxPoolSize(int size)
        {
            SessionPoolSingleton.Instance.SetMaxPoolSize(size);
        }

        public static void SetTimeout(long time)
        {
            SessionPoolSingleton.Instance.SetTimeout(time);
        }

        public static int GetCurrentPoolSize()
        {
            return SessionPoolSingleton.Instance.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            return SessionPoolSingleton.Instance.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            return SessionPoolSingleton.Instance.GetPooling();
        }
    }
}
