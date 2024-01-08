using System;
using System.Collections.Generic;
using System.Threading;

namespace Snowflake.Data.Core.Session
{
    internal class SessionCreationTokenCounter: ISessionCreationTokenCounter
    {
        private readonly long _timeoutMillis;
        private readonly ReaderWriterLockSlim _tokenLock = new ReaderWriterLockSlim();
        private readonly List<SessionCreationToken> _tokens = new List<SessionCreationToken>();

        public SessionCreationTokenCounter(long timeoutMillis)
        {
            _timeoutMillis = timeoutMillis;
        }

        public SessionCreationToken NewToken()
        {
            _tokenLock.EnterWriteLock();
            try
            {
                var token = new SessionCreationToken(_timeoutMillis);
                _tokens.Add(token);
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => t.IsExpired(now));
                return token;
            }
            finally
            {
                _tokenLock.ExitWriteLock();
            }
        }

        public void RemoveToken(SessionCreationToken creationToken)
        {
            _tokenLock.EnterWriteLock();
            try
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => creationToken.Id == t.Id || t.IsExpired(now));
            }
            finally
            {
                _tokenLock.ExitWriteLock();
            }
        }

        public int Count()
        {
            _tokenLock.EnterReadLock();
            try
            {
                return _tokens.Count;
            }
            finally
            {
                _tokenLock.ExitReadLock();
            }
        }
    }
}
