using System;
using System.Collections.Generic;
using System.Threading;

namespace Snowflake.Data.Core.Session
{
    internal class CreateSessionTokens: ICreateSessionTokens
    {
        private readonly long _timeoutMillis;
        private readonly ReaderWriterLockSlim _tokenLock = new ReaderWriterLockSlim();
        private readonly List<CreateSessionToken> _tokens = new List<CreateSessionToken>();

        public CreateSessionTokens(long timeoutMillis)
        {
            _timeoutMillis = timeoutMillis;
        }

        public CreateSessionToken BeginCreate()
        {
            _tokenLock.EnterWriteLock();
            try
            {
                var token = new CreateSessionToken(_timeoutMillis);
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

        public void EndCreate(CreateSessionToken token)
        {
            _tokenLock.EnterWriteLock();
            try
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => token.Id == t.Id || t.IsExpired(now));
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
