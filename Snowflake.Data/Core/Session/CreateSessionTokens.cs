using System;
using System.Collections.Generic;
using System.Threading;

namespace Snowflake.Data.Core.Session
{
    internal class CreateSessionTokens: ICreateSessionTokens
    {
        internal long _timeout { get; set; }  = Timeout;
        private const long Timeout = 30000; // 30 seconds as default
        private readonly object _tokenLock = new object();
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
        private readonly List<CreateSessionToken> _tokens = new List<CreateSessionToken>();
        
        public CreateSessionToken BeginCreate()
        {
            _lock.EnterWriteLock();
            try
            {
                var token = new CreateSessionToken(_timeout);
                _tokens.Add(token);
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => t.IsExpired(now));
                return token;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void EndCreate(CreateSessionToken token)
        {
            _lock.EnterWriteLock();
            try
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => token.Id == t.Id || t.IsExpired(now));
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public int Count()
        {
            _lock.EnterReadLock();
            try
            {
                return _tokens.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }
}
