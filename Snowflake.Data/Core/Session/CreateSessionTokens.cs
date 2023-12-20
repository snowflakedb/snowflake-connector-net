using System;
using System.Collections.Generic;

namespace Snowflake.Data.Core.Session
{
    internal class CreateSessionTokens: ICreateSessionTokens
    {
        internal long _timeout { get; set; }  = Timeout;
        private const long Timeout = 30000; // 30 seconds as default
        private readonly object _tokenLock = new object();
        private readonly List<CreateSessionToken> _tokens = new List<CreateSessionToken>();
        private int _tokenCount = 0;
        
        public CreateSessionToken BeginCreate()
        {
            lock (_tokenLock)
            {
                var token = new CreateSessionToken(_timeout);
                _tokens.Add(token);
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => t.IsExpired(now));
                _tokenCount = _tokens.Count;
                return token;
            }
        }

        public void EndCreate(CreateSessionToken token)
        {
            lock (_tokenLock)
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                _tokens.RemoveAll(t => token.Id == t.Id || t.IsExpired(now));
                _tokenCount = _tokens.Count;
            }
        }

        public int Count()
        {
            return _tokenCount;
        }
    }
}
