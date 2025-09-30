using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core.Session
{
    internal class SessionOrCreationTokens
    {
        internal static readonly List<SessionCreationToken> s_emptySessionCreationTokenList = new List<SessionCreationToken>(); // used as a memory optimization not to create many short living empty list

        public SFSession Session { get; }
        public List<SessionCreationToken> SessionCreationTokens { get; }

        public SessionOrCreationTokens(SFSession session)
        {
            Session = session ?? throw new Exception("Internal error: missing session");
            SessionCreationTokens = s_emptySessionCreationTokenList;
        }

        public SessionOrCreationTokens(List<SessionCreationToken> sessionCreationTokens)
        {
            Session = null;
            if (sessionCreationTokens == null || sessionCreationTokens.Count == 0)
            {
                throw new Exception("Internal error: missing session creation token");
            }
            SessionCreationTokens = sessionCreationTokens;
        }

        public List<SessionCreationToken> BackgroundSessionCreationTokens()
        {
            if (Session == null)
            {
                return SessionCreationTokens.Skip(1).ToList();
            }
            return SessionCreationTokens;
        }

        public SessionCreationToken SessionCreationToken() => SessionCreationTokens.First();
    }
}
