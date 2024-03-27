using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Snowflake.Data.Core.Session
{
    internal class SessionOrCreationTokens
    {
        internal static readonly IList<SessionCreationToken> s_emptySessionCreationTokenList = ImmutableList<SessionCreationToken>.Empty; // used as a memory optimization not to create many short living empty list
        
        public SFSession Session { get; }
        public IList<SessionCreationToken> SessionCreationTokens { get; }

        public SessionOrCreationTokens(SFSession session)
        {
            Session = session ?? throw new Exception("Internal error: missing session");
            SessionCreationTokens = s_emptySessionCreationTokenList;
        }

        public SessionOrCreationTokens(IList<SessionCreationToken> sessionCreationTokens)
        {
            Session = null;
            if (sessionCreationTokens == null || sessionCreationTokens.Count == 0)
            {
                throw new Exception("Internal error: missing session creation token");
            }
            SessionCreationTokens = sessionCreationTokens;
        }

        public IList<SessionCreationToken> BackgroundSessionCreationTokens()
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
