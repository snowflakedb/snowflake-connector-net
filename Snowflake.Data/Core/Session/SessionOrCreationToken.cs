using System;

namespace Snowflake.Data.Core.Session
{
    internal class SessionOrCreationToken
    {
        public SFSession Session { get; }
        public SessionCreationToken SessionCreationToken { get; }

        public SessionOrCreationToken(SFSession session)
        {
            Session = session ?? throw new Exception("Internal error: missing session");
            SessionCreationToken = null;
        }

        public SessionOrCreationToken(SessionCreationToken sessionCreationToken)
        {
            Session = null;
            SessionCreationToken = sessionCreationToken ?? throw new Exception("Internal error: missing session creation token");
        }
    }
}
