using System;

namespace Snowflake.Data.Core.Session
{
    internal class SessionOrCreateToken
    {
        public SFSession Session { get; }
        public CreateSessionToken CreateToken { get; }

        public SessionOrCreateToken(SFSession session)
        {
            Session = session ?? throw new Exception("Internal error: missing session");
            CreateToken = null;
        }

        public SessionOrCreateToken(CreateSessionToken createToken)
        {
            Session = null;
            CreateToken = createToken ?? throw new Exception("Internal error: missing create token");
        }
    }
}
