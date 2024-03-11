using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core.Session
{
    /**
     * It describes what should happen to a session with a changed state (e. g. schema/role/etc.) when it is being returned to the pool. 
     */
    internal enum ChangedSessionBehavior
    {
        OriginalPool,
        ChangePool,
        Destroy
    }

    internal static class ChangedSessionBehaviorExtensions
    {
        public static List<string> StringValues()
        {
            return Enum.GetValues(typeof(ChangedSessionBehavior))
                .Cast<ChangedSessionBehavior>()
                .Where(e => e == ChangedSessionBehavior.OriginalPool) // currently we support only OriginalPool case; TODO: SNOW-937188
                .Select(b => b.ToString())
                .ToList();
        }
        
        public static ChangedSessionBehavior From(string changedSession)
        {
            return (ChangedSessionBehavior) Enum.Parse(typeof(ChangedSessionBehavior), changedSession, true);
        }
    }
}
