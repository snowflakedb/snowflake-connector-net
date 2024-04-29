using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core.Session
{
    /**
     * ChangedSessionBehavior describes what should happen to a session with a changed state (schema/role/database/warehouse) when it is being returned to the pool.
     */
    public enum ChangedSessionBehavior
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
                .Where(e => e != ChangedSessionBehavior.ChangePool) // no support yet for ChangedSessionBehavior.ChangePool case
                .Select(b => b.ToString())
                .ToList();
        }

        public static ChangedSessionBehavior From(string changedSession)
        {
            return (ChangedSessionBehavior) Enum.Parse(typeof(ChangedSessionBehavior), changedSession, true);
        }
    }
}
