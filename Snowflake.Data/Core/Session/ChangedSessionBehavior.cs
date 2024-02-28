using System;
using System.Collections.Generic;
using System.Linq;

namespace Snowflake.Data.Core.Session
{
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
                .Where(e => e == ChangedSessionBehavior.OriginalPool) // currently we support only OriginalPool case
                .Select(b => b.ToString())
                .ToList();
        }
        
        public static ChangedSessionBehavior From(string changedSession)
        {
            return (ChangedSessionBehavior) Enum.Parse(typeof(ChangedSessionBehavior), changedSession, true);
        }
    }
}