using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Client
{
    public struct AsynchronousQueryStatus
    {
        /// <summary>
        /// When true, indicates that the query has finished for one reason or another, and there is no reason to wait further for
        /// the query to finish.  If false, the query is still executing, so the result will not be available.
        /// </summary>
        public bool IsQueryDone { get; private set; }
        /// <summary>
        /// true indicates that the query completely finished running without any issues, so the result is available.  false indicates
        /// the result is not ready.  You need to inspect <see cref="IsQueryDone"/> to determine if the query is still running 
        /// as opposed to encountering an error.
        /// </summary>
        public bool IsQuerySuccessful { get; private set; }

        public AsynchronousQueryStatus(bool isQueryDone, bool isQuerySuccessful)
        {
            this.IsQueryDone = isQueryDone;
            this.IsQuerySuccessful = isQuerySuccessful;
        }
    }
}
