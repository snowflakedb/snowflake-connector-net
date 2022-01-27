using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Client
{
    public class SnowflakeQueryStatus

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

        /// <summary>
        /// The id used to track the query in Snowflake.
        /// </summary>
        public string QueryId { get; private set; }


        public SnowflakeQueryStatus(string queryId, bool isQueryDone, bool isQuerySuccessful)
        {
            this.QueryId = queryId;
            this.IsQueryDone = isQueryDone;
            this.IsQuerySuccessful = isQuerySuccessful;
        }
    }
}
