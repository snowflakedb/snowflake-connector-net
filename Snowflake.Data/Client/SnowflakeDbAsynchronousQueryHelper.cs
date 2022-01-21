using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Client
{
    /// <summary>
    /// Methods to help perform asynchronous queries.
    /// </summary>
    public static class SnowflakeDbAsynchronousQueryHelper
    {
        /// <summary>
        /// Starts a query asynchronously.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns>The query id.</returns>
        public static string StartAsynchronousQuery(SnowflakeDbCommand cmd)
        {
            return cmd.StartAsynchronousQuery();
        }

        /// <summary>
        /// Starts a query asynchronously.
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The query id.</returns>
        public static async Task<string> StartAsynchronousQueryAsync(SnowflakeDbCommand cmd, CancellationToken cancellationToken)
        {
            return await cmd.StartAsynchronousQueryAsync(cancellationToken);
        }

        // https://docs.snowflake.com/en/sql-reference/functions/result_scan.html
        // select * from table(result_scan('query id'));

        // https://docs.snowflake.com/en/sql-reference/functions/query_history.html
        // select *
        // from table(information_schema.query_history())
        // only returns 

        // https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html
        // Latency for the view may be up to 45 minutes.

        //public static string GetAsynchronousQueryStatus(SnowflakeDbConnection conn, string queryId)
        //{
        //    return GetAsynchronousQueryStatusAsync(conn, queryId, CancellationToken.None).Result;
        //}

        //public static async Task<string> GetAsynchronousQueryStatusAsync(SnowflakeDbConnection conn, 
        //    string queryId, CancellationToken cancellationToken)
        //{
        //}

        /// <summary>
        /// Can use the resulting <see cref="SnowflakeDbCommand"/> to fetch the results of the query.
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="queryId"></param>
        /// <returns></returns>
        public static SnowflakeDbCommand CreateAsynchronousQueryResultsCommand(SnowflakeDbConnection conn, string queryId)
        {
            var cmd = conn.CreateCommand();

            cmd.CommandType = System.Data.CommandType.Text;

            // todo: HELP!  For some reason I get this exception when I try to pass the queryId in using a parameter,
            // but it works fine when I pass the queryId in as part of the query text
            // SQL compilation error: argument needs to be a string: '1'
            // https://stackoverflow.com/questions/68289534/using-java-and-snowflake-how-do-you-query-using-the-queryid-in-a-prepared-state

            if (!Guid.TryParse(queryId, out var parsedQueryId))
            {
                // since cannot use parameter, need to make sure queryId is a valid guid to avoid sql injection
                throw new Exception($"Invalid queryId provided.  '{queryId}'");
            }
            cmd.CommandText = $"select * from table(result_scan('{parsedQueryId.ToString()}'));";
            //cmd.CommandText = "select * from table(result_scan(?));";

            //var p = (SnowflakeDbParameter)cmd.CreateParameter();
            //p.ParameterName = "1";
            //p.DbType = System.Data.DbType.String;
            ////p.SFDataType = Core.SFDataType.TEXT;
            //p.Value = queryId;
            //cmd.Parameters.Add(p);

            return (SnowflakeDbCommand)cmd;

        }
    }
}
