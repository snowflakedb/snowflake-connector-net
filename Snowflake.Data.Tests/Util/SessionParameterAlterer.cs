using System.Data;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.Util
{
    class SessionParameterAlterer
    {
        public static void SetResultFormat(IDbConnection conn, ResultFormat resultFormat)
        {
            IDbCommand cmd = conn.CreateCommand();
            if (resultFormat == ResultFormat.ARROW)
                // ARROW_FORCE to set Arrow format regardless driver version
                cmd.CommandText = $"alter session set DOTNET_QUERY_RESULT_FORMAT = ARROW_FORCE";
            else
                cmd.CommandText = $"alter session set DOTNET_QUERY_RESULT_FORMAT = {resultFormat}";
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (SnowflakeDbException ex)
            {
                if (ex.Message.Contains("invalid parameter"))
                    return;
                throw;
            }
        }

        public static void RestoreResultFormat(IDbConnection conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "alter session set DOTNET_QUERY_RESULT_FORMAT = default";
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (SnowflakeDbException ex)
            {
                if (ex.Message.Contains("invalid parameter"))
                    return;
                throw;
            }
        }

        public static void SetPrefetchThreads(IDbConnection conn, int prefetchThreads)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"alter session set CLIENT_PREFETCH_THREADS = {prefetchThreads}";
            cmd.ExecuteNonQuery();
        }

        public static void RestorePrefetchThreads(IDbConnection conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "alter session set CLIENT_PREFETCH_THREADS = default";
            cmd.ExecuteNonQuery();
        }
    }
}
