/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

//using System;
//using System.Collections.Generic;
using System.Linq;
using System.Text;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal static class SqlUtil
    {
        static private SFLogger logger = SFLoggerFactory.GetLogger<SFStatement>();

        /// <summary>
        /// Trim the query by removing spaces and comments at the beginning.
        /// </summary>
        /// <param name="originalSql">The original sql query.</param>
        /// <returns>The query without the blanks and comments at the beginning.</returns>
        internal static string TrimSql(string originalSql)
        {
            char[] sqlQueryBuf = originalSql.ToCharArray();
            var builder = new StringBuilder();

            // skip old c-style comment
            var idx = 0;
            var sqlQueryLen = sqlQueryBuf.Length;
            do
            {
                if (('/' == sqlQueryBuf[idx]) &&
                    (idx + 1 < sqlQueryLen) &&
                    ('*' == sqlQueryBuf[idx + 1]))
                {
                    // Search for the matching */
                    var matchingPos = originalSql.IndexOf("*/", idx + 2);
                    if (matchingPos >= 0)
                    {
                        // Found the comment closing, skip to after
                        idx = matchingPos + 2;
                    }
                }
                else if ((sqlQueryBuf[idx] == '-') &&
                         (idx + 1 < sqlQueryLen) &&
                         (sqlQueryBuf[idx + 1] == '-'))
                {
                    // Search for the new line
                    var newlinePos = originalSql.IndexOf("\n", idx + 2);

                    if (newlinePos >= 0)
                    {
                        // Found the new line, skip to after
                        idx = newlinePos + 1;
                    }
                }

                // No more characters after the closing comment character, stop trimming the query
                if (idx >= sqlQueryLen)
                {
                    break;
                }

                builder.Append(sqlQueryBuf[idx]);
                idx++;
            }
            while (idx < sqlQueryLen);

            var trimmedQuery = builder.ToString();
            trimmedQuery = trimmedQuery.Trim();
            logger.Debug("Trimmed query : " + trimmedQuery);
          
            return trimmedQuery;
        }
    }
}
