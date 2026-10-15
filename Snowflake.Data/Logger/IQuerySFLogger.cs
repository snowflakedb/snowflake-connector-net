using Snowflake.Data.Core;
using Snowflake.Data.Log;

namespace Snowflake.Data.Logger;

internal interface IQuerySFLogger : SFLogger
{
    void LogQuery(SFRestRequest request, string requestId);
}
