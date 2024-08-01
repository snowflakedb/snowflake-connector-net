using System.Linq;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public abstract class StructuredTypesIT : SFBaseTest
    {
        protected void EnableStructuredTypes(SnowflakeDbConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true";
                command.ExecuteNonQuery();
                command.CommandText = "alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = JSON";
                command.ExecuteNonQuery();
            }
        }

        protected string RemoveWhiteSpaces(string text)
        {
            var charArrayWithoutWhiteSpaces = text.ToCharArray()
                .Where(c => !char.IsWhiteSpace(c))
                .ToArray();
            return new string(charArrayWithoutWhiteSpaces);
        }
    }
}
