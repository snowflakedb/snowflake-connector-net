using System.Linq;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public abstract class StructuredTypesIT : SFBaseTestAsync
    {
        protected StructuredTypesIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { }

        protected async Task EnableStructuredTypesAsync(SnowflakeDbConnection connection, ResultFormat resultFormat = ResultFormat.JSON, bool nativeArrow = false)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true";
                await command.ExecuteNonQueryAsync();
                command.CommandText = "alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true";
                await command.ExecuteNonQueryAsync();
                command.CommandText = $"ALTER SESSION SET DOTNET_QUERY_RESULT_FORMAT = {resultFormat}";
                await command.ExecuteNonQueryAsync();
                if (resultFormat == ResultFormat.ARROW)
                {
                    command.CommandText = $"ALTER SESSION SET ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = {nativeArrow}";
                    await command.ExecuteNonQueryAsync();
                    command.CommandText = $"ALTER SESSION SET FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT  = {nativeArrow}";
                    await command.ExecuteNonQueryAsync();
                }
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
