using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.Mock
{
    class MockSessionFactoryForRequester : ISessionFactory
    {
        private readonly IMockRestRequester restRequester;

        public MockSessionFactoryForRequester(IMockRestRequester restRequester)
        {
            this.restRequester = restRequester;
        }

        public SFSession NewSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            return new SFSession(connectionString, sessionContext, EasyLoggingStarter.Instance, restRequester);
        }
    }
}
