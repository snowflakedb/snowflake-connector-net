/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;
    using System.Threading;
    using Snowflake.Data.Log;
    using System.Diagnostics;
    using Snowflake.Data.Tests.Mock;

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Test]
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AZURE", "GCP" })]
        public void TestSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();

                Assert.AreEqual(testConfig.database.ToUpper(), conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);

                conn.Close();
            }

        }        
    }
}


