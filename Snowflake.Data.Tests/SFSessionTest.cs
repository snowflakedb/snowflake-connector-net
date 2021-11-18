/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;

    [TestFixture]
    class SFSessionTest
    {
        // Mock test for session gone
        [Test]
        public void TestSessionGoneWhenClose()
        {
            Mock.MockCloseSessionGone restRequester = new Mock.MockCloseSessionGone();
            SFSession sfSession = new SFSession("account=test;user=test;password=test", null, restRequester);
            sfSession.Open();
            sfSession.close(); // no exception is raised.
        }

    }
}
