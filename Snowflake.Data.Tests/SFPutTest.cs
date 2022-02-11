/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using Snowflake.Data.Log;
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Data.Common;

    [TestFixture]
    class SFPutTest : SFBaseTest
    {
        [Test]
        public void TestPutCommand()
        {
            Console.WriteLine("host: " + testConfig.host);
            Console.WriteLine("account: " + testConfig.account);
            Console.WriteLine("db: " + testConfig.database);
            Console.WriteLine("schema: " + testConfig.schema);
            throw new Exception();
        }
    }
}
