/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using Snowflake.Data.Core;
    using NUnit.Framework;
    using System.Threading;
    using System.Globalization;

    [TestFixture]
    class SFDataConverterTest
    {
        [Test]
        public void TestConvertBindToSFValFinlandLocale()
        {
            Thread testThread = new Thread(() =>
            {
                CultureInfo ci = new CultureInfo("en-FI");

                Thread.CurrentThread.CurrentCulture = ci;

                System.Tuple<string, string> t = 
                    SFDataConverter.csharpTypeValToSfTypeVal(System.Data.DbType.Double, 1.2345);

                Assert.AreEqual("REAL", t.Item1);
                Assert.AreEqual("1.2345", t.Item2);
            });
            testThread.Start();
            testThread.Join();
        }
    }
}
