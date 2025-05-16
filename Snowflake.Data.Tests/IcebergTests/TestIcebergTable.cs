using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using static Snowflake.Data.Tests.Util.TestData;

namespace Snowflake.Data.Tests.IcebergTests
{
    public class TestIcebergTable : SFBaseTest
    {
        private readonly ResultFormat _resultFormat;
        public TestIcebergTable(ResultFormat resultFormat)
        {
            _resultFormat = resultFormat;
        }
    }
}
