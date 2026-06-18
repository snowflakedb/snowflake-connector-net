using System;
using Xunit;

namespace Snowflake.Data.Tests.Util
{
    public enum SFTableType
    {
        Standard,
        Hybrid,
        Iceberg
    }

    static class TableTypeExtensions
    {
        internal static string TableDDLCreationPrefix(this SFTableType val) => val == SFTableType.Standard ? "" : val.ToString().ToUpper();

        internal static string TableDDLCreationFlags(this SFTableType val)
        {
            if (val != SFTableType.Iceberg)
                return "";
            var externalVolume = Environment.GetEnvironmentVariable("ICEBERG_EXTERNAL_VOLUME");
            var catalog = Environment.GetEnvironmentVariable("ICEBERG_CATALOG");
            var baseLocation = Environment.GetEnvironmentVariable("ICEBERG_BASE_LOCATION");
            Assert.NotNull(externalVolume);
            Assert.NotNull(catalog);
            Assert.NotNull(baseLocation);
            return $"EXTERNAL_VOLUME = '{externalVolume}' CATALOG = '{catalog}' BASE_LOCATION = '{baseLocation}'";
        }
    }
}
