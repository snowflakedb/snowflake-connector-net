using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Text;
    public class SFDbParameterTest
    {
        SnowflakeDbParameter _parameter;

        public static IEnumerable<object[]> AllSFDataTypes() =>
            Enum.GetValues(typeof(SFDataType)).Cast<SFDataType>().Select(v => new object[] { v });

        public static IEnumerable<object[]> AllDbTypes() =>
            Enum.GetValues(typeof(DbType)).Cast<DbType>().Select(v => new object[] { v });

        public static IEnumerable<object[]> AllParameterDirections() =>
            Enum.GetValues(typeof(ParameterDirection)).Cast<ParameterDirection>().Select(v => new object[] { v });

        [SFFact]
        public void TestDefaultDbParameter()
        {
            _parameter = new SnowflakeDbParameter();
            Assert.Equal(SFDataType.None, _parameter.SFDataType);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterWithNameAndDataType(SFDataType expectedSFDataType)
        {
            string expectedParameterName = "1";
            _parameter = new SnowflakeDbParameter(expectedParameterName, expectedSFDataType);
            Assert.Equal(expectedParameterName, _parameter.ParameterName);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterWithIndexAndDataType(SFDataType expectedSFDataType)
        {
            int expectedParameterIndex = 1;

            _parameter = new SnowflakeDbParameter(expectedParameterIndex, expectedSFDataType);
            Assert.Equal(expectedParameterIndex.ToString(), _parameter.ParameterName);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);
        }

        [SFTheory]
        [MemberData(nameof(AllDbTypes))]
        public void TestDbParameterDbType(DbType expectedDbType)
        {
            _parameter = new SnowflakeDbParameter();
            _parameter.DbType = expectedDbType;
            Assert.Equal(expectedDbType, _parameter.DbType);
        }

        [SFTheory]
        [MemberData(nameof(AllParameterDirections))]
        public void TestDbParameterDirection(ParameterDirection ParameterDirection)
        {
            _parameter = new SnowflakeDbParameter();
            if (ParameterDirection == ParameterDirection.Input)
            {
                _parameter.Direction = ParameterDirection;
                Assert.Equal(ParameterDirection.Input, _parameter.Direction);
            }
            else
            {
                SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => _parameter.Direction = ParameterDirection);
                Assert.Equal(SFError.UNSUPPORTED_FEATURE.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
            }
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterIsNullable(SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.False(_parameter.IsNullable);

            _parameter.IsNullable = true;
            Assert.True(_parameter.IsNullable);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterSize(SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(0, _parameter.Size);

            _parameter.Size = 1;
            Assert.Equal(1, _parameter.Size);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterSourceColumn(SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Null(_parameter.SourceColumn);

            string col = "col";
            _parameter.SourceColumn = col;
            Assert.Equal(col, _parameter.SourceColumn);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterSourceColumnNullMapping(SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.False(_parameter.SourceColumnNullMapping);

            _parameter.SourceColumnNullMapping = true;
            Assert.True(_parameter.SourceColumnNullMapping);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterValue(SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Null(_parameter.Value);

            object obj = new object();
            _parameter.Value = obj;
            Assert.Equal(obj, _parameter.Value);
        }

        [SFTheory]
        [MemberData(nameof(AllSFDataTypes))]
        public void TestDbParameterResetDbType(SFDataType expectedSFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, expectedSFDataType);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);

            _parameter.ResetDbType();
            Assert.Equal(SFDataType.None, _parameter.SFDataType);
        }

        [SFTheory]
        [MemberData(nameof(AllDbTypes))]
        public void TestDbTypeExplicitAssignment(DbType expectedDbType)
        {
            _parameter = new SnowflakeDbParameter();

            switch (expectedDbType)
            {
                case DbType.SByte:
                    _parameter.Value = new sbyte();
                    break;
                case DbType.Byte:
                    _parameter.Value = new byte();
                    break;
                case DbType.Int16:
                    _parameter.Value = new short();
                    break;
                case DbType.Int32:
                    _parameter.Value = new int();
                    break;
                case DbType.Int64:
                    _parameter.Value = new long();
                    break;
                case DbType.UInt16:
                    _parameter.Value = new ushort();
                    break;
                case DbType.UInt32:
                    _parameter.Value = new uint();
                    break;
                case DbType.UInt64:
                    _parameter.Value = new ulong();
                    break;
                case DbType.Decimal:
                    _parameter.Value = new decimal();
                    break;
                case DbType.Boolean:
                    _parameter.Value = true;
                    break;
                case DbType.Single:
                    _parameter.Value = new float();
                    break;
                case DbType.Double:
                    _parameter.Value = new double();
                    break;
                case DbType.Guid:
                    _parameter.Value = new Guid();
                    break;
                case DbType.String:
                    _parameter.Value = "thisIsAString";
                    break;
                case DbType.DateTime:
                    _parameter.Value = DateTime.Now;
                    break;
                case DbType.DateTimeOffset:
                    _parameter.Value = DateTimeOffset.Now;
                    break;
                case DbType.Binary:
                    _parameter.Value = Encoding.UTF8.GetBytes("BinaryData");
                    break;
                case DbType.Object:
                    _parameter.Value = new object();
                    break;
                default:
                    // Not supported
                    expectedDbType = default(DbType);
                    break;
            }

            Assert.Equal(expectedDbType, _parameter.DbType);
        }

        [SFFact]
        public void TestDbTypeExplicitAssignmentWithNullValueAndDefaultDbType()
        {
            _parameter = new SnowflakeDbParameter();
            _parameter.Value = null;
            Assert.Equal(default(DbType), _parameter.DbType);
        }

        [SFFact]
        public void TestDbTypeExplicitAssignmentWithNullValueAndNonDefaultDbType()
        {
            var nonDefaultDbType = DbType.String;
            _parameter = new SnowflakeDbParameter();
            _parameter.Value = null;
            _parameter.DbType = nonDefaultDbType;
            Assert.Equal(nonDefaultDbType, _parameter.DbType);
        }
    }
}
