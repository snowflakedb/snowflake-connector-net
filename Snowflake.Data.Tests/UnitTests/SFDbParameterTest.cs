namespace Snowflake.Data.Tests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using System;
    using System.Data;
    using System.Text;

    [TestFixture]
    class SFDbParameterTest
    {
        SnowflakeDbParameter _parameter;

        [Test]
        public void TestDefaultDbParameter()
        {
            _parameter = new SnowflakeDbParameter();
            Assert.Equal(SFDataType.None, _parameter.SFDataType);
        }

        [Test]
        public void TestDbParameterWithNameAndDataType([Values] SFDataType expectedSFDataType)
        {
            string expectedParameterName = "1";
            _parameter = new SnowflakeDbParameter(expectedParameterName, expectedSFDataType);
            Assert.Equal(expectedParameterName, _parameter.ParameterName);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);
        }

        [Test]
        public void TestDbParameterWithIndexAndDataType([Values] SFDataType expectedSFDataType)
        {
            int expectedParameterIndex = 1;

            _parameter = new SnowflakeDbParameter(expectedParameterIndex, expectedSFDataType);
            Assert.Equal(expectedParameterIndex.ToString(), _parameter.ParameterName);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);
        }

        [Test]
        public void TestDbParameterDbType([Values] DbType expectedDbType)
        {
            _parameter = new SnowflakeDbParameter();
            _parameter.DbType = expectedDbType;
            Assert.Equal(expectedDbType, _parameter.DbType);
        }

        [Test]
        public void TestDbParameterDirection([Values] ParameterDirection ParameterDirection)
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

        [Test]
        public void TestDbParameterIsNullable([Values] SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(false, _parameter.IsNullable);

            _parameter.IsNullable = true;
            Assert.Equal(true, _parameter.IsNullable);
        }

        [Test]
        public void TestDbParameterSize([Values] SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Zero(_parameter.Size);

            _parameter.Size = 1;
            Assert.Equal(1, _parameter.Size);
        }

        [Test]
        public void TestDbParameterSourceColumn([Values] SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(null, _parameter.SourceColumn);

            string col = "col";
            _parameter.SourceColumn = col;
            Assert.Equal(col, _parameter.SourceColumn);
        }

        [Test]
        public void TestDbParameterSourceColumnNullMapping([Values] SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(false, _parameter.SourceColumnNullMapping);

            _parameter.SourceColumnNullMapping = true;
            Assert.Equal(true, _parameter.SourceColumnNullMapping);
        }

        [Test]
        public void TestDbParameterValue([Values] SFDataType SFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, SFDataType);
            Assert.Equal(null, _parameter.Value);

            object obj = new object();
            _parameter.Value = obj;
            Assert.Equal(obj, _parameter.Value);
        }

        [Test]
        public void TestDbParameterResetDbType([Values] SFDataType expectedSFDataType)
        {
            _parameter = new SnowflakeDbParameter(1, expectedSFDataType);
            Assert.Equal(expectedSFDataType, _parameter.SFDataType);

            _parameter.ResetDbType();
            Assert.Equal(SFDataType.None, _parameter.SFDataType);
        }

        [Test]
        public void TestDbTypeExplicitAssignment([Values] DbType expectedDbType)
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

        [Test]
        public void TestDbTypeExplicitAssignmentWithNullValueAndDefaultDbType()
        {
            _parameter = new SnowflakeDbParameter();
            _parameter.Value = null;
            Assert.Equal(default(DbType), _parameter.DbType);
        }

        [Test]
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
