using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class ArrowResultSetTest
    {
        private const int RowCount = 10;
        private const int ColumnIndex = 0;

        private RecordBatch _recordBatch;
        private ArrowResultSet _arrowResultSet;

        [SetUp]
        public void BeforeTest()
        {
            // by default generate Int32 values from 1 to RowCount
            PrepareTestCase(SFDataType.FIXED, 0, Enumerable.Range(1, RowCount).ToArray());
        }

        [Test]
        public void TestResultFormatIsArrow()
        {
            Assert.AreEqual(ResultFormat.ARROW, _arrowResultSet.ResultFormat);
        }

        [Test]
        public void TestNextReturnsFalseIfNoData()
        {
            var responseData = PrepareResponseData(_recordBatch, SFDataType.FIXED, 0);
            var sfStatement = PrepareStatement();

            // if there are no results int the response, rowserBaset64 is empty
            responseData.rowsetBase64 = "";
            var arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());

            Assert.IsFalse(arrowResultSet.Next());
        }

        [Test]
        public void TestNextReturnsTrueUntilRowsExist()
        {
            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(_arrowResultSet.Next());
            }
            Assert.IsFalse(_arrowResultSet.Next());
        }

        [Test]
        public async Task TestNextAsyncReturnsTrueUntilRowsExist()
        {
            for (var i = 0; i < RowCount; ++i)
            {
                Assert.IsTrue(await _arrowResultSet.NextAsync());
            }
            Assert.IsFalse(await _arrowResultSet.NextAsync());
        }

        [Test]
        public void TestNextResultReturnsFalse()
        {
            Assert.IsFalse(_arrowResultSet.NextResult());
        }

        [Test]
        public async Task TestNextResultAsyncReturnsFalse()
        {
            Assert.IsFalse(await _arrowResultSet.NextResultAsync(CancellationToken.None));
        }

        [Test]
        public void TestHasRowsReturnsTrueIfRowExists()
        {
            Assert.IsTrue(_arrowResultSet.HasRows());
        }

        [Test]
        public void TestHasRowsReturnsFalseIfNoRows()
        {
            PrepareTestCase(SFDataType.FIXED, 0, new sbyte[] { });

            Assert.IsFalse(_arrowResultSet.HasRows());
        }

        [Test]
        public void TestRewindReturnsFalseBeforeFirstRow()
        {
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsFalseForFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsTrueForSecondRowAndMovesToFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            _arrowResultSet.Next(); // move to second row
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestRewindReturnsTrueForThirdRowAndMovesToFirstRow()
        {
            _arrowResultSet.Next(); // move to first row
            _arrowResultSet.Next(); // move to second row
            _arrowResultSet.Next(); // move to third row
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsTrue(_arrowResultSet.Rewind());
            Assert.IsFalse(_arrowResultSet.Rewind());
        }

        [Test]
        public void TestGetValueReturnsNull()
        {
            var responseData = PrepareResponseData(ArrowResultChunkTest.RecordBatchWithNullValue, SFDataType.FIXED, 0);
            var sfStatement = PrepareStatement();
            var arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());

            arrowResultSet.Next();

            Assert.AreEqual(true, arrowResultSet.IsDBNull(0));
            Assert.AreEqual(DBNull.Value, arrowResultSet.GetValue(0));
        }

        [Test]
        public void TestGetDecimal()
        {
            var testValues = new decimal[] { 0, 100, -100, Decimal.MaxValue, Decimal.MinValue };

            TestGetNumber(testValues);
        }

        [Test]
        public void TestGetNumber64()
        {
            var testValues = new long[] { 0, 100, -100, Int64.MaxValue, Int64.MinValue };

            TestGetNumber(testValues);
        }

        [Test]
        public void TestGetNumber32()
        {
            var testValues = new int[] { 0, 100, -100, Int32.MaxValue, Int32.MinValue };

            TestGetNumber(testValues);
        }

        [Test]
        public void TestGetNumber16()
        {
            var testValues = new short[] { 0, 100, -100, Int16.MaxValue, Int16.MinValue };

            TestGetNumber(testValues);
        }

        [Test]
        public void TestGetNumber8()
        {
            var testValues = new sbyte[] { 0, 127, -128 };

            TestGetNumber(testValues);
        }

        private void TestGetNumber(IEnumerable testValues)
        {
            for (var scale = 0; scale <= 9; ++scale)
            {
                PrepareTestCase(SFDataType.FIXED, scale, testValues);

                foreach (var testValue in testValues)
                {
                    _arrowResultSet.Next();

                    var expectedValue = Convert.ToDecimal(testValue) / (decimal)Math.Pow(10, scale);
                    Assert.AreEqual(expectedValue, _arrowResultSet.GetValue(ColumnIndex));
                    Assert.AreEqual(expectedValue, _arrowResultSet.GetDecimal(ColumnIndex));
                    Assert.AreEqual(expectedValue, _arrowResultSet.GetDouble(ColumnIndex));
                    Assert.AreEqual(expectedValue, _arrowResultSet.GetFloat(ColumnIndex));

                    if (expectedValue >= Int64.MinValue && expectedValue <= Int64.MaxValue)
                    {
                        // get integer value
                        long expectedInteger = (long)expectedValue;

                        Assert.AreEqual(expectedInteger, _arrowResultSet.GetInt64(ColumnIndex));
                        if (expectedInteger >= Int32.MinValue && expectedInteger <= Int32.MaxValue)
                            Assert.AreEqual(expectedInteger, _arrowResultSet.GetInt32(ColumnIndex));
                        else
                            Assert.Throws<OverflowException>(() => _arrowResultSet.GetInt32(ColumnIndex));
                        if (expectedInteger >= Int16.MinValue && expectedInteger <= Int16.MaxValue)
                            Assert.AreEqual(expectedInteger, _arrowResultSet.GetInt16(ColumnIndex));
                        else
                            Assert.Throws<OverflowException>(() => _arrowResultSet.GetInt16(ColumnIndex));
                        if (expectedInteger >= 0 && expectedInteger <= 255)
                            Assert.AreEqual(expectedInteger, _arrowResultSet.GetByte(ColumnIndex));
                        else
                            Assert.Throws<OverflowException>(() => _arrowResultSet.GetByte(ColumnIndex));
                    }
                }
            }
        }

        [Test]
        public void TestGetBoolean()
        {
            var testValues = new bool[] { true, false };

            PrepareTestCase(SFDataType.BOOLEAN, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                Assert.AreEqual(testValue, _arrowResultSet.GetBoolean(ColumnIndex));
            }
        }

        [Test]
        public void TestGetReal()
        {
            var testValues = new double[] { 0, Double.MinValue, Double.MaxValue };

            PrepareTestCase(SFDataType.REAL, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                Assert.AreEqual(testValue, _arrowResultSet.GetDouble(ColumnIndex));
            }
        }

        [Test]
        public void TestGetText()
        {
            var testValues = new string[]
            {
                "",
                TestDataGenarator.StringWithUnicode
            };

            PrepareTestCase(SFDataType.TEXT, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                Assert.AreEqual(testValue, _arrowResultSet.GetString(ColumnIndex));
            }
        }

        [Test]
        public void TestGetTextWithOneChar()
        {
            char[] testValues;

#if NET462
            var charArr = TestDataGenarator.AsciiCodes.ToList();
            charArr.Add(TestDataGenarator.SnowflakeUnicode);
            testValues = charArr.ToArray();
#else
            testValues =
            TestDataGenarator.AsciiCodes.ToCharArray()
                .Append(TestDataGenarator.SnowflakeUnicode)
                .ToArray();
#endif

            PrepareTestCase(SFDataType.TEXT, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetChar(ColumnIndex));
            }
        }

        [Test]
        public void TestGetArray()
        {
            var testValues = new string[]
            {
                "",
                TestDataGenarator.StringWithUnicode
            };

            PrepareTestCase(SFDataType.ARRAY, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                char[] buffer = new char[1000];
                var len = _arrowResultSet.GetChars(ColumnIndex, 0, buffer, 0, buffer.Length);
                var str = new String(buffer, 0, (int)len);
                Assert.AreEqual(testValue, str);
                Assert.AreEqual(testValue.Length, str.Length);
            }
        }

        [Test]
        public void TestGetBinary()
        {
            var testValues = new byte[][]
            {
                new byte[] { },
                new byte[] { 0, 19, 33, 200, 10, 13, 255 }
            };

            PrepareTestCase(SFDataType.BINARY, 0, testValues);
            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                byte[] buffer = new byte[100];
                var len = _arrowResultSet.GetBytes(ColumnIndex, 0, buffer, 0, buffer.Length);
                Assert.AreEqual(testValue.Length, len);
                for (var j = 0; j < len; ++j)
                    Assert.AreEqual(testValue[j], buffer[j], "position " + j);
            }
        }

        [Test]
        public void TestGetDate()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01"),
                DateTime.Parse("0001-01-01"),
                DateTime.Parse("9999-12-31")
            };

            PrepareTestCase(SFDataType.DATE, 0, testValues);

            foreach (var testValue in testValues)
            {
                _arrowResultSet.Next();
                Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                Assert.AreEqual(testValue, _arrowResultSet.GetDateTime(ColumnIndex));
            }
        }

        [Test]
        public void TestGetTime()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01 12:12:12.1234567"),
                DateTime.Parse("0001-01-01 00:00:00.0000000"),
                DateTime.Parse("9999-12-31 23:59:59.9999999")
            };

            for (var scale = 0; scale <= 7; ++scale)
            {
                var values = ArrowResultChunkTest.TruncateValues(testValues, scale);
                PrepareTestCase(SFDataType.TIME, scale, values);

                foreach (var testValue in values)
                {
                    _arrowResultSet.Next();
                    Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                    Assert.AreEqual(testValue, _arrowResultSet.GetDateTime(ColumnIndex));
                }
            }
        }

        [Test]
        public void TestGetTimestampTz()
        {
            var testValues = new DateTimeOffset[]
            {
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567 +0500"),
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567 -0500"),
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567 +1400"),
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567 -1400"),
                DateTimeOffset.Parse("0001-01-01 00:00:00.0000000 +0000"),
                DateTimeOffset.Parse("9999-12-31 23:59:59.9999999 +0000"),
            };

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = ArrowResultChunkTest.TruncateValues(testValues, scale);
                PrepareTestCase(SFDataType.TIMESTAMP_TZ, scale, values);

                foreach (var testValue in values)
                {
                    _arrowResultSet.Next();
                    Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                }
            }
        }

        [Test]
        public void TestGetTimestampLtz()
        {
            var testValues = new DateTimeOffset[]
            {
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567").ToLocalTime(),
                DateTimeOffset.Parse("0001-01-01 00:00:00.0000000 +0000").ToLocalTime(),
                DateTimeOffset.Parse("9999-12-31 23:59:59.9999999 +0000").ToLocalTime(),
            };

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = ArrowResultChunkTest.TruncateValues(testValues, scale);
                PrepareTestCase(SFDataType.TIMESTAMP_LTZ, scale, values);

                foreach (var testValue in values)
                {
                    _arrowResultSet.Next();
                    Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                }
            }
        }

        [Test]
        public void TestGetTimestampNtz()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01 12:12:12.1234567"),
                DateTime.Parse("0001-01-01 00:00:00.0000000"),
                DateTime.Parse("9999-12-31 23:59:59.9999999")
            };

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = ArrowResultChunkTest.TruncateValues(testValues, scale);
                PrepareTestCase(SFDataType.TIMESTAMP_NTZ, scale, values);

                foreach (var testValue in values)
                {
                    _arrowResultSet.Next();
                    Assert.AreEqual(testValue, _arrowResultSet.GetValue(ColumnIndex));
                    Assert.AreEqual(testValue, _arrowResultSet.GetDateTime(ColumnIndex));
                }
            }
        }

        [Test]
        public void TestThrowsExceptionForResultSetWithUnknownSFDataType()
        {
            const string UnknownDataType = "FAKE_TYPE";
            QueryExecResponseData responseData = new QueryExecResponseData()
            {
                rowType = new List<ExecResponseRowType>()
                {
                    new ExecResponseRowType
                    {
                        name = "name",
                        type = UnknownDataType
                    }
                }
            };

            var exception = Assert.Throws<SnowflakeDbException>(() => new ArrowResultSet(responseData, PrepareStatement(), new CancellationToken()));
            Assert.IsTrue(exception.Message.Contains($"Unknown column type: {UnknownDataType}"));
        }

        [Test]
        public void TestThrowsExceptionForResultSetWithUnknownNativeType()
        {
            QueryExecResponseData responseData = new QueryExecResponseData()
            {
                rowType = new List<ExecResponseRowType>()
                {
                    new ExecResponseRowType
                    {
                        name = "name",
                        type = SFDataType.None.ToString()
                    }
                }
            };

            var exception = Assert.Throws<SnowflakeDbException>(() => new ArrowResultSet(responseData, PrepareStatement(), new CancellationToken()));
            Assert.IsTrue(exception.Message.Contains($"Unknown column type: {SFDataType.None.ToString()}"));
        }

        private void PrepareTestCase(SFDataType sfType, long scale, object values)
        {
            _recordBatch = ArrowResultChunkTest.PrepareRecordBatch(sfType, scale, values);
            var responseData = PrepareResponseData(_recordBatch, sfType, scale);
            var sfStatement = PrepareStatement();

            _arrowResultSet = new ArrowResultSet(responseData, sfStatement, new CancellationToken());
        }

        private QueryExecResponseData PrepareResponseData(RecordBatch recordBatch, SFDataType sfType, long scale)
        {
            return new QueryExecResponseData
            {
                rowType = recordBatch.Schema.FieldsList
                    .Select(col =>
                        new ExecResponseRowType
                        {
                            name = col.Name,
                            type = sfType.ToString(),
                            scale = scale
                        }).ToList(),
                parameters = new List<NameValueParameter>(),
                chunks = null,
                queryResultFormat = ResultFormat.ARROW,
                rowsetBase64 = ConvertToBase64String(recordBatch)
            };
        }

        private string ConvertToBase64String(RecordBatch recordBatch)
        {
            if (recordBatch == null)
                return "";

            using (var stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, recordBatch.Schema))
                {
                    writer.WriteRecordBatch(recordBatch);
                    writer.WriteEnd();
                }
                return Convert.ToBase64String(stream.ToArray());
            }
        }

        private SFStatement PrepareStatement()
        {
            SFSession session = new SFSession("user=user;password=password;account=account;", new SessionPropertiesContext());
            return new SFStatement(session);
        }

    }
}
