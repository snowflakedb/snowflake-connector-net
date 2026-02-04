using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class ArrowResultChunkTest
    {
        private const int RowCountBatchOne = 10;
        private const int RowCountBatchTwo = 20;
        private readonly RecordBatch _recordBatchOne = new RecordBatch.Builder()
            .Append("Col_Int32", false, col => col.Int32(
                array => array.AppendRange(Enumerable.Range(1, RowCountBatchOne))))
            .Build();
        private readonly RecordBatch _recordBatchTwo = new RecordBatch.Builder()
            .Append("Col_Int32", false, col => col.Int32(
                array => array.AppendRange(Enumerable.Range(1, RowCountBatchTwo))))
            .Build();
        internal static readonly RecordBatch RecordBatchWithNullValue = new RecordBatch.Builder()
            .Append("Col_Int32", false, col => col.Int32(array => array.AppendNull()))
            .Build();

        [Test]
        public void TestResultFormatIsArrow()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.AreEqual(ResultFormat.ARROW, chunk.ResultFormat);
        }

        [Test]
        public void TestAddRecordBatchAddsBatchTwo()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);
            chunk.AddRecordBatch(_recordBatchTwo);

            Assert.AreEqual(2, chunk.RecordBatch.Count);
        }

        [Test]
        public void TestNextReturnsFalseIfNoData()
        {
            var chunk = new ArrowResultChunk(0);
            Assert.IsFalse(chunk.Next());
        }

        [Test]
        public void TestNextIteratesThroughAllRecordsOfOneBatch()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                Assert.IsTrue(chunk.Next());
            }
            Assert.IsFalse(chunk.Next());
        }

        [Test]
        public void TestNextIteratesThroughAllRecordsOfTwoBatches()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);
            chunk.AddRecordBatch(_recordBatchTwo);

            for (var i = 0; i < RowCountBatchOne + RowCountBatchTwo; ++i)
            {
                Assert.IsTrue(chunk.Next());
            }
            Assert.IsFalse(chunk.Next());
        }

        [Test]
        public void TestNextSkipsEmptyBatchesBetweenDataBatches()
        {
            // This test reproduces the production issue with an empty batch between two data batches
            // Simulates: batch1 (data) -> batch2 (empty) -> batch3 (data)
            // The bug would cause IndexOutOfRangeException when trying to read from the empty batch

            var batch1 = new RecordBatch.Builder()
                .Append("Col_Text", false, col => col.String(array => array.Append("row0").Append("row1")))
                .Build();

            var emptyBatch = new RecordBatch.Builder()
                .Append("Col_Text", false, col => col.String(array => { }))
                .Build();

            var batch3 = new RecordBatch.Builder()
                .Append("Col_Text", false, col => col.String(array => array.Append("row2").Append("row3")))
                .Build();

            var chunk = new ArrowResultChunk(batch1);
            chunk.AddRecordBatch(emptyBatch);
            chunk.AddRecordBatch(batch3);

            // Process batch 1
            Assert.IsTrue(chunk.Next());
            Assert.AreEqual("row0", chunk.ExtractCell(0, SFDataType.TEXT, 0));
            Assert.IsTrue(chunk.Next());
            Assert.AreEqual("row1", chunk.ExtractCell(0, SFDataType.TEXT, 0));

            // With the fix: Next() should skip the empty batch and go to batch 3
            // With the bug: Next() returns true for empty batch, ExtractCell throws IndexOutOfRangeException
            Assert.IsTrue(chunk.Next(), "Next() should skip empty batch and return true for batch3");
            Assert.AreEqual("row2", chunk.ExtractCell(0, SFDataType.TEXT, 0), "Should read from batch3 after skipping empty batch");
            Assert.IsTrue(chunk.Next());
            Assert.AreEqual("row3", chunk.ExtractCell(0, SFDataType.TEXT, 0));

            Assert.IsFalse(chunk.Next());
        }

        [Test]
        public void TestRewindIteratesThroughAllRecordsOfBatchOne()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            // move to the end of the batch
            while (chunk.Next()) { }

            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                Assert.IsTrue(chunk.Rewind());
            }
            Assert.IsFalse(chunk.Rewind());
        }

        [Test]
        public void TestRewindIteratesThroughAllRecordsOfTwoBatches()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);
            chunk.AddRecordBatch(_recordBatchTwo);

            // move to the end of the batch
            while (chunk.Next()) { }

            for (var i = 0; i < RowCountBatchOne + RowCountBatchTwo; ++i)
            {
                Assert.IsTrue(chunk.Rewind());
            }
            Assert.IsFalse(chunk.Rewind());
        }

        [Test]
        public void TestResetClearsChunkData()
        {
            ExecResponseChunk chunkInfo = new ExecResponseChunk()
            {
                url = "new_url",
                uncompressedSize = 100,
                rowCount = 2
            };
            var chunk = new ArrowResultChunk(_recordBatchOne);

            chunk.Reset(chunkInfo, 0);

            Assert.AreEqual(0, chunk.ChunkIndex);
            Assert.AreEqual(chunkInfo.url, chunk.Url);
            Assert.AreEqual(chunkInfo.rowCount, chunk.RowCount);
        }

        [Test]
        public void TestRowCountReturnsNumberOfRows()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.AreEqual(RowCountBatchOne, chunk.RowCount);
        }

        [Test]
        public void TestGetChunkIndexReturnsFirstChunk()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.AreEqual(-1, chunk.ChunkIndex);
        }

        [Test]
        public void TestUnusedExtractCellThrowsNotSupportedException()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.Throws<NotSupportedException>(() => chunk.ExtractCell(0));
            // Disable warning as we are testing the obsolete method behavior
#pragma warning disable CS0618 // Type or member is obsolete
            Assert.Throws<NotSupportedException>(() => chunk.ExtractCell(0, 0));
#pragma warning restore CS0618 // Type or member is obsolete
        }

        [Test]
        public void TestExtractCellReturnsNull()
        {
            var cases = new Dictionary<ArrowResultChunk, SFDataType>
            {
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Int8", false, col => col.Int8(array => array.AppendNull())).Build()), SFDataType.FIXED },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Int16", false, col => col.Int16(array => array.AppendNull())).Build()), SFDataType.FIXED },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Int32", false, col => col.Int32(array => array.AppendNull())).Build()), SFDataType.FIXED },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Int64", false, col => col.Int64(array => array.AppendNull())).Build()), SFDataType.FIXED },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Decimal128", false, col => col.Decimal128(new Decimal128Type(0, 0), array => array.AppendNull())).Build()), SFDataType.FIXED },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Boolean", false, col => col.Boolean(array => array.AppendNull())).Build()), SFDataType.BOOLEAN },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Real", false, col => col.Double(array => array.AppendNull())).Build()), SFDataType.REAL },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Text", false, col => col.String(array => array.AppendNull())).Build()), SFDataType.TEXT },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Array", false, col => col.String(array => array.AppendNull())).Build()), SFDataType.ARRAY },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Variant", false, col => col.String(array => array.AppendNull())).Build()), SFDataType.VARIANT },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Object", false, col => col.String(array => array.AppendNull())).Build()), SFDataType.OBJECT },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Binary", false, col => col.Binary(array => array.AppendNull())).Build()), SFDataType.BINARY },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Date", false, col => col.Date32(array => array.AppendNull())).Build()), SFDataType.DATE },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Time", false, col => col.Int32(array => array.AppendNull())).Build()), SFDataType.TIME },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Timestamp_TZ", false, col => col.Int32(array => array.AppendNull())).Build()), SFDataType.TIMESTAMP_TZ },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Timestamp_LTZ", false, col => col.Int32(array => array.AppendNull())).Build()), SFDataType.TIMESTAMP_LTZ },
                { new ArrowResultChunk(new RecordBatch.Builder().Append("Col_Timestamp_NTZ", false, col => col.Int32(array => array.AppendNull())).Build()), SFDataType.TIMESTAMP_NTZ },
            };

            foreach (var pair in cases)
            {
                var chunk = pair.Key;
                var type = pair.Value;
                chunk.Next();
                Assert.AreEqual(DBNull.Value, chunk.ExtractCell(0, type, 0), $"Expected DBNull.Value for SFDataType: {type}");
            }
        }

        [Test]
        public void TestExtractCellThrowsExceptionForNoneType()
        {
            var chunk = new ArrowResultChunk(_recordBatchOne);
            chunk.Next();

            Assert.Throws<NotSupportedException>(() => chunk.ExtractCell(0, SFDataType.None, 0));
        }

        [Test]
        public void TestExtractCellReturnsDecimal()
        {
            var testValues = new decimal[] { 0, 100, -100, Decimal.MaxValue, Decimal.MinValue };
            var sfType = SFDataType.FIXED;

            for (var scale = 0; scale <= 9; ++scale)
            {
                TestExtractCell(testValues, sfType, scale, (long)Math.Pow(10, scale));
            }
        }

        [Test]
        public void TestExtractCellReturnsNumber64()
        {
            var testValues = new long[] { 0, 100, -100, Int64.MaxValue, Int64.MinValue };
            var sfType = SFDataType.FIXED;

            for (var scale = 0; scale <= 9; ++scale)
            {
                TestExtractCell(testValues, sfType, scale, (long)Math.Pow(10, scale));
            }
        }

        [Test]
        public void TestExtractCellReturnsNumber32()
        {
            var testValues = new int[] { 0, 100, -100, Int32.MaxValue, Int32.MinValue };
            var sfType = SFDataType.FIXED;

            for (var scale = 0; scale <= 9; ++scale)
            {
                TestExtractCell(testValues, sfType, scale, (long)Math.Pow(10, scale));
            }
        }

        [Test]
        public void TestExtractCellReturnsNumber16()
        {
            var testValues = new short[] { 0, 100, -100, Int16.MaxValue, Int16.MinValue };
            var sfType = SFDataType.FIXED;

            for (var scale = 0; scale <= 9; ++scale)
            {
                TestExtractCell(testValues, sfType, scale, (long)Math.Pow(10, scale));
            }
        }

        [Test]
        public void TestExtractCellReturnsNumber8()
        {
            var testValues = new sbyte[] { 0, 127, -128 };
            var sfType = SFDataType.FIXED;

            for (var scale = 0; scale <= 9; ++scale)
            {
                TestExtractCell(testValues, sfType, scale, (long)Math.Pow(10, scale));
            }
        }

        [Test]
        public void TestExtractCellReturnsBoolean()
        {
            var testValues = new bool[] { true, false };
            var sfType = SFDataType.BOOLEAN;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsReal()
        {
            var testValues = new double[] { 0, Double.MinValue, Double.MaxValue };
            var sfType = SFDataType.REAL;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsText()
        {
            var testValues = new string[]
            {
                "",
                TestDataGenarator.StringWithUnicode
            };
            var sfType = SFDataType.TEXT;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsArray()
        {
            var testValues = new string[]
            {
                "",
                TestDataGenarator.StringWithUnicode
            };
            var sfType = SFDataType.ARRAY;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsBinary()
        {
            var testValues = new byte[][]
            {
                new byte[] { },
                new byte[] { 0, 19, 33, 200, 10, 13, 255 }
            };
            var sfType = SFDataType.BINARY;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsDate()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01"),
                DateTime.Parse("0001-01-01"),
                DateTime.Parse("9999-12-31")
            };
            var sfType = SFDataType.DATE;
            var scale = 0;

            TestExtractCell(testValues, sfType, scale);
        }

        [Test]
        public void TestExtractCellReturnsTime()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01 12:12:12.1234567"),
                DateTime.Parse("0001-01-01 00:00:00.0000000"),
                DateTime.Parse("9999-12-31 23:59:59.9999999")
            };
            var sfType = SFDataType.TIME;

            for (var scale = 0; scale <= 8; ++scale)
            {
                var values = TruncateValues(testValues, scale);
                TestExtractCell(values, sfType, scale);
            }
        }

        [Test]
        public void TestExtractCellReturnsTimestampTz()
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
            var sfType = SFDataType.TIMESTAMP_TZ;

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = TruncateValues(testValues, scale);
                TestExtractCell(values, sfType, scale);
            }
        }

        [Test]
        public void TestExtractCellReturnsTimestampLtz()
        {
            var testValues = new DateTimeOffset[]
            {
                DateTimeOffset.Parse("2019-01-01 12:12:12.1234567").ToLocalTime(),
                DateTimeOffset.Parse("0001-01-01 00:00:00.0000000 +0000").ToLocalTime(),
                DateTimeOffset.Parse("9999-12-31 23:59:59.9999999 +0000").ToLocalTime(),
            };
            var sfType = SFDataType.TIMESTAMP_LTZ;

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = TruncateValues(testValues, scale);
                TestExtractCell(values, sfType, scale);
            }
        }

        [Test]
        public void TestExtractCellReturnsTimestampNtz()
        {
            var testValues = new DateTime[]
            {
                DateTime.Parse("2019-01-01 12:12:12.1234567"),
                DateTime.Parse("0001-01-01 00:00:00.0000000"),
                DateTime.Parse("9999-12-31 23:59:59.9999999")
            };
            var sfType = SFDataType.TIMESTAMP_NTZ;

            for (var scale = 0; scale <= 9; ++scale)
            {
                var values = TruncateValues(testValues, scale);
                TestExtractCell(values, sfType, scale);
            }
        }

        void TestExtractCell(IEnumerable testValues, SFDataType sfType, long scale, long divider = 0)
        {
            var recordBatch = PrepareRecordBatch(sfType, scale, testValues);
            var chunk = new ArrowResultChunk(recordBatch);

            foreach (var testValue in testValues)
            {
                chunk.Next();

                var expectedValue = (divider == 0) ? testValue : Convert.ToDecimal(testValue) / divider;
                Assert.AreEqual(expectedValue, chunk.ExtractCell(0, sfType, scale));
            }
        }
        public static RecordBatch PrepareRecordBatch(SFDataType sfType, long scale, object values)
        {
            IArrowArray column = null;
            switch (sfType)
            {
                case SFDataType.FIXED:
                    switch (values)
                    {
                        case decimal[] val:
                            column = new Decimal128Array.Builder(new Decimal128Type(100, (int)scale))
                                .AppendRange(val.Select(v => v / (decimal)Math.Pow(10, scale)))
                                .Build();
                            break;
                        case long[] val:
                            column = new Int64Array.Builder().AppendRange(val).Build();
                            break;
                        case int[] val:
                            column = new Int32Array.Builder().AppendRange(val).Build();
                            break;
                        case short[] val:
                            column = new Int16Array.Builder().AppendRange(val).Build();
                            break;
                        case sbyte[] val:
                            column = new Int8Array.Builder().AppendRange(val).Build();
                            break;
                    }

                    break;

                case SFDataType.BOOLEAN:
                    column = new BooleanArray.Builder()
                        .AppendRange(values as bool[])
                        .Build();
                    break;

                case SFDataType.REAL:
                    column = new DoubleArray.Builder()
                        .AppendRange(values as double[])
                        .Build();
                    break;

                case SFDataType.TEXT:
                case SFDataType.ARRAY:
                case SFDataType.VARIANT:
                case SFDataType.OBJECT:
                    switch (values)
                    {
                        case string[] arr:
                            column = new StringArray.Builder()
                                .AppendRange(arr)
                                .Build();
                            break;
                        case char[] arr:
                            column = new StringArray.Builder()
                                .AppendRange(arr.Select(ch => ch.ToString()))
                                .Build();
                            break;
                    }
                    break;

                case SFDataType.BINARY:
                    column = new BinaryArray.Builder()
                        .AppendRange(values as byte[][])
                        .Build();
                    break;

                case SFDataType.DATE:
                    column = new Date32Array.Builder()
                        .AppendRange(values as DateTime[])
                        .Build();
                    break;

                case SFDataType.TIME:
                    {
                        var arr = values as DateTime[];
                        column = new Int64Array.Builder()
                            .AppendRange(arr.Select(dt => ConvertTicksToInt64(dt.Ticks, scale)))
                            .Build();
                        break;
                    }

                case SFDataType.TIMESTAMP_TZ:
                    {
                        var arr = values as DateTimeOffset[];
                        if (scale <= 3)
                        {
                            var structField = new StructType(new[]
                            {
                            new Field("value", new Int64Type(), nullable: false),
                            new Field("timezone", new Int32Type(), nullable: false)
                        });

                            column = new StructArray(structField, arr.Length, new IArrowArray[]
                            {
                            new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => ConvertTicksToInt64(dt.UtcTicks, scale)))
                                .Build(),
                            new Int32Array.Builder()
                                .AppendRange(arr.Select(dt => (int)(1440 + dt.Offset.TotalMinutes)))
                                .Build()
                            }, ArrowBuffer.Empty, nullCount: 0);
                        }
                        else
                        {
                            var structField = new StructType(new[]
                            {
                            new Field("epoch", new Int64Type(), nullable: false),
                            new Field("fraction", new Int32Type(), nullable: false),
                            new Field("timezone", new Int32Type(), nullable: false)
                        });

                            column = new StructArray(structField, arr.Length, new IArrowArray[]
                            {
                            new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => dt.ToUnixTimeSeconds()))
                                .Build(),
                            new Int32Array.Builder()
                                .AppendRange(arr.Select(dt => (int)(100 * (dt.UtcTicks % 10000000))))
                                .Build(),
                            new Int32Array.Builder()
                                .AppendRange(arr.Select(dt => (int)(1440 + dt.Offset.TotalMinutes)))
                                .Build()
                            }, ArrowBuffer.Empty, nullCount: 0);
                        }

                        break;
                    }
                case SFDataType.TIMESTAMP_LTZ:
                    {
                        var arr = values as DateTimeOffset[];
                        if (scale <= 3)
                        {
                            column = new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => ConvertTicksToInt64(dt.UtcTicks, scale)))
                                .Build();
                        }
                        else
                        {
                            var structField = new StructType(new[]
                            {
                            new Field("epoch", new Int64Type(), nullable: false),
                            new Field("fraction", new Int32Type(), nullable: false)
                        });

                            column = new StructArray(structField, arr.Length, new IArrowArray[]
                            {
                            new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => dt.ToUnixTimeSeconds()))
                                .Build(),
                            new Int32Array.Builder()
                                .AppendRange(arr.Select(dt => (int)(100 * (dt.UtcTicks % 10000000))))
                                .Build()
                            }, ArrowBuffer.Empty, nullCount: 0);
                        }
                        break;
                    }
                case SFDataType.TIMESTAMP_NTZ:
                    {
                        var arr = values as DateTime[];
                        if (scale <= 3)
                        {
                            column = new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => ConvertTicksToInt64(dt.Ticks, scale)))
                                .Build();
                        }
                        else
                        {
                            var structField = new StructType(new[]
                            {
                            new Field("epoch", new Int64Type(), nullable: false),
                            new Field("fraction", new Int32Type(), nullable: false)
                        });

                            column = new StructArray(structField, arr.Length, new IArrowArray[]
                            {
                            new Int64Array.Builder()
                                .AppendRange(arr.Select(dt => (dt.Ticks - SFDataConverter.UnixEpoch.Ticks) / (long)10000000))
                                .Build(),
                            new Int32Array.Builder()
                                .AppendRange(arr.Select(dt => (int)(100 * (dt.Ticks % 10000000))))
                                .Build()
                            }, ArrowBuffer.Empty, nullCount: 0);
                        }

                        break;
                    }
                default:
                    throw new NotSupportedException();
            }

            return new RecordBatch.Builder()
                .Append("TestColumn", false, column)
                .Build();
        }

        private static long ConvertTicksToInt64(long ticks, long scale)
        {
            long ticksFromEpoch = ticks - SFDataConverter.UnixEpoch.Ticks;
            if (scale <= 7)
                return ticksFromEpoch / (long)Math.Pow(10, 7 - scale);
            else
                return ticksFromEpoch * (long)Math.Pow(10, scale - 7);
        }

        public static DateTime[] TruncateValues(DateTime[] testValues, int scale)
        {
            DateTime[] ret = new DateTime[testValues.Length];
            for (var i = 0; i < testValues.Length; ++i)
                if (scale < 7)
                    ret[i] = testValues[i].AddTicks(-(testValues[i].Ticks % (long)Math.Pow(10, 7 - scale)));
            return ret;
        }

        public static DateTimeOffset[] TruncateValues(DateTimeOffset[] testValues, int scale)
        {
            DateTimeOffset[] ret = new DateTimeOffset[testValues.Length];
            for (var i = 0; i < testValues.Length; ++i)
                if (scale < 7)
                    ret[i] = testValues[i].AddTicks(-(testValues[i].Ticks % (long)Math.Pow(10, 7 - scale)));
            return ret;
        }
    }
}
