/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFArrowResultChunkTest
    {
        [Test]
        [Ignore("SFArrowResultChunkTest")]
        public void SFArrowResultChunkTestDone()
        {
            // Do nothing;
        }

        private RecordBatch PrepareRecordBatch(int rows)
        {
            var range = Enumerable.Range(1, rows);
            return new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(range)))
                .Append("Col_Float", false, col => col.Float(array => array.AppendRange(range.Select(x => Convert.ToSingle(x * 2)))))
                .Append("Col_String", false, col => col.String(array => array.AppendRange(range.Select(x => $"Item {x}"))))
                .Append("Col_Boolean", false, col => col.Boolean(array => array.AppendRange(range.Select(x => x % 2 == 0))))
                .Build();
        }
        
        [Test]
        public void TestChunk()
        {
            var rowCount = 10;
            var recordBatch = PrepareRecordBatch(rowCount);
            SFArrowResultChunk chunk = new SFArrowResultChunk(recordBatch);

            Assert.AreEqual(rowCount, chunk.GetRowCount());
            Assert.AreEqual(0, chunk.GetChunkIndex());
            for (var i = 0; i < rowCount; ++i)
            {
                Assert.AreEqual((i + 1).ToString(), chunk.ExtractCell(i, 0).SafeToString());

                // not supported yet
                Assert.IsNull(chunk.ExtractCell(i, 1).SafeToString());
                Assert.IsNull(chunk.ExtractCell(i, 2).SafeToString());
                Assert.IsNull(chunk.ExtractCell(i, 3).SafeToString());
            }

            Assert.Throws<ArgumentOutOfRangeException>(() => chunk.ExtractCell(rowCount, 0).SafeToString());
        }

    }
}
