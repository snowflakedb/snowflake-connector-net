/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class ArrowResultChunkTest
    {
        private const int RowCount = 10;
        private RecordBatch _recordBatch;
        private ArrowResultChunk _chunk;
        
        [Test]
        [Ignore("ArrowResultChunkTest")]
        public void SFArrowResultChunkTestDone()
        {
            // Do nothing - test progress marker
        }
        
        [SetUp]
        public void BeforeTest()
        {
            _recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, RowCount))))
                .Build();
            _chunk = new ArrowResultChunk(_recordBatch);
        }
        
        [Test]
        public void TestExtractCellReadsAllRows()
        {
            var column = (Int32Array)_recordBatch.Column(0);
            for (var i = 0; i < RowCount; ++i)
            {
                Assert.AreEqual(column.GetValue(i).ToString(), _chunk.ExtractCell(i, 0).SafeToString());
            }
        }

        [Test]
        public void TestExtractCellThrowsOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _chunk.ExtractCell(RowCount, 0).SafeToString());
        }
        
        [Test]
        public void TestGetRowCountReturnsNumberOfRows()
        {
            Assert.AreEqual(RowCount, _chunk.GetRowCount());
        }

        [Test]
        public void TestGetChunkIndexReturnsFirstChunk()
        {
            Assert.AreEqual(0, _chunk.GetChunkIndex());
        }
        
    }
}
