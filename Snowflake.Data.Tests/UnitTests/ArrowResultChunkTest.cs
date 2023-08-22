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
        private static int s_rowCount = 10;
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
            s_rowCount = 10;
            _recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, s_rowCount))))
                .Build();
            _chunk = new ArrowResultChunk(_recordBatch);
        }
        
        [Test]
        public void TestExtractCell()
        {
            var column = (Int32Array)_recordBatch.Column(0);
            for (var i = 0; i < s_rowCount; ++i)
            {
                Assert.AreEqual(column.GetValue(i).ToString(), _chunk.ExtractCell(i, 0).SafeToString());
            }
        }

        [Test]
        public void TestExtractCellOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _chunk.ExtractCell(s_rowCount, 0).SafeToString());
        }
        
        [Test]
        public void TestGetRowCount()
        {
            Assert.AreEqual(s_rowCount, _chunk.GetRowCount());
        }

        [Test]
        public void TestGetChunkIndex()
        {
            Assert.AreEqual(0, _chunk.GetChunkIndex());
        }
        
    }
}
