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
        private ArrowResultChunk _chunk;
        
        [Test]
        public void TestAddRecordBatchAddsBatchTwo()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);
            _chunk.AddRecordBatch(_recordBatchTwo);

            Assert.AreEqual(2, _chunk.RecordBatch.Count);
        }

        [Test]
        public void TestNextIteratesThroughAllRecordsOfOneBatch()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);
            
            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                Assert.IsTrue(_chunk.Next());
            }
            Assert.IsFalse(_chunk.Next());
        }

        [Test]
        public void TestNextIteratesThroughAllRecordsOfTwoBatches()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);
            _chunk.AddRecordBatch(_recordBatchTwo);
            
            for (var i = 0; i < RowCountBatchOne + RowCountBatchTwo; ++i)
            {
                Assert.IsTrue(_chunk.Next());
            }
            Assert.IsFalse(_chunk.Next());
        }

        [Test]
        public void TestRewindIteratesThroughAllRecordsOfBatchOne()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            // move to the end of the batch
            while (_chunk.Next()) {}
            
            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                Assert.IsTrue(_chunk.Rewind());
            }
            Assert.IsFalse(_chunk.Rewind());
        }
        
        [Test]
        public void TestRewindIteratesThroughAllRecordsOfTwoBatches()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);
            _chunk.AddRecordBatch(_recordBatchTwo);
            
            // move to the end of the batch
            while (_chunk.Next()) {}
            
            for (var i = 0; i < RowCountBatchOne + RowCountBatchTwo; ++i)
            {
                Assert.IsTrue(_chunk.Rewind());
            }
            Assert.IsFalse(_chunk.Rewind());
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
            _chunk = new ArrowResultChunk(_recordBatchOne);
            
            _chunk.Reset(chunkInfo, 0);
            
            Assert.AreEqual(0, _chunk.ChunkIndex);
            Assert.AreEqual(chunkInfo.url, _chunk.Url);
            Assert.AreEqual(chunkInfo.rowCount, _chunk.RowCount);
        }

        [Test]
        public void TestExtractCellWithRowParameterReadsAllRows()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            var column = (Int32Array)_recordBatchOne.Column(0);
            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                var valueFromRecordBatch = column.GetValue(i).ToString();
                Assert.AreEqual(valueFromRecordBatch, _chunk.ExtractCell(i, 0).SafeToString());
            }
        }

        [Test]
        public void TestExtractCellReadsAllRows()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            var column = (Int32Array)_recordBatchOne.Column(0);
            for (var i = 0; i < RowCountBatchOne; ++i)
            {
                var valueFromRecordBatch = column.GetValue(i).ToString();
                
                _chunk.Next();
                Assert.AreEqual(valueFromRecordBatch, _chunk.ExtractCell(0).SafeToString());
            }
        }

        [Test]
        public void TestExtractCellThrowsOutOfRangeException()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            // move to the end of the batch
            while (_chunk.Next()) {}

            Assert.Throws<ArgumentOutOfRangeException>(() => _chunk.ExtractCell(0).SafeToString());
        }
        
        [Test]
        public void TestRowCountReturnsNumberOfRows()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.AreEqual(RowCountBatchOne, _chunk.RowCount);
        }

        [Test]
        public void TestGetChunkIndexReturnsFirstChunk()
        {
            _chunk = new ArrowResultChunk(_recordBatchOne);

            Assert.AreEqual(0, _chunk.ChunkIndex);
        }
        
    }
}
