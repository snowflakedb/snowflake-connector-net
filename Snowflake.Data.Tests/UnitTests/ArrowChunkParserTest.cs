/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Configuration;
    using Snowflake.Data.Core;
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    [TestFixture, NonParallelizable]
    class ArrowChunkParserTest
    {
        [Test]
        [Ignore("ArrowChunkParserTest")]
        public void ArrowChunkParserTestDone()
        {
            // Do nothing - test progress marker
        }

        [Test]
        public void TestParseChunkReadsRecordBatches([Values(1, 2, 4)] int numberOfRecordBatch)
        {
            // Arrange
            var recordBatch = new RecordBatch.Builder()
                .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, 10))))
                .Build();

            MemoryStream stream = new MemoryStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(stream, recordBatch.Schema);
            for (var i = 0; i < numberOfRecordBatch; i++)
            {
                writer.WriteRecordBatch(recordBatch);
            }
            writer.WriteEnd();
            stream.Position = 0;
            
            var parser = new ArrowChunkParser(stream);
            
            // Act
            var chunk = new ArrowResultChunk(1);
            var task = parser.ParseChunk(chunk);
            task.Wait();
            
            // Assert
            Assert.AreEqual(numberOfRecordBatch, chunk.RecordBatch.Count);
        }
    }
}
