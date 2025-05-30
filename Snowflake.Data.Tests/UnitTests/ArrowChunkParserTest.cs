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
        public void TestParseChunkReadsRecordBatches([Values(1, 2, 4)] int numberOfRecordBatch)
        {
            // Arrange
            MemoryStream stream = new MemoryStream();

            for (var i = 0; i < numberOfRecordBatch; i++)
            {
                var numberOfRecordsInBatch = 10 * i;
                var recordBatch = new RecordBatch.Builder()
                    .Append("Col_Int32", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(1, numberOfRecordsInBatch))))
                    .Build();

                ArrowStreamWriter writer = new ArrowStreamWriter(stream, recordBatch.Schema, true);
                writer.WriteRecordBatch(recordBatch);
            }
            stream.Position = 0;

            var parser = new ArrowChunkParser(stream);

            // Act
            var chunk = new ArrowResultChunk(1);
            var task = parser.ParseChunk(chunk);
            task.Wait();

            // Assert
            Assert.AreEqual(numberOfRecordBatch, chunk.RecordBatch.Count);
            for (var i = 0; i < numberOfRecordBatch; i++)
            {
                var numberOfRecordsInBatch = 10 * i;
                Assert.AreEqual(numberOfRecordsInBatch, chunk.RecordBatch[i].Length);
            }
        }
    }
}
