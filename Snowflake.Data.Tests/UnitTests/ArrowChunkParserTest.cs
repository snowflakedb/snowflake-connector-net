using System.IO;
using System.Linq;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests
{
    public class ArrowChunkParserTest
    {
        [SFTheory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        public void TestParseChunkReadsRecordBatches(int numberOfRecordBatch)
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
            var task = parser.ParseChunkAsync(chunk, CancellationToken.None);
            task.Wait();

            // Assert
            Assert.Equal(numberOfRecordBatch, chunk.RecordBatch.Count);
            for (var i = 0; i < numberOfRecordBatch; i++)
            {
                var numberOfRecordsInBatch = 10 * i;
                Assert.Equal(numberOfRecordsInBatch, chunk.RecordBatch[i].Length);
            }
        }
    }
}
