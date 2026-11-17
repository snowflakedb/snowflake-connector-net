using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests;

public sealed partial class ArrowResultChunkTest
{
    [SFFact]
    public void TestResetDisposesRecordBatches()
    {
        var allocator = new TrackingAllocator();
        var chunk = new ArrowResultChunk(ReadRecordBatch(_recordBatchOne, allocator));
        chunk.AddRecordBatch(ReadRecordBatch(_recordBatchTwo, allocator));
        Assert.Equal(2, allocator.LiveAllocations);

        chunk.Reset(new ExecResponseChunk { rowCount = RowCountBatchOne, url = "http://test" }, 1);

        Assert.Equal(0, allocator.LiveAllocations);
        Assert.Empty(chunk.RecordBatch);
    }

    [SFFact]
    public void TestResetForRetryDisposesRecordBatches()
    {
        var allocator = new TrackingAllocator();
        var chunk = new ArrowResultChunk(ReadRecordBatch(_recordBatchOne, allocator));

        chunk.ResetForRetry();

        Assert.Equal(0, allocator.LiveAllocations);
        Assert.Empty(chunk.RecordBatch);
    }

    [SFFact]
    public void TestReleaseResourcesDisposesRecordBatchesAndEndsIteration()
    {
        var allocator = new TrackingAllocator();
        var chunk = new ArrowResultChunk(ReadRecordBatch(_recordBatchOne, allocator));
        Assert.True(chunk.Next());

        chunk.ReleaseResources();

        Assert.Equal(0, allocator.LiveAllocations);
        Assert.Empty(chunk.RecordBatch);
        Assert.Equal(0, chunk.RowCount);
        Assert.False(chunk.Next());
    }

    [SFFact]
    public void TestValuesReadBeforeReleaseRemainValidAfterRelease()
    {
        var allocator = new TrackingAllocator();
        var recordBatch = new RecordBatch.Builder()
            .Append("Col_String", false, col => col.String(array => array.Append("abc")))
            .Append("Col_Binary", false, col => col.Binary(array => array.Append(new byte[] { 1, 2, 3 }.AsSpan())))
            .Build();
        var chunk = new ArrowResultChunk(ReadRecordBatch(recordBatch, allocator));
        Assert.True(chunk.Next());
        var text = chunk.ExtractCell(0, SFDataType.TEXT, 0, TimeZoneInfo.Utc);
        var binary = chunk.ExtractCell(1, SFDataType.BINARY, 0, TimeZoneInfo.Utc);

        chunk.ReleaseResources();

        Assert.Equal(0, allocator.LiveAllocations);
        Assert.Equal("abc", text);
        Assert.Equal(new byte[] { 1, 2, 3 }, binary);
    }

    private static RecordBatch ReadRecordBatch(RecordBatch recordBatch, MemoryAllocator allocator)
    {
        var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, recordBatch.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }
        stream.Position = 0;
        using var reader = new ArrowStreamReader(stream, allocator);
        return reader.ReadNextRecordBatch();
    }

    private sealed class TrackingAllocator : MemoryAllocator
    {
        private int _liveAllocations;

        public int LiveAllocations => Volatile.Read(ref _liveAllocations);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            bytesAllocated = length;
            Interlocked.Increment(ref _liveAllocations);
            return new TrackedMemoryOwner(new byte[length], this);
        }

        private sealed class TrackedMemoryOwner : IMemoryOwner<byte>
        {
            private readonly TrackingAllocator _allocator;
            private int _disposed;

            public TrackedMemoryOwner(byte[] buffer, TrackingAllocator allocator)
            {
                Memory = buffer;
                _allocator = allocator;
            }

            public Memory<byte> Memory { get; }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Interlocked.Decrement(ref _allocator._liveAllocations);
                }
            }
        }
    }
}
