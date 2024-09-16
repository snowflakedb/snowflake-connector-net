/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core
{
    using System.Buffers;
    using Log;
    using log4net;
    using log4net.Repository.Hierarchy;

    class SFReusableChunk : BaseResultChunk
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFReusableChunk>();

        internal override ResultFormat ResultFormat => ResultFormat.JSON;

        private readonly BlockResultData data;

        private int _currentRowIndex = -1;

        internal SFReusableChunk(int columnCount)
        {
            ColumnCount = columnCount;
            data = new BlockResultData();
        }

        internal SFReusableChunk(int columnCount, double chunkBlockSize)
        {
            ColumnCount = columnCount;
            data = new BlockResultData(chunkBlockSize);

        }

        internal override void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            base.Reset(chunkInfo, chunkIndex);
            _currentRowIndex = -1;
            data.Reset(RowCount, ColumnCount, chunkInfo.uncompressedSize);
        }

        internal override void ResetForRetry()
        {
            data.ResetForRetry();
        }

        [Obsolete("ExtractCell with rowIndex is deprecated", false)]
        public override UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            _currentRowIndex = rowIndex;
            return ExtractCell(columnIndex);
        }

        public override UTF8Buffer ExtractCell(int columnIndex)
        {
            return data.get(_currentRowIndex * ColumnCount + columnIndex);
        }

        public void AddCell(string val)
        {
            // This method should not be used - we want to avoid unnecessary conversions between string and bytes
            throw new NotImplementedException();
        }

        public void AddCell(byte[] bytes, int length)
        {
            data.add(bytes, length);
        }

        internal override bool Next()
        {
            _currentRowIndex += 1;
            return _currentRowIndex < RowCount;
        }

        internal override bool Rewind()
        {
            _currentRowIndex -= 1;
            return _currentRowIndex >= 0;
        }

        private class BlockResultData
        {
            private static SFLogger logger = SFLoggerFactory.GetLogger<BlockResultData>();

            private static readonly int NULL_VALUE = -100;
            private int blockCount;

            private static int blockLengthBits = 24;
            private static int blockLength = 1 << blockLengthBits;
            int metaBlockCount;
            private static int metaBlockLengthBits = 15;
            private static int metaBlockLength = 1 << metaBlockLengthBits;

            private List<byte[]> data = new List<byte[]>();
            private List<int[]> offsets = new List<int[]>();
            private List<int[]> lengths = new List<int[]>();
            private int nextIndex = 0;
            private int currentDatOffset = 0;

            int savedRowCount;
            int savedColCount;

            internal BlockResultData()
            { }

            public BlockResultData(double chunkBlockSize)
            {
                blockLengthBits = Math.Min(24, (int)Math.Ceiling(Math.Log(chunkBlockSize/2, 2)));
                blockLength = 1 << blockLengthBits;
            }

            internal void Reset(int rowCount, int colCount, int uncompressedSize)
            {
                savedRowCount = rowCount;
                savedColCount = colCount;
                currentDatOffset = 0;
                nextIndex = 0;
                int bytesNeeded = uncompressedSize - (rowCount * 2) - (rowCount * colCount);
                this.blockCount = getBlock(bytesNeeded - 1) + 1;
                this.metaBlockCount = getMetaBlock(rowCount * colCount - 1) + 1;
                this.freeData();
            }

            internal void ResetForRetry()
            {
                currentDatOffset = 0;
                nextIndex = 0;
            }

            public UTF8Buffer get(int index)
            {
                int block = getMetaBlock(index);
                int blockIndex = getMetaBlockIndex(index);

                if (block < 0 || block >= lengths.Count)
                    return null;
                if (blockIndex < 0 || block >= lengths[block].Length)
                    return null;

                int length = lengths[block][blockIndex];

                if (length == NULL_VALUE)
                {
                    return null;
                }
                else
                {
                    int offset = offsets[getMetaBlock(index)]
                        [getMetaBlockIndex(index)];

                    // Create string from the char arrays
                    if (spaceLeftOnBlock(offset) < length)
                    {
                        int copied = 0;
                        byte[] cell = new byte[length];
                        while (copied < length)
                        {
                            int copySize
                                = Math.Min(length - copied, spaceLeftOnBlock(offset + copied));
                            Array.Copy(data[getBlock(offset + copied)],
                                             getBlockOffset(offset + copied),
                                             cell, copied,
                                             copySize);

                            copied += copySize;
                        }
                        return new UTF8Buffer(cell);
                    }
                    else
                    {
                        return new UTF8Buffer(data[getBlock(offset)], getBlockOffset(offset), length);
                    }
                }
            }


            public void freeData()
            {
                var pool = ArrayPool<byte>.Shared;
                var poolInt = ArrayPool<int>.Shared;
                foreach (var d in data)
                {
                    pool.Return(d);
                }
                foreach (var l in lengths)
                {
                    poolInt.Return(l);
                }
                foreach (var o in offsets)
                {
                    poolInt.Return(o);
                }
                this.data.Clear();
                this.lengths.Clear();
                this.offsets.Clear();
                this.data = new List<byte[]>();
                this.lengths = new List<int[]>();
                this.offsets = new List<int[]>();
            }

            public void add(byte[] bytes, int length)
            {
                if (data.Count < blockCount || offsets.Count < metaBlockCount)
                {
                    allocateArrays();
                }

                if (bytes == null)
                {
                    lengths[getMetaBlock(nextIndex)]
                        [getMetaBlockIndex(nextIndex)] = NULL_VALUE;
                }
                else
                {
                    int offset = currentDatOffset;

                    // store offset and length
                    int block = getMetaBlock(nextIndex);
                    int index = getMetaBlockIndex(nextIndex);
                    offsets[block][index] = offset;
                    lengths[block][index] = length;

                    // copy bytes to data array
                    int copied = 0;
                    if (spaceLeftOnBlock(offset) < length)
                    {
                        while (copied < length)
                        {
                            int copySize
                                = Math.Min(length - copied, spaceLeftOnBlock(offset + copied));
                            Array.Copy(bytes, copied,
                                             data[getBlock(offset + copied)],
                                             getBlockOffset(offset + copied),
                                             copySize);
                            copied += copySize;
                        }
                    }
                    else
                    {
                        Array.Copy(bytes, 0,
                                         data[getBlock(offset)],
                                         getBlockOffset(offset), length);
                    }
                    currentDatOffset += length;
                }
                nextIndex++;
            }

            private static int getBlock(int offset)
            {
                return offset >> blockLengthBits;
            }

            private static int getBlockOffset(int offset)
            {
                return offset & (blockLength - 1);
            }

            private static int spaceLeftOnBlock(int offset)
            {
                return blockLength - getBlockOffset(offset);
            }

            private static int getMetaBlock(int index)
            {
                return index >> metaBlockLengthBits;
            }

            private static int getMetaBlockIndex(int index)
            {
                return index & (metaBlockLength - 1);
            }

            private void allocateArrays()
            {
                logger.Debug($"DEBUG: Allocating arrays for BlockResultData {blockCount}");
                var sharedByte = ArrayPool<byte>.Shared;
                var sharedInt = ArrayPool<int>.Shared;
                while (data.Count < blockCount)
                {
                    data.Add(sharedByte.Rent(1 << blockLengthBits));
                }
                while (offsets.Count < metaBlockCount)
                {
                    offsets.Add(sharedInt.Rent(1 << metaBlockLengthBits));
                    lengths.Add(sharedInt.Rent(1 << metaBlockLengthBits));
                }
            }

        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.data.freeData();
            }
            base.Dispose(disposing);
        }
    }
}
