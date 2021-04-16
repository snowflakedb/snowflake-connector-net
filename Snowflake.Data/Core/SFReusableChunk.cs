/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core
{
    class SFReusableChunk : IResultChunk
    {
       
        public int RowCount { get; set; }

        public int ColCount { get; set; }

        public string Url { get; set; }

        public int chunkIndexToDownload { get; set; }

        private readonly BlockResultData data;

        internal SFReusableChunk(int colCount)
        {
            ColCount = colCount;
            data = new BlockResultData();
        }

        internal void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            this.RowCount = chunkInfo.rowCount;
            this.Url = chunkInfo.url;
            this.chunkIndexToDownload = chunkIndex;
            data.Reset(this.RowCount, this.ColCount, chunkInfo.uncompressedSize);
        }

        public int GetRowCount()
        {
            return RowCount;
        }

        public int GetChunkIndex()
        {
            return chunkIndexToDownload;
        }

        public UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            return data.get(rowIndex * ColCount + columnIndex);
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

        private class BlockResultData
        {
            private static readonly int NULL_VALUE = -100;
            private int blockCount;

            private static int blockLengthBits = 24;
            private static int blockLength = 1 << blockLengthBits;
            int metaBlockCount;
            private static int metaBlockLengthBits = 15;
            private static int metaBlockLength = 1 << metaBlockLengthBits;

            private readonly List<byte[]> data = new List<byte[]>();
            private readonly List<int[]> offsets = new List<int[]>();
            private readonly List<int[]> lengths = new List<int[]>();
            private int nextIndex = 0;
            private int currentDatOffset = 0;

            int savedRowCount;
            int savedColCount;

            internal BlockResultData()
            { }

            internal void Reset(int rowCount, int colCount, int uncompressedSize)
            {
                savedRowCount = rowCount;
                savedColCount = colCount;
                currentDatOffset = 0;
                nextIndex = 0;
                int bytesNeeded = uncompressedSize - (rowCount * 2) - (rowCount * colCount);
                this.blockCount = getBlock(bytesNeeded - 1) + 1;
                this.metaBlockCount = getMetaBlock(rowCount * colCount - 1) + 1;
            }

            public UTF8Buffer get(int index)
            {
                int length = lengths[getMetaBlock(index)]
                    [getMetaBlockIndex(index)];
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
                while (data.Count < blockCount)
                {
                    data.Add(new byte[1 << blockLengthBits]);
                }
                while (offsets.Count < metaBlockCount)
                {
                    offsets.Add(new int[1 << metaBlockLengthBits]);
                    lengths.Add(new int[1 << metaBlockLengthBits]);
                }
            }
        }
    }
}
