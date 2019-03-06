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

        public string ExtractCell(int rowIndex, int columnIndex)
        {
            return data.get(rowIndex * ColCount + columnIndex);
        }

        public void AddCell(string val)
        {
            data.add(val);
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

            private readonly List<char[]> data = new List<char[]>();
            private readonly List<int[]> offsets = new List<int[]>();
            private readonly List<int[]> lengths = new List<int[]>();
            private int nextIndex = 0;
            private int currentDatOffset = 0;

            internal BlockResultData()
            { }

            internal void Reset(int rowCount, int colCount, int uncompressedSize)
            {
                currentDatOffset = 0;
                nextIndex = 0;
                int bytesNeeded = uncompressedSize - (rowCount * 2) - (rowCount * colCount);

                this.blockCount = getBlock(bytesNeeded - 1) + 1;
                this.metaBlockCount = getMetaBlock(rowCount * colCount - 1) + 1;
            }

            public String get(int index)
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
                        char[] cell = new char[length];
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
                        return new String(cell);
                    }
                    else
                    {
                        return new String(data[getBlock(offset)], getBlockOffset(offset), length);
                    }
                }
            }

            public void add(String val)
            {
                if (data.Count < blockCount || offsets.Count < metaBlockCount)
                {
                    allocateArrays();
                }

                if (val == null)
                {
                    lengths[getMetaBlock(nextIndex)]
                        [getMetaBlockIndex(nextIndex)] = NULL_VALUE;
                }
                else
                {
                    int offset = currentDatOffset;
                    int length = val.Length;

                    // store offset and length
                    offsets[getMetaBlock(nextIndex)]
                        [getMetaBlockIndex(nextIndex)] = offset;
                    lengths[getMetaBlock(nextIndex)]
                        [getMetaBlockIndex(nextIndex)] = length;

                    // copy string to the char array
                    int copied = 0;
                    if (spaceLeftOnBlock(offset) < length)
                    {
                        char[] source = val.ToCharArray();
                        while (copied < length)
                        {
                            int copySize
                                = Math.Min(length - copied, spaceLeftOnBlock(offset + copied));
                            Array.Copy(source, copied,
                                             data[getBlock(offset + copied)],
                                             getBlockOffset(offset + copied),
                                             copySize);
                            copied += copySize;
                        }
                    }
                    else
                    {
                        Array.Copy(val.ToCharArray(), 0,
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
                    data.Add(new char[1 << blockLengthBits]);
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
