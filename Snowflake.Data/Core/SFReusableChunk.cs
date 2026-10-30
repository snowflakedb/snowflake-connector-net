using System;
using System.Collections.Generic;
using System.Text;

namespace Snowflake.Data.Core;

class SFReusableChunk : BaseResultChunk
{
    internal override ResultFormat ResultFormat => ResultFormat.JSON;

    internal readonly BlockResultData data;

    private int _currentRowIndex = -1;

    internal SFReusableChunk(int columnCount)
    {
        ColumnCount = columnCount;
        data = new BlockResultData();
    }

    internal override void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
    {
        base.Reset(chunkInfo, chunkIndex);
        _currentRowIndex = -1;
        data.Reset(RowCount, ColumnCount, chunkInfo.uncompressedSize);
    }

    internal override void Clear()
    {
        base.Clear();
        _currentRowIndex = -1;
        data.Clear();
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
        return data.Get(_currentRowIndex * ColumnCount + columnIndex);
    }

    public void AddCell(string val)
    {
        // This method should not be used - we want to avoid unnecessary conversions between string and bytes
        throw new NotImplementedException();
    }

    public void AddCell(byte[] bytes, int length)
    {
        data.Add(bytes, length);
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

    internal class BlockResultData
    {
        private static readonly int NULL_VALUE = -100;

        internal int blockCount;
        private static readonly int blockLengthBits = 23;
        private static readonly int blockLength = 1 << blockLengthBits;

        internal int metaBlockCount;
        private static readonly int metaBlockLengthBits = 15;
        private static readonly int metaBlockLength = 1 << metaBlockLengthBits;

        private readonly List<byte[]> data = new();
        private readonly List<int[]> offsets = new();
        private readonly List<int[]> lengths = new();
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
            blockCount = 1; // init with 1 block only
            metaBlockCount = GetMetaBlock(rowCount * colCount - 1) + 1;
        }

        internal void Clear()
        {
            savedRowCount = 0;
            savedColCount = 0;
            currentDatOffset = 0;
            nextIndex = 0;
            blockCount = 0;
            metaBlockCount = 0;

            data.Clear();
            offsets.Clear();
            lengths.Clear();
        }

        internal void ResetForRetry()
        {
            currentDatOffset = 0;
            nextIndex = 0;
        }

        public UTF8Buffer Get(int index)
        {
            int block = GetMetaBlock(index);
            int blockIndex = GetMetaBlockIndex(index);

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
                int offset = offsets[GetMetaBlock(index)]
                    [GetMetaBlockIndex(index)];

                // Create string from the char arrays
                if (SpaceLeftOnBlock(offset) < length)
                {
                    int copied = 0;
                    byte[] cell = new byte[length];
                    while (copied < length)
                    {
                        int copySize
                            = Math.Min(length - copied, SpaceLeftOnBlock(offset + copied));
                        Array.Copy(data[GetBlock(offset + copied)],
                            GetBlockOffset(offset + copied),
                            cell, copied,
                            copySize);

                        copied += copySize;
                    }
                    return new UTF8Buffer(cell);
                }
                else
                {
                    return new UTF8Buffer(data[GetBlock(offset)], GetBlockOffset(offset), length);
                }
            }
        }

        public void Add(byte[] bytes, int length)
        {
            // check if a new block for data is needed
            if (GetBlock(currentDatOffset) == blockCount - 1)
            {
                var neededSize = length - SpaceLeftOnBlock(currentDatOffset);
                while (neededSize >= 0)
                {
                    blockCount++;
                    neededSize -= blockLength;
                }
            }
            if (data.Count < blockCount || offsets.Count < metaBlockCount)
            {
                AllocateArrays();
            }

            if (bytes == null)
            {
                lengths[GetMetaBlock(nextIndex)]
                    [GetMetaBlockIndex(nextIndex)] = NULL_VALUE;
            }
            else
            {
                int offset = currentDatOffset;

                // store offset and length
                int block = GetMetaBlock(nextIndex);
                int index = GetMetaBlockIndex(nextIndex);
                offsets[block][index] = offset;
                lengths[block][index] = length;

                // copy bytes to data array
                int copied = 0;
                if (SpaceLeftOnBlock(offset) < length)
                {
                    while (copied < length)
                    {
                        int copySize
                            = Math.Min(length - copied, SpaceLeftOnBlock(offset + copied));
                        Array.Copy(bytes, copied,
                            data[GetBlock(offset + copied)],
                            GetBlockOffset(offset + copied),
                            copySize);
                        copied += copySize;
                    }
                }
                else
                {
                    Array.Copy(bytes, 0,
                        data[GetBlock(offset)],
                        GetBlockOffset(offset), length);
                }
                currentDatOffset += length;
            }
            nextIndex++;
        }

        private static int GetBlock(int offset)
        {
            return offset >> blockLengthBits;
        }

        private static int GetBlockOffset(int offset)
        {
            return offset & (blockLength - 1);
        }

        private static int SpaceLeftOnBlock(int offset)
        {
            return blockLength - GetBlockOffset(offset);
        }

        private static int GetMetaBlock(int index)
        {
            return index >> metaBlockLengthBits;
        }

        private static int GetMetaBlockIndex(int index)
        {
            return index & (metaBlockLength - 1);
        }

        private void AllocateArrays()
        {
            while (data.Count < blockCount)
            {
                data.Add(new byte[blockLength]);
            }
            while (offsets.Count < metaBlockCount)
            {
                offsets.Add(new int[metaBlockLength]);
                lengths.Add(new int[metaBlockLength]);
            }
        }
    }
}
