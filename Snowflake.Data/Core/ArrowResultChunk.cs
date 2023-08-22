/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Snowflake.Data.Core
{
    internal class ArrowResultChunk : IResultChunk
    {
        public RecordBatch RecordBatch { get; set; }

        private int _rowCount;
        private int _colCount;
        private int _chunkIndex;

        public ArrowResultChunk(RecordBatch recordBatch)
        {
            RecordBatch = recordBatch;
            
            _rowCount = recordBatch.Length;
            _colCount = recordBatch.ColumnCount;
            _chunkIndex = 0;
        }

        public UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            var column = RecordBatch.Column(columnIndex);

            string s;
            switch (column.Data.DataType.TypeId)
            {
                case ArrowTypeId.Int32: 
                    s = ((Int32Array)column).GetValue(rowIndex).ToString();
                    break;
                
                // TODO in SNOW-893834 -  other types
                
                default:
                    throw new NotImplementedException();
            }
            
            if (s == null)
                return null;
            
            return new UTF8Buffer(Encoding.UTF8.GetBytes(s));
        }
        
        public int GetRowCount()
        {
            return _rowCount;
        }

        public int GetChunkIndex()
        {
            return _chunkIndex;
        }
    }

}
