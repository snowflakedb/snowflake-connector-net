/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Snowflake.Data.Core
{
    internal class ArrowResultChunk : BaseResultChunk
    {
        internal override ResultFormat Format => ResultFormat.ARROW;

        public List<RecordBatch> RecordBatch { get; set; }

        private int _currentBatchIndex = 0;
        private int _currentRecordIndex = -1;
        
        public ArrowResultChunk(RecordBatch recordBatch)
        {
            RecordBatch = new List<RecordBatch>{recordBatch};
            
            RowCount = recordBatch.Length;
            ColumnCount = recordBatch.ColumnCount;
            ChunkIndex = 0;
        }

        public ArrowResultChunk(int columnCount)
        {
            RecordBatch = new List<RecordBatch>();

            RowCount = 0;
            ColumnCount = columnCount;
            ChunkIndex = 0;
        }
        
        public void AddRecordBatch(RecordBatch recordBatch)
        {
            RecordBatch.Add(recordBatch);
        }

        internal override void Reset(ExecResponseChunk chunkInfo, int chunkIndex)
        {
            base.Reset(chunkInfo, chunkIndex);

            _currentBatchIndex = 0;
            _currentRecordIndex = -1;
            RecordBatch.Clear();
        }

        internal override bool Next()
        {
            _currentRecordIndex += 1;
            if (_currentRecordIndex < RecordBatch[_currentBatchIndex].Length)
                return true;
            
            _currentBatchIndex += 1;
            _currentRecordIndex = 0;

            return _currentBatchIndex < RecordBatch.Count;
        }

        internal override bool Rewind()
        {
            _currentRecordIndex -= 1;
            if (_currentRecordIndex >= 0)
                return true;
            
            _currentBatchIndex -= 1;

            if (_currentBatchIndex >= 0)
            {
                _currentRecordIndex = RecordBatch[_currentBatchIndex].Length - 1;
                return true;
            }

            return false;
        }
        
        public override UTF8Buffer ExtractCell(int rowIndex, int columnIndex)
        {
            _currentBatchIndex = 0;
            _currentRecordIndex = rowIndex;
            while (_currentRecordIndex >= RecordBatch[_currentBatchIndex].Length)
            {
                _currentRecordIndex -= RecordBatch[_currentBatchIndex].Length;
                _currentBatchIndex += 1;
            }

            return ExtractCell(columnIndex);
        }

        public override UTF8Buffer ExtractCell(int columnIndex)
        {
            var column = RecordBatch[_currentBatchIndex].Column(columnIndex);

            string stringBuffer;
            switch (column.Data.DataType.TypeId)
            {
                case ArrowTypeId.Int32: 
                    stringBuffer = ((Int32Array)column).GetValue(_currentRecordIndex).ToString();
                    break;
                
                // TODO in SNOW-893834 -  other types
                
                default:
                    throw new NotImplementedException();
            }
            
            if (stringBuffer == null)
                return null;
            
            return new UTF8Buffer(Encoding.UTF8.GetBytes(stringBuffer));
        }
    }

}
