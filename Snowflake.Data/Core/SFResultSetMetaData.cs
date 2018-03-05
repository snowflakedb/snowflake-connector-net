/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Data;
using Common.Logging;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    class SFResultSetMetaData
    {
        static private readonly ILog logger = LogManager.GetLogger<SFResultSetMetaData>();

        private int columnCount;

        internal readonly string dateOutputFormat;

        internal readonly string timeOutputFormat;

        internal readonly string timestampeNTZOutputFormat;

        internal readonly string timestampeLTZOutputFormat;

        internal readonly string timestampeTZOutputFormat;

        List<ExecResponseRowType> rowTypes;

        internal readonly SFStatementType statementType;  
        
        /// <summary>
        ///     This map is used to cache column name to column index. Index is 0-based.
        /// </summary>
        private Dictionary<string, int> columnNameToIndexCache = new Dictionary<string, int>();

        internal SFResultSetMetaData(QueryExecResponseData queryExecResponseData)
        {
            rowTypes = queryExecResponseData.rowType;
            columnCount = rowTypes.Count;
            statementType = findStatementTypeById(queryExecResponseData.statementTypeId);
            
            foreach (NameValueParameter parameter in queryExecResponseData.parameters)
            {
                switch(parameter.name)
                {
                    case "DATE_OUTPUT_FORMAT":
                        dateOutputFormat = parameter.value;
                        break;
                    case "TIME_OUTPUT_FORMAT":
                        timeOutputFormat = parameter.value;
                        break;
                }
            }
        }

        /// <summary>
        /// </summary>
        /// <returns>index of column given a name, -1 if no column names are found</returns>
        internal int getColumnIndexByName(string targetColumnName)
        {
            int resultIndex;
            if (columnNameToIndexCache.TryGetValue(targetColumnName, out resultIndex))
            {
                return resultIndex;
            }
            else
            {
                int indexCounter = 0;
                foreach (ExecResponseRowType rowType in rowTypes)
                {
                    if (String.Compare(rowType.name, targetColumnName, false ) == 0 )
                    {
                        logger.DebugFormat("Found colun name {0} under index {1}", targetColumnName, indexCounter);
                        columnNameToIndexCache[targetColumnName] = indexCounter;
                        return indexCounter;
                    }
                    indexCounter++; 
                }
            }
            return -1;
        }
        
        internal SFDataType getColumnTypeByIndex(int targetIndex)
        {
            if (targetIndex < 0 || targetIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, targetIndex);
            }

            return GetSFDataType(rowTypes[targetIndex].type);
        }

        internal Tuple<SFDataType, Type> GetTypesByIndex(int targetIndex)
        {
            if (targetIndex < 0 || targetIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, targetIndex);
            }

            var column = rowTypes[targetIndex];
            var dataType = GetSFDataType(column.type);
            var nativeType = GetNativeTypeForColumn(dataType, column);

            return Tuple.Create(dataType, nativeType);
        }

        private SFDataType GetSFDataType(string type)
        {
            SFDataType rslt;
            if (Enum.TryParse(type, true, out rslt))
                return rslt;

            throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                $"Unknow column type: {type}"); 
        }

        private Type GetNativeTypeForColumn(SFDataType sfType, ExecResponseRowType col)
        {
            switch (sfType)
            {
                case SFDataType.FIXED:
                    return col.scale == 0 ? typeof(long) : typeof(decimal);
                case SFDataType.REAL:
                    return typeof(double);
                case SFDataType.TEXT:
                case SFDataType.VARIANT:
                case SFDataType.OBJECT:
                    return typeof(string);
                case SFDataType.DATE:
                case SFDataType.TIME:
                case SFDataType.TIMESTAMP_NTZ:
                    return typeof(DateTime);
                case SFDataType.TIMESTAMP_LTZ:
                case SFDataType.TIMESTAMP_TZ:
                    return typeof(DateTimeOffset);
                case SFDataType.BINARY:
                    return typeof(byte[]);
                case SFDataType.BOOLEAN:
                    return typeof(bool);
                default:
                    throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                        $"Unknow column type: {sfType}");
            }
        }
        
        internal Type getCSharpTypeByIndex(int targetIndex)
        {
            if (targetIndex < 0 || targetIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, targetIndex);
            }

            SFDataType sfType = getColumnTypeByIndex(targetIndex);
            return GetNativeTypeForColumn(sfType, rowTypes[targetIndex]);  
        }

        internal string getColumnNameByIndex(int targetIndex)
        {
            if (targetIndex < 0 || targetIndex >= columnCount)
            {
                throw new SnowflakeDbException(SFError.COLUMN_INDEX_OUT_OF_BOUND, targetIndex);
            }

            return rowTypes[targetIndex].name;
        }

        internal DataTable toDataTable()
        {
            return null;
        }

        private SFStatementType findStatementTypeById(long id)
        {
            foreach (SFStatementType type in Enum.GetValues(typeof(SFStatementType)))
            {
                if (id == type.GetAttribute<SFStatementTypeAttr>().typeId)
                {
                    return type;
                }
            }

            // if specific type not found, we will try to find the range
            if (id >= SFStatementType.SCL.GetAttribute<SFStatementTypeAttr>().typeId &&
                id < SFStatementType.SCL.GetAttribute<SFStatementTypeAttr>().typeId + 0x1000)
            {
                return SFStatementType.SCL;
            }
            else if (id >= SFStatementType.TCL.GetAttribute<SFStatementTypeAttr>().typeId &&
                id < SFStatementType.TCL.GetAttribute<SFStatementTypeAttr>().typeId + 0x1000)
            {
                return SFStatementType.TCL;
            }
            else if (id >= SFStatementType.DDL.GetAttribute<SFStatementTypeAttr>().typeId &&
                id < SFStatementType.DDL.GetAttribute<SFStatementTypeAttr>().typeId + 0x1000)
            {
                return SFStatementType.DDL;
            }
            else
            {
                return SFStatementType.UNKNOWN;
            }
        }
    }

    internal enum SFStatementType
    {
        [SFStatementTypeAttr(typeId = 0x0000)]
        UNKNOWN, 

        [SFStatementTypeAttr(typeId = 0x1000)]
        SELECT,
        
        /// <remark>
        ///     Data Manipulation Language 
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x3000)]
        DML,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x100)]
        INSERT,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x200)]
        UPDATE,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x300)]
        DELETE,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x400)]
        MERGE,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x500)]
        MULTI_INSERT,
        [SFStatementTypeAttr(typeId = 0x3000 + 0x600)]
        COPY,

        /// <remark>
        ///     System Command Language
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x4000)]
        SCL,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x100)]
        ALTER_SESSION,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x300)]
        USE,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x300 + 0x10)]
        USE_DATABASE,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x300 + 0x20)]
        USE_SCHEMA,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x300 + 0x30)]
        USE_WAREHOUSE,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x400)]
        SHOW,
        [SFStatementTypeAttr(typeId = 0x4000 + 0x500)]
        DESCRIBE,

        /// <remark>
        ///     Transaction Command Language
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x5000)]
        TCL, 

        /// <remark>
        ///     Data Definition Language
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x6000)]
        DDL,
    }

    class SFStatementTypeAttr : Attribute
    {
        public long typeId { get; set; }
    }
}
