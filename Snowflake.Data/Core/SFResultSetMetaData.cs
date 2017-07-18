using System;
using System.Collections.Generic;
using System.Data;
using Common.Logging;

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
                        return indexCounter;
                    }
                    indexCounter++; 
                }
            }
            return -1;
        }

        internal SFDataType getColumnTypeByIndex(int targetIndex)
        {
            string sfDataTypeStr = rowTypes[targetIndex].type;
            return (SFDataType)Enum.Parse(typeof(SFDataType), sfDataTypeStr.ToUpper());
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
