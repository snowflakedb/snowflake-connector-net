using System;
using System.Collections.Generic;
using System.Data;
using Snowflake.Data.Log;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    class SFResultSetMetaData
    {
        static private readonly SFLogger logger = SFLoggerFactory.GetLogger<SFResultSetMetaData>();

        private int columnCount;

        internal readonly string dateOutputFormat;

        internal readonly string timeOutputFormat;

        internal readonly string timestampeNTZOutputFormat;

        internal readonly string timestampeLTZOutputFormat;

        internal readonly string timestampeTZOutputFormat;

        internal List<ExecResponseRowType> rowTypes;

        internal readonly SFStatementType statementType;

        internal readonly List<Tuple<SFDataType, Type>> columnTypes;

        /// <summary>
        ///     This map is used to cache column name to column index. Index is 0-based.
        /// </summary>
        private Dictionary<string, int> columnNameToIndexCache = new Dictionary<string, int>();

        internal SFResultSetMetaData(QueryExecResponseData queryExecResponseData, SFSession session)
        {
            rowTypes = queryExecResponseData.rowType;
            columnCount = rowTypes.Count;
            statementType = FindStatementTypeById(queryExecResponseData.statementTypeId);
            columnTypes = InitColumnTypes();

            if (session.ParameterMap.ContainsKey(SFSessionParameter.DATE_OUTPUT_FORMAT))
            {
                dateOutputFormat = session.ParameterMap[SFSessionParameter.DATE_OUTPUT_FORMAT].ToString();
            }
            if (session.ParameterMap.ContainsKey(SFSessionParameter.TIME_OUTPUT_FORMAT))
            {
                timeOutputFormat = session.ParameterMap[SFSessionParameter.TIME_OUTPUT_FORMAT].ToString();
            }
        }

        internal SFResultSetMetaData(PutGetResponseData putGetResponseData)
        {
            rowTypes = putGetResponseData.rowType;
            columnCount = rowTypes.Count;
            statementType = FindStatementTypeById(putGetResponseData.statementTypeId);
            columnTypes = InitColumnTypes();
        }

        private List<Tuple<SFDataType, Type>> InitColumnTypes()
        {
            List<Tuple<SFDataType, Type>> types = new List<Tuple<SFDataType, Type>>();
            for (int i = 0; i < columnCount; i++)
            {
                var column = rowTypes[i];
                var dataType = GetSFDataType(column.type);
                var nativeType = GetNativeTypeForColumn(dataType, column);

                types.Add(Tuple.Create(dataType, nativeType));
            }
            return types;
        }

        /// <summary>
        /// </summary>
        /// <returns>index of column given a name, -1 if no column names are found</returns>
        internal int GetColumnIndexByName(string targetColumnName)
        {
            if (columnNameToIndexCache.TryGetValue(targetColumnName, out var resultIndex))
            {
                return resultIndex;
            }
            else
            {
                int indexCounter = 0;
                foreach (ExecResponseRowType rowType in rowTypes)
                {
                    if (String.Compare(rowType.name, targetColumnName, false) == 0)
                    {
                        logger.Info($"Found column name {targetColumnName} under index {indexCounter}");
                        columnNameToIndexCache[targetColumnName] = indexCounter;
                        return indexCounter;
                    }
                    indexCounter++;
                }
            }
            return -1;
        }

        internal SFDataType GetColumnTypeByIndex(int targetIndex)
        {
            return columnTypes[targetIndex].Item1;
        }

        internal Tuple<SFDataType, Type> GetTypesByIndex(int targetIndex)
        {
            return columnTypes[targetIndex];
        }

        internal long GetScaleByIndex(int targetIndex)
        {
            return rowTypes[targetIndex].scale;
        }

        internal bool IsStructuredType(int targetIndex)
        {
            var fields = rowTypes[targetIndex].fields;
            return fields != null && fields.Count > 0;
        }

        private SFDataType GetSFDataType(string type)
        {
            SFDataType rslt;
            if (Enum.TryParse(type, true, out rslt))
                return rslt;

            throw new SnowflakeDbException(SFError.INTERNAL_ERROR,
                $"Unknown column type: {type}");
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
                case SFDataType.ARRAY:
                case SFDataType.VECTOR:
                case SFDataType.MAP:
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
                        $"Unknown column type: {sfType}");
            }
        }

        internal Type GetCSharpTypeByIndex(int targetIndex)
        {
            return columnTypes[targetIndex].Item2;
        }

        internal string GetColumnNameByIndex(int targetIndex)
        {
            return rowTypes[targetIndex].name;
        }

        private SFStatementType FindStatementTypeById(long id)
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

        [SFStatementTypeAttr(typeId = 0x2000)]
        EXPLAIN,

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
        [SFStatementTypeAttr(typeId = 0x3000 + 0x700)]
        COPY_UNLOAD,

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
        [SFStatementTypeAttr(typeId = 0x4000 + 0x700 + 0x01)]
        LIST_FILES,

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

        /// <remark>
        ///     Stage Operations
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x7000)]
        STAGE_FILE_OPERATIONS,
        [SFStatementTypeAttr(typeId = 0x7000 + 0x100 + 0x01)]
        GET_FILES,
        [SFStatementTypeAttr(typeId = 0x7000 + 0x100 + 0x02)]
        PUT_FILES,
        [SFStatementTypeAttr(typeId = 0x7000 + 0x100 + 0x03)]
        REMOVE_FILES,

        /// <remark>
        ///     Misc Query types
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x8000)]
        MISC_QUERY_TYPES,
        [SFStatementTypeAttr(typeId = 0x8000 + 0x100 + 0x01)]
        BEGIN,
        [SFStatementTypeAttr(typeId = 0x8000 + 0x100 + 0x02)]
        END,
        [SFStatementTypeAttr(typeId = 0x8000 + 0x100 + 0x03)]
        COMMIT,
        [SFStatementTypeAttr(typeId = 0x8000 + 0x100 + 0x04)]
        SET,

        /// <remark>
        ///     Procedure Call
        /// </remark>
        [SFStatementTypeAttr(typeId = 0x9000)]
        CALL,
    }

    class SFStatementTypeAttr : Attribute
    {
        public long typeId { get; set; }
    }
}
