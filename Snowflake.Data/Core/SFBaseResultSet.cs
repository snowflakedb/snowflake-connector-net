/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Snowflake.Data.Core
{
    abstract class SFBaseResultSet
    {
        private static readonly SFLogger Logger = SFLoggerFactory.GetLogger<SFBaseResultSet>();

        internal SFStatement sfStatement;

        internal SFResultSetMetaData sfResultSetMetaData;

        internal int columnCount;

        internal bool isClosed;

        internal Dictionary<string, long> timings = new Dictionary<string, long>();

        internal Dictionary<string, long> last100000Rowstimings = new Dictionary<string, long>();

        internal int fetchCount = 0;

        internal abstract bool Next();

        internal abstract Task<bool> NextAsync();

        protected abstract string getObjectInternal(int columnIndex);

        private SFDataConverter dataConverter;

        protected SFBaseResultSet()
        {
            dataConverter = new SFDataConverter();
        }

        internal T Measure<T>(string name, out long currentTime, Func<T> action)
        {
            if (Logger.IsDebugEnabled())
            {
                var start = Environment.TickCount;
                var result = action();
                var end = Environment.TickCount;

                currentTime = end - start;

                addTimesToTimingMaps(name, currentTime);
            
                return result;
            }
            else
            {
                // No op
                currentTime = -1;
                return action();
            }
        }
        private void addTimesToTimingMaps(string key, long value)
        {
            if (timings.TryGetValue(key, out long time))
            {
                timings[key] = value + time;
            }
            else
            {
                timings.Add(key, value);
            }

            if (last100000Rowstimings.TryGetValue(key, out long last100000Rowstime))
            {
                last100000Rowstimings[key] = last100000Rowstime + value;
            }
            else
            {
                last100000Rowstimings.Add(key, value);
            }
        }

        internal T GetValue<T>(int columnIndex)
        {
            string val = Measure(@"getObjectInternal", out long getObjectInternalTime, () =>
            {
                return getObjectInternal(columnIndex);
            }); 
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            var result = Measure(
                $"ConvertToCSharpVal(SF {types.Item1} -> C# {typeof(T)})", 
                out long convertToCSharpValTime, 
                () =>
                {
                    return (T)dataConverter.ConvertToCSharpVal(val, types.Item1, typeof(T));
                });

            // Total time for GetValue
            addTimesToTimingMaps($"GetValue<{typeof(T)}>", convertToCSharpValTime + getObjectInternalTime);

            return result;
        }

        internal string GetString(int columnIndex)
        {
            var type = sfResultSetMetaData.getColumnTypeByIndex(columnIndex);
            switch (type)
            {
                case SFDataType.DATE:
                    var val = GetValue(columnIndex, out long GetValueTime);
                    if (val == DBNull.Value)
                        return null;
                    var result = Measure(
                        @"SFDataConverter.toDateString", 
                        out long toDateStringTime,
                        () =>
                    {
                        return SFDataConverter.toDateString((DateTime)val,
                        sfResultSetMetaData.dateOutputFormat);
                    });

                    // Total time for GetString
                    addTimesToTimingMaps("GetString", GetValueTime + toDateStringTime);

                    return result;
                //TODO: Implement SqlFormat for timestamp type, aka parsing format specified by user and format the value
                default:
                    return Measure(@"getObjectInternal", out long getObjectInternalTime, () =>
                    {
                        return getObjectInternal(columnIndex);
                    });
            }
        }

        internal object GetValue(int columnIndex, out long time)
        {
            string val = Measure(@"getObjectInternal", out long getObjectInternalTime, () =>
            {
                return getObjectInternal(columnIndex);
            });
            var types = sfResultSetMetaData.GetTypesByIndex(columnIndex);
            var result = Measure(
                $"ConvertToCSharpVal(SF {types.Item1} -> C# {types.Item2})",
                out long convertToCSharpValTime,
                () =>
            {
                return dataConverter.ConvertToCSharpVal(val, types.Item1, types.Item2);
            });

            time = convertToCSharpValTime + getObjectInternalTime;

            // Total time for GetValue
            addTimesToTimingMaps("GetValue", time);

            return result;
        }

        internal object GetValue(int columnIndex)
        {
            return GetValue(columnIndex, out long time);
        }

        internal void close()
        {
            isClosed = true;
        }
        
        internal void LogAverageConversionTimesForLast100000rows()
        {
            Logger.Debug("Average conversion time for the last 100000");
            foreach (KeyValuePair<string, long> kvp in last100000Rowstimings)
            {
                if (!kvp.Key.Equals("nextChunk"))
                {
                    Logger.Debug($"{kvp.Key} : {kvp.Value / 100000.0} ms");
                    last100000Rowstimings = new Dictionary<string, long>();
                }
            }
        }

        internal void LogAverageConversionTimes()
        {
            Logger.Debug("---- Time spent on data conversion operations ----");
            foreach (KeyValuePair<string, long> kvp in timings)
            {
                if (!kvp.Key.Equals("nextChunk"))
                {
                    Logger.Debug($"Total time for {kvp.Key} : {kvp.Value} ms");
                    Logger.Debug($"Average time for {kvp.Key} : {kvp.Value / (float)fetchCount} ms\n");
                }
            }
        }
    }
}
