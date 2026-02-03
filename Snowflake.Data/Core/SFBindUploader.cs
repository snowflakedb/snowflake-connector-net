using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class SFBindUploader
    {
        private const string STAGE_NAME = "SYSTEM$BIND";
        private const string CREATE_STAGE_STMT = "CREATE OR REPLACE TEMPORARY STAGE "
            + STAGE_NAME
            + " file_format=("
            + " type=csv"
            + " field_optionally_enclosed_by='\"'"
            + ")";

        private const string PUT_STATEMENT =
            "PUT file://{0} '{1}' overwrite=true auto_compress=false source_compression=gzip";

        private const string ACCOUNT_GCP = "gcp";
        private const string ACCOUNT_AZURE = "azure";
        private const string ACCOUNT_LOCAL = "localhost";

        private static long inputStreamBufferSize = 1024 * 1024 * 10;

        private int fileCount = 0;

        private SFSession session;

        private string requestId;

        private SFLogger logger = SFLoggerFactory.GetLogger<SFBindUploader>();

        private string stagePath;

        public SFBindUploader(SFSession session, string requestId)
        {
            this.session = session;
            this.requestId = requestId;
            this.stagePath = "@" + STAGE_NAME + "/" + requestId;
        }

        public string getStagePath()
        {
            return this.stagePath;
        }

        public void Upload(Dictionary<string, BindingDTO> bindings)
        {
            if (bindings == null)
            {
                return;
            }

            List<string> dataRows = new List<string>();
            CreateDataRows(ref dataRows, bindings);

            int startIndex = 0;
            int rowNum = 0;
            int curBytes = 0;

            while (rowNum < dataRows.Count)
            {
                while (curBytes < inputStreamBufferSize && rowNum < dataRows.Count)
                {
                    curBytes += dataRows[rowNum].Length;
                    rowNum++;
                }

                StringBuilder sBuffer = new StringBuilder();
                MemoryStream ms = new MemoryStream();
                using (StreamWriter tw = new StreamWriter(ms))
                {

                    for (int i = startIndex; i < rowNum; i++)
                    {
                        sBuffer.Append(dataRows[i]);
                    }

                    tw.Write(sBuffer.ToString());
                    tw.Flush();

                    try
                    {
                        string fileName = (++fileCount).ToString();
                        UploadStream(ref ms, fileName);
                        startIndex = rowNum;
                        curBytes = 0;
                    }
                    catch (IOException e)
                    {
                        // failure using stream put
                        throw new Exception("file stream upload error." + e.ToString());
                    }
                }
            }
        }

        internal async Task UploadAsync(Dictionary<string, BindingDTO> bindings, CancellationToken cancellationToken)
        {
            if (bindings == null)
            {
                return;
            }

            List<string> dataRows = new List<string>();
            CreateDataRows(ref dataRows, bindings);

            int startIndex = 0;
            int rowNum = 0;
            int curBytes = 0;

            while (rowNum < dataRows.Count)
            {
                while (curBytes < inputStreamBufferSize && rowNum < dataRows.Count)
                {
                    curBytes += dataRows[rowNum].Length;
                    rowNum++;
                }

                StringBuilder sBuffer = new StringBuilder();
                MemoryStream ms = new MemoryStream();
                using (StreamWriter tw = new StreamWriter(ms))
                {
                    for (int i = startIndex; i < rowNum; i++)
                    {
                        sBuffer.Append(dataRows[i]);
                    }

                    tw.Write(sBuffer.ToString());
                    tw.Flush();

                    try
                    {
                        string fileName = (++fileCount).ToString();
                        await UploadStreamAsync(ms, fileName, cancellationToken).ConfigureAwait(false);
                        startIndex = rowNum;
                        curBytes = 0;
                    }
                    catch (IOException e)
                    {
                        // failure using stream put
                        throw new Exception("file stream upload error." + e.ToString());
                    }
                }
            }
        }

        private void CreateDataRows(ref List<string> dataRows, Dictionary<string, BindingDTO> bindings)
        {
            List<BindingDTO> arrbinds = bindings.Values.ToList();
            List<List<object>> bindList = new List<List<object>>();
            List<string> types = new List<string>(); // for the binding types
            dataRows = new List<string>(); // for the converted data string
            int rowSize = ((List<object>)arrbinds[0].value).Count;
            int paramSize = arrbinds.Count;

            foreach (BindingDTO bind in arrbinds)
            {
                List<object> values = (List<object>)bind.value;
                types.Add(bind.type);
                if (values.Count != rowSize)
                {
                    //throw here different row size
                }
                bindList.Add(values);
            }

            for (int i = 0; i < rowSize; i++)
            {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < paramSize; j++)
                {
                    if (j > 0)
                    {
                        sb.Append(',');
                    }

                    sb.Append(GetCSVData(types[j], (string)bindList[j][i]));
                }
                sb.Append('\n');
                dataRows.Add(sb.ToString());
            }
        }

        private void UploadStream(ref MemoryStream stream, string destFileName)
        {
            CreateStage();
            string stageName = this.stagePath;
            if (stageName == null)
            {
                throw new Exception("Stage name is null.");
            }
            if (destFileName == null)
            {
                throw new Exception("file name is null.");
            }

            string putStmt = string.Format(PUT_STATEMENT, destFileName, stageName);

            SFStatement statement = new SFStatement(session);
            statement.SetUploadStream(stream, destFileName, stagePath);
            statement.ExecuteTransfer(putStmt);

        }

        internal async Task UploadStreamAsync(MemoryStream stream, string destFileName, CancellationToken cancellationToken)
        {
            await CreateStageAsync(cancellationToken).ConfigureAwait(false);
            string stageName = this.stagePath;
            if (stageName == null)
            {
                throw new Exception("Stage name is null.");
            }
            if (destFileName == null)
            {
                throw new Exception("file name is null.");
            }

            string putStmt = string.Format(PUT_STATEMENT, destFileName, stageName);

            SFStatement statement = new SFStatement(session);
            statement.SetUploadStream(stream, destFileName, stagePath);
            await statement.ExecuteTransferAsync(putStmt, cancellationToken).ConfigureAwait(false);
        }

        internal string GetCSVData(string sType, string sValue)
        {
            if (sValue == null)
                return sValue;

            DateTime epoch = SFDataConverter.UnixEpoch;
            switch (sType)
            {
                case "TEXT":
                    if (sValue == null)
                        return "";
                    if (sValue == "")
                        return "\"\"";
                    if (sValue.IndexOf('"') >= 0
                        || sValue.IndexOf(',') >= 0
                        || sValue.IndexOf('\\') >= 0
                        || sValue.IndexOf('\n') >= 0
                        || sValue.IndexOf('\t') >= 0)
                        return '"' + sValue.Replace("\"", "\"\"") + '"';
                    return sValue;
                case "DATE":
                    long msFromEpoch = long.Parse(sValue); // SFDateConverter.CSharpValToSfVal provides in [ms] from Epoch
                    DateTime date = epoch.AddMilliseconds(msFromEpoch);
                    return date.ToShortDateString();
                case "TIME":
                    long nsSinceMidnight = long.Parse(sValue); // SFDateConverter.CSharpValToSfVal provides in [ns] from Midnight
                    DateTime time = epoch.AddTicks(nsSinceMidnight / 100);
                    return time.ToString("HH:mm:ss.fffffff");
                case "TIMESTAMP_LTZ":
                    long ticksFromEpochLtz =
                        long.TryParse(sValue, out var nsLtz)
                            ? nsLtz / 100
                            : (long)(decimal.Parse(sValue) / 100);

                    var utcDateTimeOffset = new DateTimeOffset(epoch.AddTicks(ticksFromEpochLtz), TimeSpan.Zero);
                    var sessionTimezone = session?.GetSessionTimezone() ?? TimeZoneInfo.Local;
                    // Use ToLocalTime() for local timezone to maintain exact backward compatibility
                    if (sessionTimezone.Equals(TimeZoneInfo.Local))
                    {
                        return utcDateTimeOffset.ToLocalTime().ToString("O"); // ISO 8601 format
                    }
                    DateTime ltz = TimeZoneInfo.ConvertTimeFromUtc(utcDateTimeOffset.UtcDateTime, sessionTimezone);
                    return new DateTimeOffset(ltz, sessionTimezone.GetUtcOffset(ltz)).ToString("O"); // ISO 8601 format
                case "TIMESTAMP_NTZ":
                    long ticksFromEpochNtz =
                        long.TryParse(sValue, out var nsNtz)
                            ? nsNtz / 100
                            : (long)(decimal.Parse(sValue) / 100);

                    DateTime ntz = epoch.AddTicks(ticksFromEpochNtz);
                    return ntz.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
                case "TIMESTAMP_TZ":
                    string[] tstzString = sValue.Split(' ');
                    long ticksFromEpochTz =
                        long.TryParse(tstzString[0], out var nsTz)
                            ? nsTz / 100
                            : (long)(decimal.Parse(tstzString[0]) / 100);

                    int timeZoneOffset = int.Parse(tstzString[1]) - 1440; // SFDateConverter provides in minutes increased by 1440m
                    DateTime timestamp = epoch.AddTicks(ticksFromEpochTz).AddMinutes(timeZoneOffset);
                    TimeSpan offset = TimeSpan.FromMinutes(timeZoneOffset);
                    DateTimeOffset tzDateTimeOffset = new DateTimeOffset(timestamp.Ticks, offset);
                    return tzDateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz");
            }
            return sValue;
        }

        private void CreateStage()
        {
            if (session.GetArrayBindStage() != null)
            {
                return;
            }
            lock (session)
            {
                if (session.GetArrayBindStage() == null)
                {
                    try
                    {
                        SFStatement statement = new SFStatement(session);
                        SFBaseResultSet resultSet = statement.Execute(0, CREATE_STAGE_STMT, null, false, false);
                        session.SetArrayBindStage(STAGE_NAME);
                    }
                    catch (Exception e)
                    {
                        session.SetArrayBindStageThreshold(0);
                        logger.Error("Failed to create temporary stage for array binds.", e);
                        throw;
                    }
                }
            }
        }

        internal async Task CreateStageAsync(CancellationToken cancellationToken)
        {
            if (session.GetArrayBindStage() != null)
            {
                return;
            }
            if (session.GetArrayBindStage() == null)
            {
                try
                {
                    SFStatement statement = new SFStatement(session);
                    var resultSet = await statement.ExecuteAsync(0, CREATE_STAGE_STMT, null, false, false, cancellationToken).ConfigureAwait(false);
                    session.SetArrayBindStage(STAGE_NAME);
                }
                catch (Exception e)
                {
                    session.SetArrayBindStageThreshold(0);
                    logger.Error("Failed to create temporary stage for array binds.", e);
                    throw;
                }
            }
        }
    }
}
