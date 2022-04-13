using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    class SFBindUploader
    {
        private const string STAGE_NAME = "SYSTEM$BIND";
        private const string CREATE_STAGE_STMT = "CREATE TEMPORARY STAGE "
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

        private static long inputStreamBufferSize = 1024 * 1024 * 100;

        private Boolean closed = false;

        private int fileCount = 0;

        private SFSession session;

        private string requestId;

        private SFLogger logger = SFLoggerFactory.GetLogger<SFBindUploader>();

        private string stagePath;

        private string locationType;

        public SFBindUploader(SFSession session, string requestId)
        {
            this.session = session;
            this.requestId = requestId;
            this.closed = false;
            this.stagePath = "@" + STAGE_NAME + "/" + requestId;
        }

        public string getStagePath()
        {
            return this.stagePath;
        }

        public void Upload(Dictionary<string, BindingDTO> bindings)
        {
            if(bindings == null)
            {
                return;
            }
            
            List<BindingDTO> arrbinds = bindings.Values.ToList();
            List<List<object>> bindList = new List<List<object>>();
            List<string> types = new List<string>(); // for the binding types
            List<string> dataRows = new List<string>(); // for the converted data string 
            int rowSize = ((List<object>)arrbinds[0].value).Count;
            int paramSize = arrbinds.Count;

            foreach(BindingDTO bind in arrbinds)
            {
                List<object> values = (List<object>)bind.value;
                types.Add(bind.type);
                if(values.Count != rowSize)
                {
                    //throw here different row size
                }
                bindList.Add(values);
            }

            for(int i=0; i<rowSize; i++)
            {
                StringBuilder sb = new StringBuilder();
                for (int j=0; j<paramSize; j++)
                {
                    if (j > 0)
                    {
                        sb.Append(',');
                    }

                    sb.Append(GetCSVData(types[j], (string)bindList[j][i]));
                }
                sb.Append('\n');
                //dataRows.Add(Encoding.UTF8.GetBytes(sb.ToString()));
                dataRows.Add(sb.ToString());
            }

            int startIndex = 0;
            int rowNum = 0;
            int curBytes = 0;
            StringBuilder sBuffer = new StringBuilder();
            while (rowNum < dataRows.Count)
            {
                while(curBytes < inputStreamBufferSize && rowNum < dataRows.Count)
                {
                    curBytes += dataRows[rowNum].Length;
                    rowNum++;
                }

                MemoryStream ms = new MemoryStream();
                StreamWriter tw = new StreamWriter(ms);
                for(int i = startIndex; i < rowNum; i++)
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
                catch(IOException e)
                {
                    // failure using stream put
                    throw new Exception("file stream upload error.");
                }
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
            statement.SetUploadStream(ref stream, destFileName, stagePath);
            statement.ExecuteTransfer(putStmt);
        }

        private string GetCSVData(string sType, string sValue)
        {
            if (sValue == null)
                return sValue;
            switch (sType)
            {
                case "DATE":
                    long dateLong = long.Parse(sValue);
                    DateTime dateTime1 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    DateTime date = dateTime1.AddMilliseconds(dateLong).ToLocalTime();
                    return date.ToShortDateString();
                case "TIME":
                    long timeLong = long.Parse(sValue);
                    DateTime dateTime2 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    DateTime time = dateTime2.AddMilliseconds(timeLong).ToLocalTime();
                    return time.ToLongTimeString();
                case "TIMESTAMP_LTZ":
                    return sValue;
                case "TIMESTAMP_NTZ":
                    return sValue;
                case "TIMESTAMP_TZ":
                    return sValue;
            }
            return sValue;
        }

        private void CreateStage()
        {
            if(session.GetArrayBindStage() != null)
            {
                return;
            }
            lock(session)
            {
                if (session.GetArrayBindStage() == null)
                {
                    try
                    {
                        string createStageSQL = "CREATE TEMPORARY STAGE IF NOT EXISTS " +
                            STAGE_NAME + " file_format=(" +
                            " type=csv field_optionally_enclosed_by='\"')";

                        SFStatement statement = new SFStatement(session);
                        SFBaseResultSet resultSet = statement.Execute(0, createStageSQL, null, false);
                        session.SetArrayBindStage(STAGE_NAME);
                        locationType = GetLocationType(statement.SfSession.properties[SFSessionProperty.ACCOUNT]);
                    }
                    catch (Exception e)
                    {
                        session.SetArrayBindStageThreshold(0);
                        logger.Error("Failed to create temporary stage for array binds.", e);
                        throw e;
                    }
                }
            }
        }

        private string GetLocationType(string account)
        {
            if(account.Contains(ACCOUNT_GCP))
            {
                return FileTransfer.SFRemoteStorageUtil.S3_FS;
            }
            else if(account.Contains(ACCOUNT_AZURE))
            {
                return FileTransfer.SFRemoteStorageUtil.AZURE_FS;
            }
            else if(account.Contains(ACCOUNT_LOCAL))
            {
                return FileTransfer.SFRemoteStorageUtil.LOCAL_FS;
            }
            else
            {
                return FileTransfer.SFRemoteStorageUtil.S3_FS;
            }
        }
    }
}
