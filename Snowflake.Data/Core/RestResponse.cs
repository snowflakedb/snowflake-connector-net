/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Core.FileTransfer;

namespace Snowflake.Data.Core
{
    abstract class BaseRestResponse
    {
        [JsonProperty(PropertyName = "message")]
        internal String message { get; set; }
        
        [JsonProperty(PropertyName = "code", NullValueHandling = NullValueHandling.Ignore)]
        internal int code { get; set; }
        
        [JsonProperty(PropertyName = "success")]
        internal bool success { get; set; }

        internal void FilterFailedResponse()
        {
            if (!success)
            {
                SnowflakeDbException e = new SnowflakeDbException("",code, message, "");
                throw e;
            }

        }
    }

    public interface IQueryExecResponseData
    {
        string queryId { get; }

        string sqlState { get; }
    }

    internal class BaseQueryExecResponse<T> : BaseRestResponse
    where T : IQueryExecResponseData
    {
        [JsonProperty(PropertyName = "data")]
        internal T data { get; set; }
    }

    class NullDataResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal object data { get; set; }
    }

    internal class AuthenticatorResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal AuthenticatorResponseData data { get; set; }
    }
    internal class LoginResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal LoginResponseData data { get; set; }
    }

    internal class RenewSessionResponse : BaseRestResponse {
		[JsonProperty(PropertyName = "data")]
		internal RenewSessionResponseData data { get; set; }
	}

    internal class LoginResponseData
    {
        [JsonProperty(PropertyName = "sessionId", NullValueHandling = NullValueHandling.Ignore)]
        internal string sessionId { get; set; }

        [JsonProperty(PropertyName = "token", NullValueHandling = NullValueHandling.Ignore)]
        internal string token { get; set; }

        [JsonProperty(PropertyName = "masterToken", NullValueHandling = NullValueHandling.Ignore)]
        internal string masterToken { get; set; }

        [JsonProperty(PropertyName = "serverVersion", NullValueHandling = NullValueHandling.Ignore)]
        internal string serverVersion { get; set; }

        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        internal List<NameValueParameter> nameValueParameter { get; set; }

        [JsonProperty(PropertyName = "sessionInfo", NullValueHandling = NullValueHandling.Ignore)]
        internal SessionInfo authResponseSessionInfo { get; set; }

        [JsonProperty(PropertyName = "masterValidityInSeconds", NullValueHandling = NullValueHandling.Ignore)]
        internal int masterValidityInSeconds { get; set; }
    }

    internal class AuthenticatorResponseData
    {
        [JsonProperty(PropertyName = "tokenUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string tokenUrl { get; set; }

        [JsonProperty(PropertyName = "ssoUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string ssoUrl { get; set; }

        [JsonProperty(PropertyName = "proofKey", NullValueHandling = NullValueHandling.Ignore)]
        internal string proofKey { get; set; }
    }


	internal class RenewSessionResponseData {

		[JsonProperty(PropertyName = "sessionToken", NullValueHandling = NullValueHandling.Ignore)]
		internal string sessionToken { get; set; }

		[JsonProperty(PropertyName = "validityInSecondsST", NullValueHandling = NullValueHandling.Ignore)]
		internal Int16 sessionTokenValidityInSeconds { get; set; }

		[JsonProperty(PropertyName = "masterToken", NullValueHandling = NullValueHandling.Ignore)]
		internal string masterToken { get; set; }

		[JsonProperty(PropertyName = "validityInSecondsMT", NullValueHandling = NullValueHandling.Ignore)]
		internal Int16 masterTokenValidityInSeconds { get; set; }

		[JsonProperty(PropertyName = "sessionId", NullValueHandling = NullValueHandling.Ignore)]
		internal Int64 sessionId { get; set; }
	}

    internal class SessionInfo
    {
        [JsonProperty(PropertyName = "databaseName")]
        internal string databaseName { get; set; }

        [JsonProperty(PropertyName = "schemaName")]
        internal string schemaName { get; set; }

        [JsonProperty(PropertyName = "warehouseName")]
        internal string warehouseName { get; set; }

        [JsonProperty(PropertyName = "roleName")]
        internal string roleName { get; set; }
    }

    internal class NameValueParameter
    {
        [JsonProperty(PropertyName = "name")]
        internal string name { get; set; }

        [JsonProperty(PropertyName = "value")]
        internal string value { get; set; }
    }

    internal class QueryExecResponse : BaseQueryExecResponse<QueryExecResponseData>
    {
        // data property already defined in BaseQueryExecResponse
    }

    internal class QueryExecResponseData : IQueryExecResponseData
    {
        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        internal List<NameValueParameter> parameters { get; set; }

        [JsonProperty(PropertyName = "rowtype", NullValueHandling = NullValueHandling.Ignore)]
        internal List<ExecResponseRowType> rowType { get; set; }

        [JsonProperty(PropertyName = "rowset", NullValueHandling = NullValueHandling.Ignore)]
        internal string[,] rowSet { get; set; }

        [JsonProperty(PropertyName = "total", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 total { get; set; }

        [JsonProperty(PropertyName = "returned", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 returned { get; set; }

        [JsonProperty(PropertyName = "queryId", NullValueHandling = NullValueHandling.Ignore)]
        public string queryId { get; set; }

        [JsonProperty(PropertyName = "sqlState", NullValueHandling = NullValueHandling.Ignore)]
        public string sqlState { get; set; }

        [JsonProperty(PropertyName = "databaseProvider", NullValueHandling = NullValueHandling.Ignore)]
        internal string databaseProvider { get; set; }

        [JsonProperty(PropertyName = "finalDatabaseName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalDatabaseName { get; set; }

        [JsonProperty(PropertyName = "finalSchemaName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalSchemaName { get; set; }

        [JsonProperty(PropertyName = "finalWarehouseName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalWarehouseName { get; set; }

        [JsonProperty(PropertyName = "finalRoleName", NullValueHandling = NullValueHandling.Ignore)]
        internal string finalRoleName { get; set; }

        [JsonProperty(PropertyName = "numberOfBinds", NullValueHandling = NullValueHandling.Ignore)]
        internal int numberOfBinds { get; set; }

        [JsonProperty(PropertyName = "statementTypeId", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 statementTypeId { get; set; }

        [JsonProperty(PropertyName = "version", NullValueHandling = NullValueHandling.Ignore)]
        internal int version { get; set; }

        [JsonProperty(PropertyName = "chunks", NullValueHandling = NullValueHandling.Ignore)]
        internal List<ExecResponseChunk> chunks { get; set; }

        [JsonProperty(PropertyName = "qrmk", NullValueHandling = NullValueHandling.Ignore)]
        internal string qrmk { get; set; }

        [JsonProperty(PropertyName = "chunkHeaders", NullValueHandling = NullValueHandling.Ignore)]
        internal Dictionary<string, string> chunkHeaders { get; set; }

        // ping pong response data
        [JsonProperty(PropertyName = "getResultUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string getResultUrl { get; set; }

        [JsonProperty(PropertyName = "progressDesc", NullValueHandling = NullValueHandling.Ignore)]
        internal string progressDesc { get; set; }

        [JsonProperty(PropertyName = "queryAbortAfterSecs", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 queryAbortAfterSecs { get; set; }

        // multiple statements response data
        [JsonProperty(PropertyName = "resultIds", NullValueHandling = NullValueHandling.Ignore)]
        internal string resultIds { get; set; }
    }

    internal class ExecResponseRowType
    {
        [JsonProperty(PropertyName = "name")]
        internal string name { get; set; }

        [JsonProperty(PropertyName = "byteLength", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 byteLength { get; set; }

        [JsonProperty(PropertyName = "length", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 length { get; set; }

        [JsonProperty(PropertyName = "type")]
        internal string type { get; set; }

        [JsonProperty(PropertyName = "scale", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 scale { get; set; }

        [JsonProperty(PropertyName = "precision", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 precision { get; set; }

        [JsonProperty(PropertyName = "nullable")]
        internal bool nullable { get; set; }
    }
    
    internal class ExecResponseChunk
    {
        [JsonProperty(PropertyName = "url")]
        internal string url { get; set; }

        [JsonProperty(PropertyName = "rowCount")]
        internal int rowCount { get; set; }

        [JsonProperty(PropertyName = "uncompressedSize")]
        internal int uncompressedSize { get; set; }
    }

    internal class CloseResponse : BaseRestResponse
    {
        [JsonProperty(PropertyName = "data")]
        internal object data { get; set; }
    }

    internal class PutGetExecResponse : BaseQueryExecResponse<PutGetResponseData>
    {
        // Defined for easy usage/access
    }

    internal class PutGetResponseData : IQueryExecResponseData
    {
        [JsonProperty(PropertyName = "command", NullValueHandling = NullValueHandling.Ignore)]
        internal string command { get; set; }

        [JsonProperty(PropertyName = "localLocation", NullValueHandling = NullValueHandling.Ignore)]
        internal string localLocation { get; set; }

        [JsonProperty(PropertyName = "src_locations", NullValueHandling = NullValueHandling.Ignore)]
        internal List<string> src_locations { get; set; }

        [JsonProperty(PropertyName = "parallel", NullValueHandling = NullValueHandling.Ignore)]
        internal int parallel { get; set; }

        [JsonProperty(PropertyName = "threshold", NullValueHandling = NullValueHandling.Ignore)]
        internal long threshold { get; set; }

        [JsonProperty(PropertyName = "autoCompress", NullValueHandling = NullValueHandling.Ignore)]
        internal bool autoCompress { get; set; }

        [JsonProperty(PropertyName = "overwrite", NullValueHandling = NullValueHandling.Ignore)]
        internal bool overwrite { get; set; }

        [JsonProperty(PropertyName = "sourceCompression", NullValueHandling = NullValueHandling.Ignore)]
        internal string sourceCompression { get; set; }

        [JsonProperty(PropertyName = "stageInfo", NullValueHandling = NullValueHandling.Ignore)]
        internal PutGetStageInfo stageInfo { get; set; }

        [JsonProperty(PropertyName = "encryptionMaterial", NullValueHandling = NullValueHandling.Ignore)]
        [JsonConverter(typeof(SingleOrArrayConverter<PutGetEncryptionMaterial>))]
        internal List<PutGetEncryptionMaterial> encryptionMaterial { get; set; }

        [JsonProperty(PropertyName = "queryId", NullValueHandling = NullValueHandling.Ignore)]
        public string queryId { get; set; }

        [JsonProperty(PropertyName = "sqlState", NullValueHandling = NullValueHandling.Ignore)]
        public string sqlState { get; set; }

        [JsonProperty(PropertyName = "presignedUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string presignedUrl { get; set; }

        [JsonProperty(PropertyName = "presignedUrls", NullValueHandling = NullValueHandling.Ignore)]
        internal List<string> presignedUrls { get; set; }

        [JsonProperty(PropertyName = "rowtype", NullValueHandling = NullValueHandling.Ignore)]
        internal List<ExecResponseRowType> rowType { get; set; }

        [JsonProperty(PropertyName = "rowset", NullValueHandling = NullValueHandling.Ignore)]
        internal string[,] rowSet { get; set; }

        [JsonProperty(PropertyName = "parameters", NullValueHandling = NullValueHandling.Ignore)]
        internal List<NameValueParameter> parameters { get; set; }

        [JsonProperty(PropertyName = "statementTypeId", NullValueHandling = NullValueHandling.Ignore)]
        internal Int64 statementTypeId { get; set; }
    }

    internal class PutGetStageInfo
    {
        [JsonProperty(PropertyName = "locationType", NullValueHandling = NullValueHandling.Ignore)]
        internal string locationType { get; set; }

        [JsonProperty(PropertyName = "location", NullValueHandling = NullValueHandling.Ignore)]
        internal string location { get; set; }

        [JsonProperty(PropertyName = "path", NullValueHandling = NullValueHandling.Ignore)]
        internal string path { get; set; }

        [JsonProperty(PropertyName = "region", NullValueHandling = NullValueHandling.Ignore)]
        internal string region { get; set; }

        [JsonProperty(PropertyName = "storageAccount", NullValueHandling = NullValueHandling.Ignore)]
        internal string storageAccount { get; set; }

        [JsonProperty(PropertyName = "isClientSideEncrypted", NullValueHandling = NullValueHandling.Ignore)]
        internal bool isClientSideEncrypted { get; set; }

        [JsonProperty(PropertyName = "creds", NullValueHandling = NullValueHandling.Ignore)]
        internal Dictionary<string, string> stageCredentials { get; set; }

        [JsonProperty(PropertyName = "presignedUrl", NullValueHandling = NullValueHandling.Ignore)]
        internal string presignedUrl { get; set; }

        [JsonProperty(PropertyName = "endPoint", NullValueHandling = NullValueHandling.Ignore)]
        internal string endPoint { get; set; }
    }

    internal class PutGetEncryptionMaterial
    {
        [JsonProperty(PropertyName = "queryStageMasterKey", NullValueHandling = NullValueHandling.Ignore)]
        internal string queryStageMasterKey { get; set; }

        [JsonProperty(PropertyName = "queryId", NullValueHandling = NullValueHandling.Ignore)]
        internal string queryId { get; set; }

        [JsonProperty(PropertyName = "smkId", NullValueHandling = NullValueHandling.Ignore)]
        internal long smkId { get; set; }
    }

    // Retrieved from: https://stackoverflow.com/a/18997172
    internal class SingleOrArrayConverter<T> : JsonConverter
    {
        public override bool CanConvert(Type objecType)
        {
            return (objecType == typeof(List<T>));
        }

        public override object ReadJson(JsonReader reader, Type objecType, object existingValue,
            JsonSerializer serializer)
        {
            JToken token = JToken.Load(reader);
            if (token.Type == JTokenType.Array)
            {
                return token.ToObject<List<T>>();
            }
            return new List<T> { token.ToObject<T>() };
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
} 
