using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Snowflake.Data.Client;

namespace Snowflake.Data.Core
{
    abstract class BaseRestResponse
    {
        [JsonPropertyName("message")]
        public String message { get; set; }

        [JsonPropertyName("code")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public int? code { get; set; }

        [JsonPropertyName("success")]
        public bool success { get; set; }

        internal void FilterFailedResponse()
        {
            if (!success)
            {
                SnowflakeDbException e = new SnowflakeDbException("", code ?? default, message, "");
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
        [JsonPropertyName("data")]
        public T data { get; set; }
    }

    class NullDataResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public object data { get; set; }
    }

    internal class AuthenticatorResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public AuthenticatorResponseData data { get; set; }
    }

    internal class LoginResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public LoginResponseData data { get; set; }
    }

    internal class RenewSessionResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public RenewSessionResponseData data { get; set; }
    }

    internal class LoginResponseData
    {
        [JsonPropertyName("sessionId")]
        [JsonConverter(typeof(JsonUtils.AnyToStringJsonConverter))]
        public string sessionId { get; set; }

        [JsonPropertyName("token")]
        public string token { get; set; }

        [JsonPropertyName("masterToken")]
        public string masterToken { get; set; }

        [JsonPropertyName("serverVersion")]
        public string serverVersion { get; set; }

        [JsonPropertyName("parameters")]
        public List<NameValueParameter> nameValueParameter { get; set; }

        [JsonPropertyName("sessionInfo")]
        public SessionInfo authResponseSessionInfo { get; set; }

        [JsonPropertyName("masterValidityInSeconds")]
        public int masterValidityInSeconds { get; set; }
    }

    internal class AuthenticatorResponseData
    {
        [JsonPropertyName("tokenUrl")]
        public string tokenUrl { get; set; }

        [JsonPropertyName("ssoUrl")]
        public string ssoUrl { get; set; }

        [JsonPropertyName("proofKey")]
        public string proofKey { get; set; }
    }

    internal class RenewSessionResponseData
    {
        [JsonPropertyName("sessionToken")]
        public string sessionToken { get; set; }

        [JsonPropertyName("validityInSecondsST")]
        public Int16 sessionTokenValidityInSeconds { get; set; }

        [JsonPropertyName("masterToken")]
        public string masterToken { get; set; }

        [JsonPropertyName("validityInSecondsMT")]
        public Int16 masterTokenValidityInSeconds { get; set; }

        [JsonPropertyName("sessionId")]
        public Int64 sessionId { get; set; }
    }

    internal class SessionInfo
    {
        [JsonPropertyName("databaseName")]
        public string databaseName { get; set; }

        [JsonPropertyName("schemaName")]
        public string schemaName { get; set; }

        [JsonPropertyName("warehouseName")]
        public string warehouseName { get; set; }

        [JsonPropertyName("roleName")]
        public string roleName { get; set; }
    }

    internal class NameValueParameter
    {
        [JsonPropertyName("name")]
        public string name { get; set; }

        [JsonPropertyName("value")]
        [JsonConverter(typeof(JsonUtils.AnyToStringJsonConverter))]
        public string value { get; set; }
    }

    internal class QueryExecResponse : BaseQueryExecResponse<QueryExecResponseData>
    {
        // data property already defined in BaseQueryExecResponse
    }

    internal class QueryExecResponseData : IQueryExecResponseData
    {
        [JsonPropertyName("parameters")]
        public List<NameValueParameter> parameters { get; set; }

        [JsonPropertyName("rowtype")]
        public List<ExecResponseRowType> rowType { get; set; }

        [JsonPropertyName("rowset")]
        public string[,] rowSet { get; set; }

        [JsonPropertyName("total")]
        public Int64 total { get; set; }

        [JsonPropertyName("returned")]
        public Int64 returned { get; set; }

        [JsonPropertyName("queryId")]
        public string queryId { get; set; }

        [JsonPropertyName("sqlState")]
        public string sqlState { get; set; }

        [JsonPropertyName("databaseProvider")]
        public string databaseProvider { get; set; }

        [JsonPropertyName("finalDatabaseName")]
        public string finalDatabaseName { get; set; }

        [JsonPropertyName("finalSchemaName")]
        public string finalSchemaName { get; set; }

        [JsonPropertyName("finalWarehouseName")]
        public string finalWarehouseName { get; set; }

        [JsonPropertyName("finalRoleName")]
        public string finalRoleName { get; set; }

        [JsonPropertyName("numberOfBinds")]
        public int numberOfBinds { get; set; }

        [JsonPropertyName("statementTypeId")]
        public Int64 statementTypeId { get; set; }

        [JsonPropertyName("version")]
        public int version { get; set; }

        [JsonPropertyName("chunks")]
        public List<ExecResponseChunk> chunks { get; set; }

        [JsonPropertyName("qrmk")]
        public string qrmk { get; set; }

        [JsonPropertyName("chunkHeaders")]
        public Dictionary<string, string> chunkHeaders { get; set; }

        [JsonPropertyName("getResultUrl")]
        public string getResultUrl { get; set; }

        [JsonPropertyName("progressDesc")]
        public string progressDesc { get; set; }

        [JsonPropertyName("queryAbortAfterSecs")]
        public Int64 queryAbortAfterSecs { get; set; }

        [JsonPropertyName("resultIds")]
        public string resultIds { get; set; }

        [JsonPropertyName("queryResultFormat")]
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public ResultFormat queryResultFormat { get; set; }

        [JsonPropertyName("rowsetBase64")]
        public string rowsetBase64 { get; set; }

        [JsonPropertyName("queryContext")]
        public ResponseQueryContext QueryContext { get; set; }
    }

    internal class ResponseQueryContext
    {
        [JsonPropertyName("entries")]
        public List<ResponseQueryContextElement> Entries { get; set; }
    }

    internal class ResponseQueryContextElement
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("timestamp")]
        public long ReadTimestamp { get; set; }

        [JsonPropertyName("priority")]
        public long Priority { get; set; }

        [JsonPropertyName("context")]
        public string Context { get; set; }

        public ResponseQueryContextElement() { }

        public ResponseQueryContextElement(QueryContextElement elem)
        {
            Id = elem.Id;
            Priority = elem.Priority;
            ReadTimestamp = elem.ReadTimestamp;
            Context = elem.Context;
        }
    }

    internal class ExecResponseRowType
    {
        [JsonPropertyName("name")]
        public string name { get; set; }

        [JsonPropertyName("byteLength")]
        public Int64? byteLength { get; set; }

        [JsonPropertyName("length")]
        public Int64? length { get; set; }

        [JsonPropertyName("type")]
        public string type { get; set; }

        [JsonPropertyName("scale")]
        public Int64? scale { get; set; }

        [JsonPropertyName("precision")]
        public Int64? precision { get; set; }

        [JsonPropertyName("nullable")]
        public bool nullable { get; set; }

        [JsonPropertyName("fields")]
        public List<FieldMetadata> fields { get; set; }
    }

    internal class FieldMetadata
    {
        [JsonPropertyName("name")]
        public string name { get; set; }

        [JsonPropertyName("byteLength")]
        public Int64 byteLength { get; set; }

        [JsonPropertyName("typeName")]
        public string typeName { get; set; }

        [JsonPropertyName("type")]
        public string type { get; set; }

        [JsonPropertyName("scale")]
        public Int64 scale { get; set; }

        [JsonPropertyName("precision")]
        public Int64 precision { get; set; }

        [JsonPropertyName("nullable")]
        public bool nullable { get; set; }

        [JsonPropertyName("fields")]
        public List<FieldMetadata> fields { get; set; }
    }

    internal class ExecResponseChunk
    {
        [JsonPropertyName("url")]
        public string url { get; set; }

        [JsonPropertyName("rowCount")]
        public int rowCount { get; set; }

        [JsonPropertyName("uncompressedSize")]
        public int uncompressedSize { get; set; }

        [JsonPropertyName("compressedSize")]
        public int compressedSize { get; set; }
    }

    internal class CloseResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public object data { get; set; }
    }

    internal class PutGetExecResponse : BaseQueryExecResponse<PutGetResponseData>
    {
        // Defined for easy usage/access
    }

    internal class PutGetResponseData : IQueryExecResponseData
    {
        [JsonPropertyName("command")]
        public string command { get; set; }

        [JsonPropertyName("localLocation")]
        public string localLocation { get; set; }

        [JsonPropertyName("src_locations")]
        public List<string> src_locations { get; set; }

        [JsonPropertyName("parallel")]
        public int parallel { get; set; }

        [JsonPropertyName("threshold")]
        public long threshold { get; set; }

        [JsonPropertyName("autoCompress")]
        public bool autoCompress { get; set; }

        [JsonPropertyName("overwrite")]
        public bool overwrite { get; set; }

        [JsonPropertyName("sourceCompression")]
        public string sourceCompression { get; set; }

        [JsonPropertyName("stageInfo")]
        public PutGetStageInfo stageInfo { get; set; }

        [JsonPropertyName("encryptionMaterial")]
        [JsonConverter(typeof(SingleOrArrayConverter<PutGetEncryptionMaterial>))]
        public List<PutGetEncryptionMaterial> encryptionMaterial { get; set; }

        [JsonPropertyName("queryId")]
        public string queryId { get; set; }

        [JsonPropertyName("sqlState")]
        public string sqlState { get; set; }

        [JsonPropertyName("presignedUrl")]
        public string presignedUrl { get; set; }

        [JsonPropertyName("presignedUrls")]
        public List<string> presignedUrls { get; set; }

        [JsonPropertyName("rowtype")]
        public List<ExecResponseRowType> rowType { get; set; }

        [JsonPropertyName("rowset")]
        [JsonConverter(typeof(Array2DConverter))]
        public string[,] rowSet { get; set; }

        [JsonPropertyName("parameters")]
        public List<NameValueParameter> parameters { get; set; }

        [JsonPropertyName("statementTypeId")]
        public Int64 statementTypeId { get; set; }
    }

    internal class PutGetStageInfo
    {
        [JsonPropertyName("locationType")]
        public string locationType { get; set; }

        [JsonPropertyName("location")]
        public string location { get; set; }

        [JsonPropertyName("path")]
        public string path { get; set; }

        [JsonPropertyName("region")]
        public string region { get; set; }

        [JsonPropertyName("storageAccount")]
        public string storageAccount { get; set; }

        [JsonPropertyName("isClientSideEncrypted")]
        public bool isClientSideEncrypted { get; set; }

        [JsonPropertyName("creds")]
        public Dictionary<string, string> stageCredentials { get; set; }

        [JsonPropertyName("presignedUrl")]
        public string presignedUrl { get; set; }

        [JsonPropertyName("endPoint")]
        public string endPoint { get; set; }
    }

    internal class PutGetEncryptionMaterial
    {
        [JsonPropertyName("queryStageMasterKey")]
        public string queryStageMasterKey { get; set; }

        [JsonPropertyName("queryId")]
        public string queryId { get; set; }

        [JsonPropertyName("smkId")]
        public long smkId { get; set; }
    }

    internal class QueryStatusResponse : BaseRestResponse
    {
        [JsonPropertyName("data")]
        public QueryStatusData data { get; set; }
    }

    internal class QueryStatusData
    {
        [JsonPropertyName("queries")]
        public List<QueryStatusDataQueries> queries { get; set; }
    }

    internal class QueryStatusDataQueries
    {
        [JsonPropertyName("id")]
        public string id { get; set; }

        [JsonPropertyName("status")]
        public string status { get; set; }

        [JsonPropertyName("state")]
        public string state { get; set; }

        [JsonPropertyName("errorCode")]
        public string errorCode { get; set; }

        [JsonPropertyName("errorMessage")]
        public string errorMessage { get; set; }
    }

    internal class SingleOrArrayConverter<T> : JsonConverter<List<T>>
    {
        public override List<T> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.StartArray)
            {
                return JsonSerializer.Deserialize<List<T>>(ref reader, options);
            }
            else
            {
                var singleValue = JsonSerializer.Deserialize<T>(ref reader, options);
                return new List<T> { singleValue };
            }
        }

        public override void Write(Utf8JsonWriter writer, List<T> value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }
}
