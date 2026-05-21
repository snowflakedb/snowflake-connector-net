using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Telemetry;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class ArchitectureInvariantsTest
{
    private static readonly string s_solutionRoot = FindSolutionRoot();
    private static readonly string s_productionSourceDir = Path.Combine(s_solutionRoot, "Snowflake.Data");

    private static readonly Assembly s_testAssembly = typeof(ArchitectureInvariantsTest).Assembly;
    private static readonly Assembly s_libAssembly = typeof(SnowflakeDbConnection).Assembly;
    private static readonly Dictionary<string, Type> s_libTypes = s_libAssembly.GetExportedTypes().ToDictionary(t => t.FullName, t => t);

    [SFFact]
    public void TestNoBareFact_AllTestsMustUseSFFactOrSFTheory()
    {
        var violations = new List<string>();
        var testTypes = s_testAssembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract);

        foreach (var type in testTypes)
        {
            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
            foreach (var method in methods)
            {
                var attributes = method.GetCustomAttributes(true);
                var hasBareFactAttribute = attributes.Any(a =>
                    a.GetType() == typeof(FactAttribute));
                var hasBareTheoryAttribute = attributes.Any(a =>
                    a.GetType() == typeof(TheoryAttribute));

                if (hasBareFactAttribute)
                    violations.Add($"{type.FullName}.{method.Name} uses bare [Fact] instead of [SFFact]");

                if (hasBareTheoryAttribute)
                    violations.Add($"{type.FullName}.{method.Name} uses bare [Theory] instead of [SFTheory]");
            }
        }

        List<string> expectedViolations =
        [
            $"{GetType().FullName}.{nameof(DummyTest)} uses bare [Fact] instead of [SFFact]",
            $"{GetType().FullName}.{nameof(DummyParametrizedTest)} uses bare [Theory] instead of [SFTheory]",
        ];
        AssertOnViolations(expectedViolations, violations);
    }

    [SFFact]
    public void TestConfigureAwaitFalse_AllAwaitsInProductionCodeMustHaveConfigureAwaitFalse()
    {
        var violations = new List<string>();
        var csFiles = Directory.GetFiles(s_productionSourceDir, "*.cs", SearchOption.AllDirectories);

        Parallel.ForEach(csFiles, file =>
        {
            var relativePath = file.Substring(s_solutionRoot.Length + "Snowflake.Data".Length + 1);
            var source = File.ReadAllText(file);
            var tree = CSharpSyntaxTree.ParseText(source, path: file);
            var root = tree.GetRoot();

            var awaitExpressions = root.DescendantNodes().OfType<AwaitExpressionSyntax>();

            foreach (var awaitExpr in awaitExpressions)
            {
                if (awaitExpr.Expression is not InvocationExpressionSyntax invocation)
                {
                    invocation = null;
                }
                else if (invocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "ConfigureAwait" } &&
                         invocation.ArgumentList.Arguments.Count == 1)
                {
                    var argument = invocation.ArgumentList.Arguments[0].Expression;
                    if (argument is LiteralExpressionSyntax literal && literal.IsKind(SyntaxKind.FalseLiteralExpression))
                        continue;
                }

                var expressionStr = awaitExpr.Expression.ToString().Trim().Replace("\n", string.Empty);
                if (invocation != null && invocation.Expression.ToString() == "Task.Run")
                    expressionStr = "Task.Run(...);";

                var lineSegment = string.Empty;
                // uncomment below to get specific line number.
                //var lineSpan = awaitExpr.GetLocation().GetLineSpan();
                //var lineNumber = lineSpan.StartLinePosition.Line + 1;
                // lineSegment = $"{lineNumber}:";
                violations.Add($"{relativePath}:{lineSegment} {expressionStr}");
            }
        });

        // TODO SNOW-3560671
        var expectedViolations = new[]
        {
            "/Core/SFResultSet.cs: Task.FromResult(false)",
            "/Core/ArrowResultSet.cs: Task.FromResult(false)",
            "/Core/SFMultiStatementsResultSet.cs: NextResultAsync(CancellationToken.None)",
            "/Core/SFMultiStatementsResultSet.cs: curResultSet.NextAsync()",
            "/Core/SFMultiStatementsResultSet.cs: sfStatement.GetResultWithIdAsync(                                        resultIds[curResultIndex],                                        cancellationToken)",
            "/Core/SFMultiStatementsResultSet.cs: Task.FromResult(curResultSet != null)",
            "/Core/ReusableChunkParser.cs: Task.Run(...);",
            "/Client/SnowflakeDbDataReader.cs: resultSet.NextAsync()",
            "/Client/SnowflakeDbCommand.cs: _queryResultsAwaiter.GetQueryStatusAsync(connection, queryId, cancellationToken)",
            "/Client/SnowflakeDbCommand.cs: _queryResultsAwaiter.RetryUntilQueryResultIsAvailable(connection, queryId, cancellationToken, true)",
            "/Core/Authenticator/BasicAuthenticator.cs: base.LoginAsync(cancellationToken)",
            "/Core/SFStatement.cs: RenewSessionIfNeededAsync(response, cancellationToken)",
            "/Core/QueryResultsAwaiter.cs: GetQueryStatusAsync(connection, queryId, cancellationToken)",
            "/Client/SnowflakeDbConnection.cs: taskCompletionSource.Task",
            "/Core/Authenticator/MFACacheAuthenticator.cs: base.LoginAsync(cancellationToken)",
            "/Core/SFBlockingChunkDownloaderV3.cs: chunk",
            "/Core/SFBlockingChunkDownloaderV3.cs: Task.FromResult<BaseResultChunk>(null)",
            "/Core/SFBlockingChunkDownloaderV3.cs: parser.ParseChunk(resultChunk)"
        }.Select(x => x.Replace('/', Path.DirectorySeparatorChar)).ToArray();
        AssertOnViolations(expectedViolations, violations);
    }

    [SFFact]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedTypesArePublic()
    {
        var publicTypes = s_libTypes
            .Keys
            .OrderBy(t => t)
            .ToArray();

        var shimTypeToIgnore = "Mono.Unix.FileAccessPermissions";
        AssertOnViolations(s_approvedPublicTypes, publicTypes, shimTypeToIgnore);
    }

    [SFTheory]
    [MemberData(nameof(ApprovedPublicTypes))]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedMethodsArePublic(string typeName)
    {
        var type = s_libTypes[typeName];
        var approvedMethods = s_apiDefinition[type].Methods;
        var actualMethods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .Select(m => new { m.Name, Params = m.GetParameters() })
            .Select(m => new { m.Name, ParamsStr = string.Join(", ", m.Params.Select(p => p.ParameterType.Name)) })
            .Select(m => $"{m.Name}({m.ParamsStr})")
            .ToList();

        AssertOnViolations(approvedMethods.OrderBy(t => t).ToList(), actualMethods);
    }

    [SFTheory]
    [MemberData(nameof(ApprovedPublicTypes))]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedFieldsArePublic(string typeName)
    {
        var type = s_libTypes[typeName];
        var approvedFields = s_apiDefinition[type].Fields;
        var actualFields = type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(f => f.Name)
            .ToList();

        var enumMemberToIgnore = "value__";
        AssertOnViolations(approvedFields, actualFields, enumMemberToIgnore);
    }

    [SFTheory]
    [MemberData(nameof(ApprovedPublicTypes))]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedConstructorsArePublic(string typeName)
    {
        var type = s_libTypes[typeName];
        var approvedConstructors = s_apiDefinition[type].Constructors;
        var actualConstructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(c => $".ctor({string.Join(", ", c.GetParameters().Select(p => p.ParameterType.Name))})")
            .ToList();

        AssertOnViolations(approvedConstructors, actualConstructors);
    }

    [SFTheory]
    [MemberData(nameof(ApprovedPublicTypes))]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedGettersArePublic(string typeName)
    {
        var type = s_libTypes[typeName];
        var approvedGetters = s_apiDefinition[type].Getters;
        var actualGetters = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(p => p.GetMethod is { IsPublic: true })
            .Select(p => p.Name)
            .ToList();

        AssertOnViolations(approvedGetters, actualGetters);
    }

    [SFTheory]
    [MemberData(nameof(ApprovedPublicTypes))]
    public void TestPublicApiSurface_OnlyExplicitlyApprovedSettersArePublic(string typeName)
    {
        var type = s_libTypes[typeName];
        var approvedSetters = s_apiDefinition[type].Setters;
        var actualSetters = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(p => p.SetMethod is { IsPublic: true })
            .Select(p => p.Name)
            .ToList();

        AssertOnViolations(approvedSetters, actualSetters);
    }

    private readonly struct ComponentFace
    {
        public string[] Constructors { get; init; }
        public string[] Methods { get; init; }
        public string[] Getters { get; init; }
        public string[] Setters { get; init; }
        public string[] Fields { get; init; }
    }

    private static string FindSolutionRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir, "snowflake-connector-net.sln")))
                return dir;
            dir = Directory.GetParent(dir)?.FullName;
        }

        // Fallback - try relative from test output
        var candidate = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
        if (File.Exists(Path.Combine(candidate, "snowflake-connector-net.sln")))
            return candidate;

        throw new InvalidOperationException("Cannot find solution root directory");
    }

    public static IEnumerable<object[]> ApprovedPublicTypes => s_approvedPublicTypes
        .OrderBy(t => t)
        .Select(t => new object[] { t });


    [Fact]
    public void DummyTest()
    {
        Assert.True(true);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DummyParametrizedTest(bool _)
    {
        Assert.True(true);
    }

    private static void AssertOnViolations(IEnumerable<string> expectedViolations, IEnumerable<string> actualViolations, params string[] toIgnore)
    {
        expectedViolations = expectedViolations.Concat(toIgnore);
        actualViolations = actualViolations.Concat(toIgnore);
        var expectedNotReceived = expectedViolations.Except(actualViolations).ToArray();
        var unexpected = actualViolations.Except(expectedViolations).ToArray();

        var failedCount = expectedNotReceived.Length + unexpected.Length;
        Assert.True(failedCount == 0,
            $"Expected, but not received: \n {string.Join(",\n", expectedNotReceived)} \n Observed unexpected: \n{string.Join(",\n", unexpected)}");
    }

    private static readonly string[] s_approvedPublicTypes =
    [
        "Snowflake.Data.Client.ISnowflakeCredentialManager",
        "Snowflake.Data.Client.SnowflakeColumn",
        "Snowflake.Data.Client.SnowflakeCredentialManagerFactory",
        "Snowflake.Data.Client.SnowflakeDbCommand",
        "Snowflake.Data.Client.SnowflakeDbCommandBuilder",
        "Snowflake.Data.Client.SnowflakeDbConnection",
        "Snowflake.Data.Client.SnowflakeDbConnectionPool",
        "Snowflake.Data.Client.SnowflakeDbConnectionStringBuilder",
        "Snowflake.Data.Client.SnowflakeDbDataAdapter",
        "Snowflake.Data.Client.SnowflakeDbDataReader",
        "Snowflake.Data.Client.SnowflakeDbException",
        "Snowflake.Data.Client.SnowflakeDbFactory",
        "Snowflake.Data.Client.SnowflakeDbLoggerConfig",
        "Snowflake.Data.Client.SnowflakeDbParameter",
        "Snowflake.Data.Client.SnowflakeDbParameterCollection",
        "Snowflake.Data.Client.SnowflakeDbSessionPool",
        "Snowflake.Data.Client.SnowflakeDbTransaction",
        "Snowflake.Data.Client.SnowflakeObject",
        "Snowflake.Data.Client.SnowflakeObjectConstructionMethod",
        "Snowflake.Data.Configuration.SFConfiguration",
        "Snowflake.Data.Core.SslProtocolsExtensions",
        "Snowflake.Data.Core.FileTransfer.SFEncryptionMetadata",
        "Snowflake.Data.Core.QueryStatus",
        "Snowflake.Data.Core.ResultFormat",
        "Snowflake.Data.Core.SFDataType",
        "Snowflake.Data.Core.SFError",
        "Snowflake.Data.Core.Session.ChangedSessionBehavior",
        "Snowflake.Data.Telemetry.ActivityStarter",
        "Snowflake.Data.Telemetry.SessionTelemetryModuleFacade",
    ];

    private static readonly Dictionary<Type, ComponentFace> s_apiDefinition = new()
    {
        [typeof(ISnowflakeCredentialManager)] = new()
        {
            Constructors = [],
            Methods = ["GetCredentials(String)", "RemoveCredentials(String)", "SaveCredentials(String, String)"],
            Getters = [],
            Setters = [],
            Fields = []
        },
        [typeof(SnowflakeColumn)] = new()
        {
            Constructors = [".ctor()"],
            Methods = [],
            Getters = ["Name", "IgnoreForPropertyOrder"],
            Setters = ["Name", "IgnoreForPropertyOrder"],
            Fields = []
        },
        [typeof(SnowflakeCredentialManagerFactory)] = new()
        {
            Constructors = [".ctor()"],
            Methods =
            [
                "UseDefaultCredentialManager()", "UseInMemoryCredentialManager()", "UseFileCredentialManager()", "UseWindowsCredentialManager()",
                "SetCredentialManager(ISnowflakeCredentialManager)", "GetCredentialManager()"
            ],
            Getters = [],
            Fields = [],
            Setters = []
        },
        [typeof(SnowflakeDbCommand)] = new()
        {
            Constructors = [".ctor()", ".ctor(SnowflakeDbConnection)", ".ctor(SnowflakeDbConnection, String)"],
            Methods =
            [
                "Cancel()", "ExecuteNonQuery()", "ExecuteNonQueryAsync(CancellationToken)", "ExecuteScalar()",
                "ExecuteScalarAsync(CancellationToken)", "Prepare()", "GetQueryId()", "ExecuteInAsyncMode()",
                "ExecuteAsyncInAsyncMode(CancellationToken)", "GetQueryStatus(String)", "GetQueryStatusAsync(String, CancellationToken)",
                "GetResultsFromQueryId(String)", "GetResultsFromQueryIdAsync(String, CancellationToken)"
            ],
            Getters = ["CommandText", "CommandTimeout", "QueryTag", "CommandType", "DesignTimeVisible", "UpdatedRowSource"],
            Setters = ["CommandText", "CommandTimeout", "QueryTag", "CommandType", "DesignTimeVisible", "UpdatedRowSource"],
            Fields = []
        },
        [typeof(SnowflakeDbCommandBuilder)] = new()
        {
            Constructors = [".ctor()", ".ctor(SnowflakeDbDataAdapter)"],
            Methods = [],
            Getters = ["QuotePrefix", "QuoteSuffix"],
            Setters = ["QuotePrefix", "QuoteSuffix"],
            Fields = ["DEFAULT_QUOTE_PREFIX", "DEFAULT_QUOTE_SUFFIX"]
        },
        [typeof(SnowflakeDbConnection)] = new()
        {
            Constructors = [".ctor()", ".ctor(String)"],
            Methods =
            [
                "IsOpen()", "PreventPooling()", "ChangeDatabase(String)", "Close()", "CloseAsync()", "CloseAsync(CancellationToken)", "Open()",
                "OpenAsync(CancellationToken)", "GetArrayBindingMutex()", "IsArrayBindStageCreated()", "SetArrayBindStageCreated()",
                "IsStillRunning(QueryStatus)", "IsAnError(QueryStatus)"
            ],
            Getters =
            [
                "ConnectionString", "Password", "Passcode", "OAuthClientSecret", "Token", "Database", "ConnectionTimeout", "DataSource",
                "ServerVersion", "State"
            ],
            Setters = ["ConnectionString", "Password", "Passcode", "OAuthClientSecret", "Token"],
            Fields = []
        },
        [typeof(SnowflakeDbConnectionPool)] = new()
        {
            Constructors = [".ctor()"],
            Methods =
            [
                "GetPool(String, SecureString, SecureString, SecureString)", "GetPool(String)", "ClearAllPools()", "SetMaxPoolSize(Int32)",
                "GetMaxPoolSize()", "SetTimeout(Int64)", "GetTimeout()", "GetCurrentPoolSize()", "SetPooling(Boolean)", "GetPooling()",
                "SetOldConnectionPoolVersion()"
            ],
            Getters = [],
            Fields = [],
            Setters = []
        },
        [typeof(SnowflakeDbConnectionStringBuilder)] = new() { Constructors = [".ctor()"], Methods = [], Getters = [], Fields = [], Setters = [] },
        [typeof(SnowflakeDbDataAdapter)] = new()
        {
            Constructors = [".ctor()", ".ctor(SnowflakeDbCommand)", ".ctor(String, SnowflakeDbConnection)"],
            Methods = [],
            Getters = ["SelectCommand"],
            Setters = ["SelectCommand"],
            Fields = []
        },
        [typeof(SnowflakeDbDataReader)] = new()
        {
            Constructors = [],
            Methods =
            [
                "GetSchemaTable()", "GetQueryId()", "GetBoolean(Int32)", "GetByte(Int32)", "GetBytes(Int32, Int64, Byte[], Int32, Int32)",
                "GetChar(Int32)", "GetChars(Int32, Int64, Char[], Int32, Int32)", "GetDataTypeName(Int32)", "GetDateTime(Int32)",
                "GetTimeSpan(Int32)", "GetDecimal(Int32)", "GetDouble(Int32)", "GetEnumerator()", "GetFieldType(Int32)", "GetFloat(Int32)",
                "GetGuid(Int32)", "GetInt16(Int32)", "GetInt32(Int32)", "GetInt64(Int32)", "GetName(Int32)", "GetOrdinal(String)", "GetString(Int32)",
                "GetValue(Int32)", "GetValues(Object[])", "GetObject(Int32)", "GetArray(Int32)", "GetMap(Int32)", "IsDBNull(Int32)", "NextResult()",
                "NextResultAsync(CancellationToken)", "Read()", "ReadAsync(CancellationToken)", "Close()"
            ],
            Getters = ["Item", "Item", "Depth", "FieldCount", "HasRows", "IsClosed", "RecordsAffected"],
            Fields = [],
            Setters = []
        },
        [typeof(SnowflakeDbException)] = new()
        {
            Constructors =
            [
                ".ctor(String, Int32, String, String)", ".ctor(SFError, String, Exception)", ".ctor(SFError, Object[])",
                ".ctor(String, SFError, Object[])", ".ctor(Exception, SFError, Object[])", ".ctor(Exception, String, SFError, Object[])"
            ],
            Methods = [],
            Getters = ["SqlState", "QueryId", "ErrorCode"],
            Fields = [],
            Setters = ["QueryId"]
        },
        [typeof(SnowflakeDbFactory)] = new()
        {
            Constructors = [".ctor()"],
            Methods =
            [
                "CreateCommand()", "CreateConnection()", "CreateParameter()", "CreateConnectionStringBuilder()", "CreateCommandBuilder()",
                "CreateDataAdapter()"
            ],
            Getters = [],
            Fields = ["Instance"],
            Setters = []
        },
        [typeof(SnowflakeDbLoggerConfig)] = new()
        { Constructors = [".ctor()"], Methods = ["ResetCustomLogger()", "SetCustomLogger(ILogger)"], Getters = [], Fields = [], Setters = [] },
        [typeof(SnowflakeDbParameter)] = new()
        {
            Constructors = [".ctor()", ".ctor(String, SFDataType)", ".ctor(Int32, SFDataType)"],
            Methods = ["ResetDbType()"],
            Getters =
            [
                "SFDataType", "DbType", "Direction", "IsNullable", "ParameterName", "Size", "SourceColumn", "SourceColumnNullMapping", "Value"
            ],
            Setters =
                ["SFDataType", "DbType", "Direction", "IsNullable", "ParameterName", "Size", "SourceColumn", "SourceColumnNullMapping", "Value"],
            Fields = []
        },
        [typeof(SnowflakeDbParameterCollection)] = new()
        {
            Constructors = [],
            Methods =
            [
                "Add(Object)", "Add(String, SFDataType)", "AddRange(Array)", "Clear()", "Contains(String)", "Contains(Object)",
                "CopyTo(Array, Int32)", "GetEnumerator()", "IndexOf(String)", "IndexOf(Object)", "Insert(Int32, Object)", "Remove(Object)",
                "RemoveAt(String)", "RemoveAt(Int32)"
            ],
            Getters = ["Count", "SyncRoot"],
            Fields = [],
            Setters = []
        },
        [typeof(SnowflakeDbSessionPool)] = new()
        {
            Constructors = [],
            Methods =
            [
                "GetPooling()", "GetMinPoolSize()", "GetMaxPoolSize()", "GetCurrentPoolSize()", "GetExpirationTimeout()", "GetConnectionTimeout()",
                "GetWaitForIdleSessionTimeout()", "ClearPool()", "GetChangedSession()"
            ],
            Getters = [],
            Fields = [],
            Setters = []
        },
        [typeof(SnowflakeDbTransaction)] = new()
        {
            Constructors = [".ctor(IsolationLevel, SnowflakeDbConnection)"],
            Methods = ["Commit()", "Rollback()"],
            Getters = ["IsolationLevel"],
            Setters = [],
            Fields = []
        },
        [typeof(SnowflakeObject)] = new()
        {
            Constructors = [".ctor()"],
            Methods = [],
            Getters = ["ConstructionMethod"],
            Setters = ["ConstructionMethod"],
            Fields = []
        },
        [typeof(SnowflakeObjectConstructionMethod)] = new()
        {
            Constructors = [],
            Methods = [],
            Getters = [],
            Fields = ["PROPERTIES_ORDER", "PROPERTIES_NAMES", "CONSTRUCTOR"],
            Setters = []
        },
        [typeof(SFConfiguration)] = new()
        {
            Constructors = [],
            Methods = ["Instance()", "GetChunkParserVersion()", "GetChunkDownloaderVersion()"],
            Getters = [],
            Setters = [],
            Fields = ["ChunkDownloaderVersion", "ChunkParserVersion"]
        },
        [typeof(SslProtocolsExtensions)] = new()
        { Constructors = [], Methods = ["FromString(String)"], Getters = [], Fields = ["Tls13"], Setters = [] },
        [typeof(SFEncryptionMetadata)] = new()
        {
            Constructors = [".ctor()"],
            Methods = [],
            Getters = ["iv", "key", "aad", "keyIV", "keyAad", "matDesc"],
            Setters = ["iv", "key", "aad", "keyIV", "keyAad", "matDesc"],
            Fields = []
        },
        [typeof(QueryStatus)] = new()
        {
            Constructors = [],
            Methods = [],
            Getters = [],
            Setters = [],
            Fields =
            [
                "NoData", "Running", "Aborting", "Success", "FailedWithError", "Aborted", "Queued", "FailedWithIncident", "Disconnected",
                "ResumingWarehouse", "QueuedReparingWarehouse", "Restarted", "Blocked"
            ]
        },
        [typeof(ResultFormat)] = new() { Constructors = [], Methods = [], Getters = [], Fields = ["JSON", "ARROW"], Setters = [] },
        [typeof(SFDataType)] = new()
        {
            Constructors = [],
            Methods = [],
            Getters = [],
            Setters = [],
            Fields =
            [
                "None", "FIXED", "REAL", "TEXT", "DATE", "VARIANT", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "OBJECT", "BINARY",
                "TIME", "BOOLEAN", "ARRAY", "MAP", "VECTOR", "DECFLOAT"
            ]
        },
        [typeof(SFError)] = new()
        {
            Constructors = [],
            Methods = [],
            Getters = [],
            Setters = [],
            Fields =
            [
                "INTERNAL_ERROR", "COLUMN_INDEX_OUT_OF_BOUND", "INVALID_DATA_CONVERSION", "STATEMENT_ALREADY_RUNNING_QUERY",
                "QUERY_CANCELLED", "MISSING_CONNECTION_PROPERTY", "REQUEST_TIMEOUT", "INVALID_CONNECTION_STRING", "UNSUPPORTED_FEATURE",
                "DATA_READER_ALREADY_CLOSED", "UNKNOWN_AUTHENTICATOR", "UNSUPPORTED_PLATFORM", "IDP_SSO_TOKEN_URL_MISMATCH",
                "IDP_SAML_POSTBACK_NOTFOUND", "IDP_SAML_POSTBACK_INVALID", "BROWSER_RESPONSE_WRONG_METHOD", "BROWSER_RESPONSE_INVALID_PREFIX",
                "JWT_ERROR_READING_PK", "UNSUPPORTED_DOTNET_TYPE", "UNSUPPORTED_SNOWFLAKE_TYPE_FOR_PARAM", "INVALID_CONNECTION_PARAMETER_VALUE",
                "INVALID_BROWSER_URL", "BROWSER_RESPONSE_TIMEOUT", "IO_ERROR_ON_GETPUT_COMMAND", "EXECUTE_COMMAND_ON_CLOSED_CONNECTION",
                "INCONSISTENT_RESULT_ERROR", "STRUCTURED_TYPE_READ_ERROR", "STRUCTURED_TYPE_READ_DETAILED_ERROR", "BROWSER_RESPONSE_ERROR",
                "OAUTH_TOKEN_REQUEST_ERROR", "EXPERIMENTAL_AUTHENTICATION_DISABLED", "WIF_ATTESTATION_ERROR", "SESSION_GONE", "EXT_AUTHN_DENIED",
                "EXT_AUTHN_LOCKED", "EXT_AUTHN_TIMEOUT", "EXT_AUTHN_INVALID", "EXT_AUTHN_EXCEPTION", "EXT_OAUTH_ACCESS_TOKEN_EXPIRED",
                "EXT_OAUTH_ACCESS_TOKEN_INVALID", "ID_TOKEN_INVALID"
            ]
        },
        [typeof(ChangedSessionBehavior)] =
            new() { Constructors = [], Methods = [], Getters = [], Fields = ["OriginalPool", "Destroy"], Setters = [] },
        [typeof(ActivityStarter)] =
            new() { Constructors = [], Methods = ["StartActivity(SnowflakeDbCommand, String)"], Getters = [], Fields = [], Setters = [] },
        [typeof(SessionTelemetryModuleFacade)] = new()
        { Constructors = [], Methods = ["SetFlushSize(Int32)", "SetFlushInterval(Int32)"], Getters = [], Fields = [], Setters = [] },
    };
}
