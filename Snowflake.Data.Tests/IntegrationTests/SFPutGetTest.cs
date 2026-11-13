using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO.Compression;
using System.Text;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
using TaskOrValueTask = System.Threading.Tasks.ValueTask;
#else
using TaskOrValueTask = System.Threading.Tasks.Task;
#endif

namespace Snowflake.Data.Tests.IntegrationTests;

using Xunit;
using System;
using System.Threading;
using System.IO;
using System.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.FileTransfer;

public sealed class SFPutGetTestFixture : IDisposable
{
    internal string OutputDirectory { get; }

    public SFPutGetTestFixture()
    {
        OutputDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(OutputDirectory);
    }

    public void Dispose()
    {
        Directory.Delete(OutputDirectory, true);
    }
}

public class SFPutGetTest : SFBaseTestAsync, IClassFixture<SFPutGetTestFixture>, IDisposable
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFPutGetTest(SFBaseTestAsyncFixture fixture, SFPutGetTestFixture fixture2) : base(fixture)
    {
        _fixture = fixture;
        _sOutputDirectory = fixture2.OutputDirectory;
        InitializeAsync().GetAwaiter().GetResult();
    }

    private const long NumberOfRows = 4;
    private static readonly string[] s_colName = { "C1", "C2", "C3" };
    private static readonly string[] s_colData = { "FIRST", "SECOND", "THIRD" };
    private readonly string _sOutputDirectory;

    private string _tSchemaName;
    private string _tTableName;
    private string _tStageName;
    private string _tStageNameSse; // server side encryption without client side encryption
    private string _tFileName;
    private string _tOutputFileName;
    private string _tInputFilePath;
    private string _tOutputFilePath;
    private string _tInternalStagePath;
    private StageType _tStageType;
    private string _tSourceCompressionType;
    private string _tDestCompressionType;
    private bool _tAutoCompress;
    private List<string> _tFilesToDelete;

    public enum StageType
    {
        User,
        Table,
        Named
    }

    public void Dispose()
    {
        DisposeAsync().GetAwaiter().GetResult();
    }

    [SFFact(RetriesCount = RetriesCount.Thrice)]
    public async Task TestPutFileAsteriskWildcard()
    {
        var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
        var files = new List<string> {
            $"{absolutePathPrefix}_one.csv",
            $"{absolutePathPrefix}_two.csv",
            $"{absolutePathPrefix}_three.csv"
        };
        PrepareFileData(files);

        // Set the PUT query variables
        _tInputFilePath = $"{absolutePathPrefix}*";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutFileAsteriskWildcardWithExtension()
    {
        var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
        var files = new List<string> {
            $"{absolutePathPrefix}_one.csv",
            $"{absolutePathPrefix}_two.csv",
            $"{absolutePathPrefix}_three.csv"
        };
        PrepareFileData(files);
        // Create file with the same name structure but with a different extension
        PrepareFileData($"{absolutePathPrefix}_four.txt");

        // Set the PUT query variables
        _tInputFilePath = $"{absolutePathPrefix}*.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact(RetriesCount = RetriesCount.Thrice)]
    public async Task TestPutFileQuestionMarkWildcard()
    {
        var absolutePathPrefix = $"{Path.GetTempPath()}{Guid.NewGuid()}";
        var files = new List<string> {
            $"{absolutePathPrefix}_1.csv",
            $"{absolutePathPrefix}_2.csv",
            $"{absolutePathPrefix}_3.csv"
        };
        PrepareFileData(files);
        // Create file which should be omitted during the transfer
        PrepareFileData($"{absolutePathPrefix}_four.csv");

        // Set the PUT query variables
        _tInputFilePath = $"{absolutePathPrefix}_?.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutFileRelativePathWithoutDirectory()
    {
        // Set the PUT query variables
        _tInputFilePath = $"{Guid.NewGuid()}_1.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        PrepareFileData(_tInputFilePath);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, new List<string> { _tInputFilePath }, _tInternalStagePath);
        }
    }

    [SFTheory]
    [InlineData("GET")]
    [InlineData("PUT")]
    public async Task TestPutGetOnClosedConnectionThrowsWithoutQueryId(string command)
    {
        _tInputFilePath = "unexisting_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            // await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false); // intentionally closed
            var snowflakeDbException = await Assert.ThrowsAsync<SnowflakeDbException>(async () => await ProcessFileAsync(command, conn).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.NotNull(snowflakeDbException);
            Assert.Null(snowflakeDbException.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.EXECUTE_COMMAND_ON_CLOSED_CONNECTION);
        }
    }

    [SFFact]
    public async Task TestGetNonExistentFileReturnsFalseAndDoesNotThrow()
    {
        _tInputFilePath = "non_existent_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var sql = $"GET {_tInternalStagePath}/{_tFileName} file://{_sOutputDirectory}";
            using (var command = conn.CreateCommand())
            {
                command.CommandText = sql;
                var reader = command.ExecuteReader();
                Assert.False(await reader.ReadAsync().ConfigureAwait(false));
            }
        }
    }

    [SFFact]
    public async Task TestPutNonExistentFileThrowsWithQueryId()
    {
        _tInputFilePath = "non_existent_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
            Assert.NotNull(snowflakeDbException);
            Assert.NotNull(snowflakeDbException.QueryId);
            SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.IO_ERROR_ON_GETPUT_COMMAND);
        }
    }

    [SFFact]
    public async Task TestPutFileProvidesQueryIdOnFailure()
    {
        // Arrange
        // Set the PUT query variables but do not create a file
        _tInputFilePath = "unexisting_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
            var queryId = snowflakeDbException.QueryId;

            // Assert
            Assert.NotEmpty(queryId);
            Guid.Parse(queryId);
            SnowflakeDbExceptionAssert.HasErrorCode(snowflakeDbException, SFError.IO_ERROR_ON_GETPUT_COMMAND);
        }
    }

    [SFFact]
    public async Task TestPutFileWithSyntaxErrorProvidesQueryIdOnFailure()
    {
        // Arrange
        // Set the PUT query variables but do not create a file
        _tInputFilePath = "unexisting_file.csv SOME CODE FORCING SYNTAX ERROR";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var snowflakeDbException = Assert.Throws<SnowflakeDbException>(() => PutFile(conn));
            var queryId = snowflakeDbException.QueryId;

            // Assert
            Assert.NotEmpty(queryId);
            Guid.Parse(queryId);
            Assert.Equal(1003, snowflakeDbException.ErrorCode);
            Assert.Null(snowflakeDbException.InnerException);
        }
    }

    [SFFact]
    public async Task TestPutFileProvidesQueryIdOnSuccess()
    {
        // Arrange
        // Set the PUT query variables
        _tInputFilePath = $"{Guid.NewGuid()}_1.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";
        PrepareFileData(_tInputFilePath);

        // Act
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            var queryId = PutFile(conn);

            // Assert
            Assert.NotNull(queryId);
            Guid.Parse(queryId);
            VerifyFilesAreUploaded(conn, new List<string> { _tInputFilePath }, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutFileRelativePathWithDirectory()
    {
        var guid = Guid.NewGuid();
        var relativePath = $"{guid}";
        Directory.CreateDirectory(relativePath);

        // Set the PUT query variables
        _tInputFilePath = $"{relativePath}{Path.DirectorySeparatorChar}{guid}_1.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        PrepareFileData(_tInputFilePath);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, new List<string> { _tInputFilePath }, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutFileRelativePathAsteriskWildcard()
    {
        var relativePath = $"{Guid.NewGuid()}";
        var files = new List<string> {
            $"{relativePath}_one.csv",
            $"{relativePath}_two.csv",
            $"{relativePath}_three.csv"
        };
        PrepareFileData(files);

        // Set the PUT query variables
        _tInputFilePath = $"{relativePath}*";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact(SkipCondition.SkipOnCloudGCP)]
    // presigned url is enabled on CI so we need to disable the test
    // it should be enabled when downscoped credential is the default option
    public async Task TestPutFileWithoutOverwriteFlagSkipsSecondUpload()
    {
        // Set the PUT query variables
        _tInputFilePath = $"{Guid.NewGuid()}.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        PrepareFileData(_tInputFilePath);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn, expectedStatus: ResultStatus.UPLOADED);
            VerifyFilesAreUploaded(conn, new List<string> { _tInputFilePath }, _tInternalStagePath);
            PutFile(conn, expectedStatus: ResultStatus.SKIPPED);
        }
    }

    [SFFact]
    public async Task TestPutFileWithOverwriteFlagRunsSecondUpload()
    {
        var overwriteAttribute = "OVERWRITE=TRUE";

        // Set the PUT query variables
        _tInputFilePath = $"{Guid.NewGuid()}.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        PrepareFileData(_tInputFilePath);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn, overwriteAttribute, expectedStatus: ResultStatus.UPLOADED);
            VerifyFilesAreUploaded(conn, new List<string> { _tInputFilePath }, _tInternalStagePath);
            PutFile(conn, overwriteAttribute, expectedStatus: ResultStatus.UPLOADED);
        }
    }

    [SFFact]
    public async Task TestPutDirectoryAsteriskWildcard()
    {
        // Prepare the data files to be copied
        var guid = Guid.NewGuid();
        var path = $"{Path.GetTempPath()}{guid}";
        var files = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var filePath = $"{path}_{i}";
            Directory.CreateDirectory(filePath);
            var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
            PrepareFileData(fullPath);
            files.Add(fullPath);
        }

        // Set the PUT query variables
        _tInputFilePath = $"{path}*{Path.DirectorySeparatorChar}*";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutDirectoryQuestionMarkWildcard()
    {
        // Prepare the data files to be copied
        var guid = Guid.NewGuid();
        var path = $"{Path.GetTempPath()}{guid}";
        var files = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var filePath = $"{path}_{i}";
            Directory.CreateDirectory(filePath);
            var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
            PrepareFileData(fullPath);
            files.Add(fullPath);
        }

        // Set the PUT query variables
        _tInputFilePath = $"{path}_?{Path.DirectorySeparatorChar}{guid}_?_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFFact]
    public async Task TestPutDirectoryMixedWildcard()
    {
        // Prepare the data files to be copied
        var guid = Guid.NewGuid();
        var path = $"{Path.GetTempPath()}{guid}";
        var files = new List<string>();
        for (var i = 0; i < 3; i++)
        {
            var filePath = $"{path}_{i}";
            Directory.CreateDirectory(filePath);
            var fullPath = $"{filePath}{Path.DirectorySeparatorChar}{guid}_{i}_file.csv";
            PrepareFileData(fullPath);
            files.Add(fullPath);
        }

        // Set the PUT query variables
        _tInputFilePath = $"{path}_*{Path.DirectorySeparatorChar}{guid}_?_file.csv";
        _tInternalStagePath = $"@{_tSchemaName}.{_tStageName}";

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            VerifyFilesAreUploaded(conn, files, _tInternalStagePath);
        }
    }

    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(PutGetCommandTestCases))]
    public async Task TestPutGetCommand(
        string sourceFileCompressionType,
        StageType stageType,
        string stagePath,
        bool autoCompress)
    {
        PrepareTest(sourceFileCompressionType, stageType, stagePath, autoCompress);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            await CopyIntoTableAsync(conn).ConfigureAwait(false);
            await GetFileAsync(conn).ConfigureAwait(false);
        }
    }

    [SFTheory, MemberData(nameof(PutGetCommandForNamedStageWithoutClientSideEncryptionTestCases))]
    public async Task TestPutGetCommandForNamedStageWithoutClientSideEncryption(
        string sourceFileCompressionType,
        string stagePath,
        bool autoCompress)
    {
        PrepareTest(sourceFileCompressionType, StageType.Named, stagePath, autoCompress, false);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn);
            await CopyIntoTableAsync(conn).ConfigureAwait(false);
            await GetFileAsync(conn).ConfigureAwait(false);
        }
    }

    // Test small file upload/download with GCS_USE_DOWNSCOPED_CREDENTIAL set to true
    [SFTheory(SkipCondition.SkipOnCloudAWS | SkipCondition.SkipOnCloudAzure), MemberData(nameof(PutGetGcsDownscopedCredentialTestCases))]
    public async Task TestPutGetGcsDownscopedCredential(
        StageType stageType,
        string stagePath)
    {
        PrepareTest(null, stageType, stagePath, false);

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString + ";GCS_USE_DOWNSCOPED_CREDENTIAL=true"))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            PutFile(conn);
            await CopyIntoTableAsync(conn).ConfigureAwait(false);
            await GetFileAsync(conn).ConfigureAwait(false);
        }
    }

    [SFTheory(SkipCondition.SkipOnJenkins), MemberData(nameof(PutGetFileWithSpaceAndSingleQuoteTestCases))]
    public async Task TestPutGetFileWithSpaceAndSingleQuote(
        StageType stageType,
        string stagePath)
    {
        PrepareTest(null, stageType, stagePath, false, true, true);
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            PutFile(conn, "", ResultStatus.UPLOADED, true);
            await CopyIntoTableAsync(conn, true).ConfigureAwait(false);
            await GetFileAsync(conn, true).ConfigureAwait(false);
        }
    }

    public static IEnumerable<object[]> PutGetCommandTestCases()
    {
        var compressionTypes = new[] { "none", "gzip", "bzip2", "brotli", "deflate", "raw_deflate", "zstd" };
        var stageTypes = (StageType[])Enum.GetValues(typeof(StageType));
        var stagePaths = new[] { "", "/TEST_PATH", "/DEEP/TEST_PATH" };
        foreach (var compressionType in compressionTypes)
            foreach (var stageType in stageTypes)
                foreach (var stagePath in stagePaths)
                    foreach (var autoCompress in new[] { false, true })
                        yield return new object[] { compressionType, stageType, stagePath, autoCompress };
    }

    public static IEnumerable<object[]> PutGetCommandForNamedStageWithoutClientSideEncryptionTestCases()
    {
        var compressionTypes = new[] { "none", "gzip" };
        var stagePaths = new[] { "", "/DEEP/TEST_PATH" };
        foreach (var compressionType in compressionTypes)
            foreach (var stagePath in stagePaths)
                foreach (var autoCompress in new[] { false, true })
                    yield return new object[] { compressionType, stagePath, autoCompress };
    }

    public static IEnumerable<object[]> PutGetFileWithSpaceAndSingleQuoteTestCases()
    {
        var stageTypes = (StageType[])Enum.GetValues(typeof(StageType));
        foreach (var stageType in stageTypes)
            yield return new object[] { stageType, "/STAGE PATH WITH SPACE" };
    }

    public static IEnumerable<object[]> PutGetGcsDownscopedCredentialTestCases()
    {
        var stageTypes = (StageType[])Enum.GetValues(typeof(StageType));
        var stagePaths = new[] { "", "/TEST_PATH" };
        foreach (var stageType in stageTypes)
            foreach (var stagePath in stagePaths)
                yield return new object[] { stageType, stagePath };
    }

    private void PrepareTest(string sourceFileCompressionType, StageType stageType, string stagePath,
        bool autoCompress, bool clientEncryption = true, bool makeFilePathWithSpace = false)
    {
        _tStageType = stageType;
        _tSourceCompressionType = sourceFileCompressionType;
        _tAutoCompress = autoCompress;
        // Prepare temp file name with specified file extension
        _tFileName = Guid.NewGuid() + ".csv" +
                     (_tAutoCompress ? SFFileCompressionTypes.LookUpByName(_tSourceCompressionType).FileExtension : "");
        var sourceFolderWithSpace = $"{Guid.NewGuid()} source file path with space";
        var inputPathBase = makeFilePathWithSpace ?
            Path.Combine(_sOutputDirectory, sourceFolderWithSpace) :
            Path.GetTempPath();
        _tInputFilePath = Path.Combine(inputPathBase, _tFileName);

        if (IsCompressedByTheDriver())
        {
            _tDestCompressionType = "gzip";
            _tOutputFileName = $"{_tFileName}.gz";
        }
        else
        {
            _tDestCompressionType = _tSourceCompressionType;
            _tOutputFileName = _tFileName;
        }
        var destinationFolderWithSpace = $"{Guid.NewGuid()} destination file path with space";
        var outputPathBase = makeFilePathWithSpace ?
            Path.Combine(_sOutputDirectory, destinationFolderWithSpace) :
            _sOutputDirectory;
        _tOutputFilePath = Path.Combine(outputPathBase, _tOutputFileName);
        if (makeFilePathWithSpace)
        {
            Directory.CreateDirectory(inputPathBase);
            Directory.CreateDirectory(outputPathBase);
        }
        _tFilesToDelete.Add(_tOutputFilePath);
        PrepareFileData(_tInputFilePath);

        // Prepare stage name
        switch (_tStageType)
        {
            case StageType.User:
                _tInternalStagePath = $"@~{stagePath}";
                break;
            case StageType.Table:
                _tInternalStagePath = $"@{_tSchemaName}.%{_tTableName}{stagePath}";
                break;
            case StageType.Named:
                _tInternalStagePath = clientEncryption
                    ? $"@{_tSchemaName}.{_tStageName}{stagePath}"
                    : $"@{_tSchemaName}.{_tStageNameSse}{stagePath}";
                break;
        }
    }

    private bool IsCompressedByTheDriver()
    {
        return _tSourceCompressionType == "none" && _tAutoCompress;
    }

    // PUT - upload file from local directory to the stage
    private string PutFile(
        SnowflakeDbConnection conn,
        string additionalAttribute = "",
        ResultStatus expectedStatus = ResultStatus.UPLOADED,
        bool encloseInSingleQuotes = false)
    {
        string queryId;
        using (var command = conn.CreateCommand())
        {
            // Prepare PUT query
            var putQuery = encloseInSingleQuotes ?
                $"PUT 'file://{_tInputFilePath.Replace("\\", "/")}' '{_tInternalStagePath}'" :
                $"PUT file://{_tInputFilePath} {_tInternalStagePath}";
            putQuery += $" AUTO_COMPRESS={(_tAutoCompress ? "TRUE" : "FALSE")}" + $" {additionalAttribute}";
            // Upload file
            command.CommandText = putQuery;
            var reader = command.ExecuteReader();
            try
            {
                Assert.True(reader.Read());
            }
            catch (SnowflakeDbException e)
            {
                // to make sure in a failure case command was set properly with a failed QueryId
                Assert.Equal(e.QueryId, ((SnowflakeDbCommand)command).GetQueryId());
                throw;
            }
            // Checking query id when reader succeeded
            queryId = ((SnowflakeDbDataReader)reader).GetQueryId();
            // Checking if query Id is provided on the command level as well
            Assert.Equal(queryId, ((SnowflakeDbCommand)command).GetQueryId());
            // Check file status
            Assert.Equal(expectedStatus.ToString(),
                reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));
            // Check source and destination compression type
            if (_tAutoCompress)
            {
                Assert.Equal(_tSourceCompressionType,
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                Assert.Equal(_tDestCompressionType,
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
            }
            else
            {
                Assert.Equal(SFFileCompressionTypes.NONE.Name,
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.SourceCompressionType));
                Assert.Equal(SFFileCompressionTypes.NONE.Name,
                    reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.DestinationCompressionType));
            }
            Assert.Null(reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ErrorDetails));
        }
        return queryId;
    }

    // COPY INTO - Copy data from the stage into temp table
    private async Task CopyIntoTableAsync(SnowflakeDbConnection conn, bool encloseInSingleQuotes = false)
    {
        using (var command = conn.CreateCommand())
        {
            switch (_tStageType)
            {
                case StageType.Table:
                    command.CommandText = $"COPY INTO {_tSchemaName}.{_tTableName}";
                    break;
                default:
                    command.CommandText = encloseInSingleQuotes ?
                        $"COPY INTO {_tSchemaName}.{_tTableName} FROM '{_tInternalStagePath}/{_tFileName}'" :
                        $"COPY INTO {_tSchemaName}.{_tTableName} FROM {_tInternalStagePath}/{_tFileName}";
                    break;
            }

            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            // Check contents are correct
            command.CommandText = $"SELECT * FROM {_tSchemaName}.{_tTableName}";
            var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            while (reader.Read())
            {
                for (var i = 0; i < s_colData.Length; i++)
                {
                    Assert.Equal(reader.GetString(i), s_colData[i]);
                }
            }

            // Check row count is correct
            command.CommandText = $"SELECT COUNT(*) FROM {_tSchemaName}.{_tTableName}";
            Assert.Equal(NumberOfRows, (long)await command.ExecuteScalarAsync().ConfigureAwait(false));
        }
    }

    // GET - Download from the stage into local directory
    private async Task GetFileAsync(DbConnection conn, bool encloseInSingleQuotes = false)
    {
        using (var command = conn.CreateCommand())
        {
            // Prepare GET query
            var getQuery = encloseInSingleQuotes ?
                $"GET '{_tInternalStagePath}/{_tFileName}' 'file://{Path.GetDirectoryName(_tOutputFilePath).Replace("\\", "/")}'" :
                $"GET {_tInternalStagePath}/{_tFileName} file://{_sOutputDirectory}";

            // Download file
            command.CommandText = getQuery;
            var reader = command.ExecuteReader();
            Assert.True(reader.Read());

            // Check file status
            Assert.Equal(ResultStatus.DOWNLOADED.ToString(),
                reader.GetString((int)SFResultSet.PutGetResponseRowTypeInfo.ResultStatus));

            // Check file contents
            foreach (var line in ReadOutputFileLines())
            {
                if (string.IsNullOrEmpty(line)) continue;
                var values = line.Split(',');

                for (var i = 0; i < s_colData.Length; i++)
                {
                    Assert.Equal(s_colData[i], values[i]);
                }
            }
        }
    }

    private async Task ProcessFileAsync(string command, SnowflakeDbConnection connection)
    {
        switch (command)
        {
            case "GET":
                await GetFileAsync(connection).ConfigureAwait(false);
                break;
            case "PUT":
                PutFile(connection);
                break;
        }
    }

    private string[] ReadOutputFileLines()
    {
        using (var outputStream = File.OpenRead(_tOutputFilePath))
        {
            using (var processedOutputStream = IsCompressedByTheDriver() ? Unzip(outputStream) : outputStream)
            {
                return SplitLines(processedOutputStream);
            }
        }
    }

    private static Stream Unzip(Stream stream)
    {
        using (var decompressingStream = new GZipStream(stream, CompressionMode.Decompress))
        {
            var decompressedStream = new MemoryStream();
            decompressingStream.CopyTo(decompressedStream);
            return decompressedStream;
        }
    }

    private static string[] SplitLines(Stream stream)
    {
        var bytes = new byte[stream.Length];
        stream.Position = 0;
        var readBytes = stream.Read(bytes, 0, (int)stream.Length);
        Assert.Equal(stream.Length, readBytes);
        return Encoding.UTF8.GetString(bytes).Split('\n');
    }

    private void PrepareFileData(string file)
    {
        // Prepare csv raw data and write to temp files
        var rawDataRow = string.Join(",", s_colData) + "\n";
        var rawData = string.Concat(Enumerable.Repeat(rawDataRow, (int)NumberOfRows));

        using (var stream = FileOperations.Instance.Create(file))
        using (var writer = new StreamWriter(stream))
        {
            writer.Write(rawData);
        }
        _tFilesToDelete.Add(file);
    }

    private void PrepareFileData(List<string> files)
    {
        files.ForEach(PrepareFileData);
    }

    private static void VerifyFilesAreUploaded(DbConnection conn, ICollection<string> files, string stage)
    {
        // Verify that all files have been uploaded
        using (var cmd = conn.CreateCommand())
        {
            var command = $"LIST {stage}";
            cmd.CommandText = command;
            var dbDataReader = cmd.ExecuteReader();
            var dt = new DataTable();
            dt.Load(dbDataReader);
            Assert.Equal(files.Count, dt.Rows.Count);
        }
    }

    private async TaskOrValueTask DisposeAsync()
    {
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            using (var command = conn.CreateCommand())
            {
                // Drop temp stage
                command.CommandText = $"DROP STAGE IF EXISTS {_tSchemaName}.{_tStageName}";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Drop temp table
                command.CommandText = $"DROP TABLE IF EXISTS {_tSchemaName}.{_tTableName}";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        // Delete temp files if necessary
        if (_tFilesToDelete != null)
        {
            foreach (var file in _tFilesToDelete)
            {
                File.Delete(file);
            }
        }
    }

    private async TaskOrValueTask InitializeAsync()
    {
        // Base object's names on on worker thread id
        var suffix = Guid.NewGuid().ToString("N");

        _tSchemaName = _fixture.testConfig.schema;
        _tTableName = $"TABLE_{suffix}";
        _tStageName = $"STAGE_{suffix}";
        _tStageNameSse = $"STAGE_{suffix}_SSE";
        _tFilesToDelete = new List<string>();

        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            using (var command = conn.CreateCommand())
            {
                // Create temp table
                var columnNamesWithTypes = string.Join(",", s_colName.Select(col => col + " STRING"));
                command.CommandText = $"CREATE OR REPLACE TABLE {_tSchemaName}.{_tTableName} ({columnNamesWithTypes})";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Create temp stage
                command.CommandText = $"CREATE OR REPLACE STAGE {_tSchemaName}.{_tStageName}";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                // Create temp stage without client side encryption
                command.CommandText = $"CREATE OR REPLACE STAGE {_tSchemaName}.{_tStageNameSse} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')";
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
