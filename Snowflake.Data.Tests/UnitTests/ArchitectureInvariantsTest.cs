using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.UnitTests;

public sealed class ArchitectureInvariantsTest
{
    private static readonly string s_solutionRoot = FindSolutionRoot();
    private static readonly string s_productionSourceDir = Path.Combine(s_solutionRoot, "Snowflake.Data");

    private static readonly Assembly s_testAssembly = typeof(ArchitectureInvariantsTest).Assembly;

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
}
