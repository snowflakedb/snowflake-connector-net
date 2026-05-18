using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

#if NET8_0_OR_GREATER
using Xunit.v3;

namespace Snowflake.Data.Tests.Util;

public class SFTestCaseRunner : XunitTestCaseRunnerBase<SFCaseRunnerContext, IXunitTestCase, IXunitTest>
{
    public static SFTestCaseRunner Instance { get; } = new();

    public async ValueTask<RunSummary> Run(
        int maxRetries,
        IXunitTestCase testCase,
        IMessageBus messageBus,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource,
        string displayName,
        string skipReason,
        ExplicitOption explicitOption,
        object[] constructorArguments)
    {
        var tests = await aggregator.RunAsync(testCase.CreateTests, []);

        if (aggregator.ToException() is { } ex)
        {
            if (ex.Message.StartsWith(DynamicSkipToken.Value, StringComparison.Ordinal))
                return XunitRunnerHelper.SkipTestCases(
                    messageBus,
                    cancellationTokenSource,
                    [testCase],
                    ex.Message.Substring(DynamicSkipToken.Value.Length),
                    sendTestCaseMessages: false
                );

            return XunitRunnerHelper.FailTestCases(
                messageBus,
                cancellationTokenSource,
                [testCase],
                ex,
                sendTestCaseMessages: false
            );
        }

        await using var ctxt = new SFCaseRunnerContext(maxRetries, testCase, tests, messageBus, aggregator, cancellationTokenSource,
            displayName, skipReason, explicitOption, constructorArguments);
        await ctxt.InitializeAsync();

        return await Run(ctxt);
    }

    protected override async ValueTask<RunSummary> RunTest(
        SFCaseRunnerContext ctxt,
        IXunitTest test)
    {
        var runCount = 0;
        var maxRetries = ctxt.MaxRetries;

        if (maxRetries < 1)
            maxRetries = 3;

        for(;;)
        {
            var delayedMessageBus = new SFDelayedMessageBus(ctxt.MessageBus);
            var aggregator = ctxt.Aggregator.Clone();
            var result = await XunitTestRunner.Instance.Run(
                test,
                delayedMessageBus,
                ctxt.ConstructorArguments,
                ctxt.ExplicitOption,
                aggregator,
                ctxt.CancellationTokenSource,
                ctxt.BeforeAfterTestAttributes
            );

            if (!(aggregator.HasExceptions || result.Failed != 0) || ++runCount >= maxRetries)
            {
                delayedMessageBus.Dispose();
                return result;
            }

            TestContext.Current.SendDiagnosticMessage("Execution of '{0}' ended with a failure (attempt #{1}), retrying...", test.TestDisplayName, runCount);
            ctxt.Aggregator.Clear();
        }
    }
}

public class SFCaseRunnerContext(
    int maxRetries,
    IXunitTestCase testCase,
    IReadOnlyCollection<IXunitTest> tests,
    IMessageBus messageBus,
    ExceptionAggregator aggregator,
    CancellationTokenSource cancellationTokenSource,
    string displayName,
    string skipReason,
    ExplicitOption explicitOption,
    object[] constructorArguments) :
    XunitTestCaseRunnerBaseContext<IXunitTestCase, IXunitTest>(testCase, tests, messageBus, aggregator, cancellationTokenSource, displayName,
        skipReason, explicitOption, constructorArguments)
{
    public int MaxRetries { get; } = maxRetries;
}
#else
#endif
