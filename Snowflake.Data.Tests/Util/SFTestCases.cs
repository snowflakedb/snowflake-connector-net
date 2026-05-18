using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Xunit.Sdk;

#if NET8_0_OR_GREATER
using System.ComponentModel;
using System.Threading;
using Xunit.v3;

namespace Snowflake.Data.Tests.Util;

public class SFTestCase : XunitTestCase, ISelfExecutingXunitTestCase
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
    public SFTestCase() { }

    public SFTestCase(
        int maxRetries,
        IXunitTestMethod testMethod,
        string testCaseDisplayName,
        string uniqueID,
        bool @explicit,
        Type[] skipExceptions = null,
        string skipReason = null,
        Type skipType = null,
        string skipUnless = null,
        string skipWhen = null,
        Dictionary<string, HashSet<string>> traits = null,
        object[] testMethodArguments = null,
        string sourceFilePath = null,
        int? sourceLineNumber = null,
        int? timeout = null) :
            base(testMethod, testCaseDisplayName, uniqueID, @explicit, skipExceptions, skipReason, skipType, skipUnless, skipWhen, traits, testMethodArguments, sourceFilePath, sourceLineNumber, timeout)
    {
        MaxRetries = maxRetries;
    }

    public int MaxRetries { get; private set; }

    protected override void Deserialize(IXunitSerializationInfo info)
    {
        base.Deserialize(info);
        MaxRetries = info.GetValue<int>(nameof(MaxRetries));
    }

    public ValueTask<RunSummary> Run(
        ExplicitOption explicitOption,
        IMessageBus messageBus,
        object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource) =>
            SFTestCaseRunner.Instance.Run(
                MaxRetries,
                this,
                messageBus,
                aggregator.Clone(),
                cancellationTokenSource,
                TestCaseDisplayName,
                SkipReason,
                explicitOption,
                constructorArguments
            );

    protected override void Serialize(IXunitSerializationInfo info)
    {
        base.Serialize(info);

        info.AddValue(nameof(MaxRetries), MaxRetries);
    }
}

// This class is used when pre-enumeration is disabled, or when the theory data was not serializable.
public class SFEnumeratedTestCase : XunitDelayEnumeratedTheoryTestCase, ISelfExecutingXunitTestCase
{
    [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
    public SFEnumeratedTestCase()
    { }

    public SFEnumeratedTestCase(
        int maxRetries,
        IXunitTestMethod testMethod,
        string testCaseDisplayName,
        string uniqueID,
        bool @explicit,
        bool skipTestWithoutData,
        Type[] skipExceptions = null,
        string skipReason = null,
        Type skipType = null,
        string skipUnless = null,
        string skipWhen = null,
        Dictionary<string, HashSet<string>> traits = null,
        string sourceFilePath = null,
        int? sourceLineNumber = null,
        int? timeout = null) :
            base(testMethod, testCaseDisplayName, uniqueID, @explicit, skipTestWithoutData, skipExceptions, skipReason, skipType, skipUnless, skipWhen, traits, sourceFilePath, sourceLineNumber, timeout)
    {
        MaxRetries = maxRetries;
    }

    public int MaxRetries { get; private set; }

    protected override void Deserialize(IXunitSerializationInfo info)
    {
        base.Deserialize(info);

        MaxRetries = info.GetValue<int>(nameof(MaxRetries));
    }

    public ValueTask<RunSummary> Run(
        ExplicitOption explicitOption,
        IMessageBus messageBus,
        object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource) =>
            SFTestCaseRunner.Instance.Run(
                MaxRetries,
                this,
                messageBus,
                aggregator.Clone(),
                cancellationTokenSource,
                TestCaseDisplayName,
                SkipReason,
                explicitOption,
                constructorArguments
            );

    protected override void Serialize(IXunitSerializationInfo info)
    {
        base.Serialize(info);

        info.AddValue(nameof(MaxRetries), MaxRetries);
    }
}

#else
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace Snowflake.Data.Tests.Util;

public sealed class SFTestCases : LongLivedMarshalByRefObject, IXunitTestCase
{
    private readonly IXunitTestCase _xunitTestCaseImplementation;
    private readonly int _maxRetriesCount;

    [Obsolete("Used for deserialization")]
    public SFTestCases()
    {
    }

    public SFTestCases(IXunitTestCase xunitTestCaseImplementation, int maxRetriesCount)
    {
        _xunitTestCaseImplementation = xunitTestCaseImplementation;
        _maxRetriesCount = maxRetriesCount;
    }

    public void Deserialize(IXunitSerializationInfo info) => _xunitTestCaseImplementation.Deserialize(info);

    public void Serialize(IXunitSerializationInfo info) => _xunitTestCaseImplementation.Serialize(info);

    public string DisplayName => _xunitTestCaseImplementation.DisplayName;

    public string SkipReason => _xunitTestCaseImplementation.SkipReason;

    public ISourceInformation SourceInformation
    {
        get => _xunitTestCaseImplementation.SourceInformation;
        set => _xunitTestCaseImplementation.SourceInformation = value;
    }

    public ITestMethod TestMethod => _xunitTestCaseImplementation.TestMethod;

    public object[] TestMethodArguments => _xunitTestCaseImplementation.TestMethodArguments;

    public Dictionary<string, List<string>> Traits => _xunitTestCaseImplementation.Traits;

    public string UniqueID => _xunitTestCaseImplementation.UniqueID;

    public async Task<RunSummary> RunAsync(IMessageSink diagnosticMessageSink, IMessageBus messageBus, object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
    {
        var messageBusDecorator = new SFMessageBus(messageBus, _maxRetriesCount);

        var retriesCount = 0;
        RunSummary baseResult;
        do
        {
            baseResult = await _xunitTestCaseImplementation
                .RunAsync(diagnosticMessageSink, messageBusDecorator, constructorArguments, aggregator, cancellationTokenSource)
                .ConfigureAwait(false);
        } while (retriesCount++ < _maxRetriesCount && baseResult.Failed > 0);

        baseResult.Failed -= messageBusDecorator.SkippedCount;
        baseResult.Skipped += messageBusDecorator.SkippedCount;
        return baseResult;
    }

    public Exception InitializationException => _xunitTestCaseImplementation.InitializationException;

    public IMethodInfo Method => _xunitTestCaseImplementation.Method;

    public int Timeout => _xunitTestCaseImplementation.Timeout;
}
#endif
