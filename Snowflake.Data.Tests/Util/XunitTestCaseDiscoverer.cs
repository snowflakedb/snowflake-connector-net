using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

#if !NET8_0_OR_GREATER
using Xunit.Abstractions;

// TODO theories, polish
public sealed class XunitTestCaseDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly FactDiscoverer _decorated;

    public XunitTestCaseDiscoverer(IMessageSink diagnosticMessageSink)
    {
        _decorated = new FactDiscoverer(diagnosticMessageSink);
    }

    public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod,
        IAttributeInfo factAttribute) =>
        _decorated.Discover(discoveryOptions, testMethod, factAttribute).Select(x => new XunitTestCaseDecorator(x));
}

public sealed class XunitTestCaseDecorator : LongLivedMarshalByRefObject, IXunitTestCase
{
    private readonly IXunitTestCase _xunitTestCaseImplementation;

    public XunitTestCaseDecorator()
    {
    }

    public XunitTestCaseDecorator(IXunitTestCase xunitTestCaseImplementation)
    {
        _xunitTestCaseImplementation = xunitTestCaseImplementation;
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
        var messageBusDecorator = new MessageBusDecorator(messageBus);
        var baseResult = await _xunitTestCaseImplementation
            .RunAsync(diagnosticMessageSink, messageBusDecorator, constructorArguments, aggregator, cancellationTokenSource).ConfigureAwait(false);
        baseResult.Failed -= messageBusDecorator.SkippedCount;
        baseResult.Skipped += messageBusDecorator.SkippedCount;
        return baseResult;
    }

    public Exception InitializationException => _xunitTestCaseImplementation.InitializationException;

    public IMethodInfo Method => _xunitTestCaseImplementation.Method;

    public int Timeout => _xunitTestCaseImplementation.Timeout;
}

public class MessageBusDecorator : IMessageBus
{
    private const string XunitSdkSkipException = "Xunit.Sdk.SkipException";
    private readonly IMessageBus _messageBusImplementation;

    public int SkippedCount { get; private set; }

    public MessageBusDecorator(IMessageBus messageBusImplementation)
    {
        _messageBusImplementation = messageBusImplementation;
    }

    public void Dispose() => _messageBusImplementation.Dispose();

    public bool QueueMessage(IMessageSinkMessage message)
    {
        if (message is not ITestFailed testFailed)
            return _messageBusImplementation.QueueMessage(message);

        var anySkipped = testFailed.ExceptionTypes.Select((x, i) => (x, i)).FirstOrDefault(x => XunitSdkSkipException.Equals(x.Item1));
        if (anySkipped != default)
        {
            SkippedCount++;
            var skipReason = testFailed.Messages[anySkipped.Item2];
            var skippedMessage = new TestSkipped(testFailed.Test, skipReason);
            return _messageBusImplementation.QueueMessage(skippedMessage);
        }

        return _messageBusImplementation.QueueMessage(message);
    }
}

#endif
