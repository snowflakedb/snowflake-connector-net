using System.Collections.Generic;
using System.Linq;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.Util;

#if NET8_0_OR_GREATER
using System.Threading.Tasks;
using Xunit.v3;

// TODO theories, polish
public sealed class XunitTestCaseDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly FactDiscoverer _decorated;

    public XunitTestCaseDiscoverer()
    {
        _decorated = new FactDiscoverer();
    }

    public async ValueTask<IReadOnlyCollection<IXunitTestCase>> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, IXunitTestMethod testMethod, IFactAttribute factAttribute)
    {
        var cases = await _decorated.Discover(discoveryOptions, testMethod, factAttribute);
        return cases.Select(x => new XunitTestCaseDecorator(x)).ToList();
    }
}

#else
using Xunit.Abstractions;

public sealed class XunitTestCaseDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly FactDiscoverer _decorated;

    public XunitTestCaseDiscoverer(IMessageSink diagnosticMessageSink)
    {
        _decorated = new FactDiscoverer(diagnosticMessageSink);
    }

    public IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions, ITestMethod testMethod, IAttributeInfo factAttribute) =>
        _decorated.Discover(discoveryOptions, testMethod, factAttribute).Select(x => new XunitTestCaseDecorator(x));
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

