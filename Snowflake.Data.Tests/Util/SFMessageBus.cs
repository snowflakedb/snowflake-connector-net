using System.Collections.Generic;
using Xunit.Sdk;

#if NET8_0_OR_GREATER

using Xunit.v3;

namespace Snowflake.Data.Tests.Util;

public sealed class SFDelayedMessageBus(IMessageBus innerBus) : IMessageBus
{
    private readonly List<IMessageSinkMessage> _messages = [];

    public bool QueueMessage(IMessageSinkMessage message)
    {
        _messages.Add(message);
        return true;
    }

    public void Dispose()
    {
        foreach (var message in _messages)
            innerBus.QueueMessage(message);
    }
}

#else
using System.Diagnostics;
using System.Linq;
using Xunit.Abstractions;
using Snowflake.Data.Tests.IntegrationTests;

namespace Snowflake.Data.Tests.Util;

public class SFMessageBus : IMessageBus
{
    private const string XunitSdkSkipException = "Xunit.Sdk.SkipException";
    private readonly IMessageBus _messageBusImplementation;
    private int _retriesCountRemaining;
    private readonly Queue<IMessageSinkMessage> _messages = [];
    private bool _isMessageProcessingDelayed;

    public int SkippedCount { get; private set; }

    public SFMessageBus(IMessageBus messageBusImplementation, int retriesCount)
    {
        _messageBusImplementation = messageBusImplementation;
        _retriesCountRemaining = retriesCount;
        _isMessageProcessingDelayed = retriesCount > 0;
    }

    public void Dispose()
    {
        _messageBusImplementation.Dispose();
    }

    public bool QueueMessage(IMessageSinkMessage message)
    {
        if (message is TestCaseStarting)
            _messages.Clear();

        if (message is ITestPassed or ITestSkipped)
        {
            foreach (var delayedMessage in _messages)
            {
                _isMessageProcessingDelayed = false;
                _messageBusImplementation.QueueMessage(delayedMessage);
            }
        }

        if (message is not ITestFailed testFailed)
            return DelayQueueMessage(message);

        var anySkipped = testFailed.ExceptionTypes.Select((x, i) => (x, i)).FirstOrDefault(x => XunitSdkSkipException.Equals(x.Item1));
        if (anySkipped != default)
        {
            SkippedCount++;
            var skipReason = testFailed.Messages[anySkipped.Item2];
            var skippedMessage = new TestSkipped(testFailed.Test, skipReason);

            while (_messages.TryDequeue(out var delayedMessage))
                _messageBusImplementation.QueueMessage(delayedMessage);

            return _messageBusImplementation.QueueMessage(skippedMessage);
        }

        if (_retriesCountRemaining-- <= 0)
            _isMessageProcessingDelayed = false;

        return DelayQueueMessage(message);
    }

    private bool DelayQueueMessage(IMessageSinkMessage message)
    {
        if (_isMessageProcessingDelayed)
        {
            _messages.Enqueue(message);
            return true;
        }

        return _messageBusImplementation.QueueMessage(message);
    }
}
#endif
