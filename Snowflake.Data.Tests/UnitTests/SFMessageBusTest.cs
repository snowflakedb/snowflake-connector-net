#if !NET8_0_OR_GREATER
using Moq;
using Snowflake.Data.Tests.Util;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFMessageBusTest
    {
        [SFFact]
        public void TestQueueMessage_WithRetries_DelaysMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 2);
            var message = new Mock<IMessageSinkMessage>();

            // act
            bus.QueueMessage(message.Object);

            // assert - message should be delayed, not forwarded to inner bus
            innerBus.Verify(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>()), Times.Never);
        }

        [SFFact]
        public void TestQueueMessage_WithoutRetries_ForwardsImmediately()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 0);
            var message = new Mock<IMessageSinkMessage>();

            // act
            bus.QueueMessage(message.Object);

            // assert
            innerBus.Verify(b => b.QueueMessage(message.Object), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestCaseStarting_ClearsQueue()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 2);
            var genericMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(genericMessage.Object);

            var testCase = new Mock<ITestCase> { DefaultValue = DefaultValue.Mock };
            var startingMessage = new TestCaseStarting(testCase.Object);

            // act
            bus.QueueMessage(startingMessage);

            // Now queue a pass message - should only flush messages after the TestCaseStarting
            var passMessage = CreateTestPassedMessage();
            bus.QueueMessage(passMessage);

            // assert - only the TestCaseStarting and pass message should be forwarded,
            // not the generic message that was cleared
            innerBus.Verify(b => b.QueueMessage(genericMessage.Object), Times.Never);
        }

        [SFFact]
        public void TestQueueMessage_TestPassed_FlushesDelayedMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 2);

            var delayedMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(delayedMessage.Object);

            // act
            var passMessage = CreateTestPassedMessage();
            bus.QueueMessage(passMessage);

            // assert - delayed message should be flushed to inner bus
            innerBus.Verify(b => b.QueueMessage(delayedMessage.Object), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestSkipped_FlushesDelayedMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 2);

            var delayedMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(delayedMessage.Object);

            // act
            var skipMessage = CreateTestSkippedMessage();
            bus.QueueMessage(skipMessage);

            // assert - delayed message should be flushed
            innerBus.Verify(b => b.QueueMessage(delayedMessage.Object), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestFailed_WithRetriesRemaining_StaysDelayed()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 2);

            var failedMessage = CreateTestFailedMessage("System.Exception", "something failed");
            var delayedMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(delayedMessage.Object);

            // act
            bus.QueueMessage(failedMessage);

            // assert - messages should still be delayed (retries remaining)
            innerBus.Verify(b => b.QueueMessage(delayedMessage.Object), Times.Never);
        }

        [SFFact]
        public void TestQueueMessage_TestFailed_RetriesExhausted_ForwardsMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 1);

            // act - two failures exhaust the single retry
            // (first failure decrements from 1->0, second checks 0<=0 and disables delay)
            bus.QueueMessage(CreateTestFailedMessage("System.Exception", "first failure"));
            bus.QueueMessage(CreateTestFailedMessage("System.Exception", "second failure"));

            // After retries exhausted, next messages should go directly
            var nextMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(nextMessage.Object);

            // assert - subsequent messages should be forwarded immediately
            innerBus.Verify(b => b.QueueMessage(nextMessage.Object), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestFailed_WithSkipException_ConvertsToSkipped()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 0);

            var failedMessage = CreateTestFailedMessage("Xunit.Sdk.SkipException", "test should be skipped");

            // act
            bus.QueueMessage(failedMessage);

            // assert - should convert to TestSkipped and forward
            innerBus.Verify(b => b.QueueMessage(It.Is<IMessageSinkMessage>(m => m is TestSkipped)), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestFailed_WithSkipException_IncrementsSkippedCount()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 0);

            var failedMessage = CreateTestFailedMessage("Xunit.Sdk.SkipException", "skipped reason");

            // act
            bus.QueueMessage(failedMessage);

            // assert
            Assert.Equal(1, bus.SkippedCount);
        }

        [SFFact]
        public void TestQueueMessage_MultipleSkips_SkippedCountAccumulates()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 0);

            // act
            bus.QueueMessage(CreateTestFailedMessage("Xunit.Sdk.SkipException", "skip 1"));
            bus.QueueMessage(CreateTestFailedMessage("Xunit.Sdk.SkipException", "skip 2"));

            // assert
            Assert.Equal(2, bus.SkippedCount);
        }

        [SFFact]
        public void TestDispose_DisposesInnerBus()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 0);

            // act
            bus.Dispose();

            // assert
            innerBus.Verify(b => b.Dispose(), Times.Once);
        }

        [SFFact]
        public void TestQueueMessage_TestFailed_WithSkipException_FlushesDelayedMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFMessageBus(innerBus.Object, retriesCount: 1);

            var delayedMessage = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(delayedMessage.Object);

            var failedMessage = CreateTestFailedMessage("Xunit.Sdk.SkipException", "skipped");

            // act
            bus.QueueMessage(failedMessage);

            // assert - delayed messages should be flushed before the skip message
            innerBus.Verify(b => b.QueueMessage(delayedMessage.Object), Times.Once);
        }

        private static IMessageSinkMessage CreateTestPassedMessage()
        {
            var test = new Mock<ITest>();
            test.Setup(t => t.DisplayName).Returns("TestMethod");
            var message = new Mock<ITestPassed>();
            message.As<IMessageSinkMessage>();
            message.Setup(m => m.Test).Returns(test.Object);
            message.Setup(m => m.ExecutionTime).Returns(0.1m);
            return message.Object;
        }

        private static IMessageSinkMessage CreateTestSkippedMessage()
        {
            var test = new Mock<ITest>();
            test.Setup(t => t.DisplayName).Returns("TestMethod");
            var message = new Mock<ITestSkipped>();
            message.As<IMessageSinkMessage>();
            message.Setup(m => m.Test).Returns(test.Object);
            message.Setup(m => m.ExecutionTime).Returns(0m);
            message.Setup(m => m.Reason).Returns("skipped");
            return message.Object;
        }

        private static IMessageSinkMessage CreateTestFailedMessage(string exceptionType, string exceptionMessage)
        {
            var test = new Mock<ITest> { DefaultValue = DefaultValue.Mock };
            test.Setup(t => t.DisplayName).Returns("TestMethod");
            var message = new Mock<ITestFailed>();
            message.As<IMessageSinkMessage>();
            message.Setup(m => m.Test).Returns(test.Object);
            message.Setup(m => m.ExecutionTime).Returns(0.5m);
            message.Setup(m => m.ExceptionTypes).Returns(new[] { exceptionType });
            message.Setup(m => m.Messages).Returns(new[] { exceptionMessage });
            message.Setup(m => m.StackTraces).Returns(new[] { "" });
            message.Setup(m => m.ExceptionParentIndices).Returns(new[] { -1 });
            return message.Object;
        }
    }
}
#endif
