#if NET8_0_OR_GREATER
using System;
using System.Threading;
using Moq;
using Snowflake.Data.Tests.Util;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class SFTestCaseRunnerTest
    {
        [SFFact]
        public void TestSFDelayedMessageBus_QueueMessage_ReturnsTrue()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            using var bus = new SFDelayedMessageBus(innerBus.Object);
            var message = new Mock<IMessageSinkMessage>();

            // act
            var result = bus.QueueMessage(message.Object);

            // assert
            Assert.True(result);
        }

        [SFFact]
        public void TestSFDelayedMessageBus_QueueMessage_DoesNotForwardImmediately()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            using var bus = new SFDelayedMessageBus(innerBus.Object);
            var message = new Mock<IMessageSinkMessage>();

            // act
            bus.QueueMessage(message.Object);

            // assert - message should not be forwarded yet
            innerBus.Verify(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>()), Times.Never);
        }

        [SFFact]
        public void TestSFDelayedMessageBus_Dispose_ForwardsAllMessages()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFDelayedMessageBus(innerBus.Object);

            var message1 = new Mock<IMessageSinkMessage>();
            var message2 = new Mock<IMessageSinkMessage>();
            bus.QueueMessage(message1.Object);
            bus.QueueMessage(message2.Object);

            // act
            bus.Dispose();

            // assert
            innerBus.Verify(b => b.QueueMessage(message1.Object), Times.Once);
            innerBus.Verify(b => b.QueueMessage(message2.Object), Times.Once);
        }

        [SFFact]
        public void TestSFDelayedMessageBus_Dispose_PreservesMessageOrder()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>(MockBehavior.Strict);
            var sequence = new MockSequence();
            var message1 = new Mock<IMessageSinkMessage>();
            var message2 = new Mock<IMessageSinkMessage>();
            var message3 = new Mock<IMessageSinkMessage>();

            innerBus.InSequence(sequence).Setup(b => b.QueueMessage(message1.Object)).Returns(true);
            innerBus.InSequence(sequence).Setup(b => b.QueueMessage(message2.Object)).Returns(true);
            innerBus.InSequence(sequence).Setup(b => b.QueueMessage(message3.Object)).Returns(true);

            var bus = new SFDelayedMessageBus(innerBus.Object);
            bus.QueueMessage(message1.Object);
            bus.QueueMessage(message2.Object);
            bus.QueueMessage(message3.Object);

            // act
            bus.Dispose();

            // assert - verified by strict mock with sequence
            innerBus.VerifyAll();
        }

        [SFFact]
        public void TestSFDelayedMessageBus_Dispose_WithNoMessages_DoesNothing()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            var bus = new SFDelayedMessageBus(innerBus.Object);

            // act
            bus.Dispose();

            // assert
            innerBus.Verify(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>()), Times.Never);
        }

        [SFFact]
        public void TestSFDelayedMessageBus_MultipleMessages_AllQueuedBeforeDispose()
        {
            // arrange
            var innerBus = new Mock<IMessageBus>();
            innerBus.Setup(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>())).Returns(true);
            var bus = new SFDelayedMessageBus(innerBus.Object);

            // act
            for (var i = 0; i < 10; i++)
                bus.QueueMessage(new Mock<IMessageSinkMessage>().Object);

            // assert - nothing forwarded yet
            innerBus.Verify(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>()), Times.Never);

            // act
            bus.Dispose();

            // assert - all 10 forwarded
            innerBus.Verify(b => b.QueueMessage(It.IsAny<IMessageSinkMessage>()), Times.Exactly(10));
        }

        [SFFact]
        public void TestSFCaseRunnerContext_MaxRetries_StoresValue()
        {
            // arrange & act
            var ctxt = CreateContext(maxRetries: 5);

            // assert
            Assert.Equal(5, ctxt.MaxRetries);
        }

        [SFFact]
        public void TestSFCaseRunnerContext_MaxRetries_ZeroIsValid()
        {
            // arrange & act
            var ctxt = CreateContext(maxRetries: 0);

            // assert
            Assert.Equal(0, ctxt.MaxRetries);
        }

        [SFFact]
        public void TestSFTestCaseRunner_Instance_IsSingleton()
        {
            // act
            var instance1 = SFTestCaseRunner.Instance;
            var instance2 = SFTestCaseRunner.Instance;

            // assert
            Assert.Same(instance1, instance2);
        }

        private static SFCaseRunnerContext CreateContext(int maxRetries)
        {
            var testCase = new Mock<IXunitTestCase>();
            var tests = Array.Empty<IXunitTest>();
            var messageBus = new Mock<IMessageBus>();
            var aggregator = new ExceptionAggregator();
            var cts = new CancellationTokenSource();

            return new SFCaseRunnerContext(
                maxRetries,
                testCase.Object,
                tests,
                messageBus.Object,
                aggregator,
                cts,
                "TestDisplay",
                null,
                ExplicitOption.Off,
                Array.Empty<object>());
        }
    }
}
#endif
