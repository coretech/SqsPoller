#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using Xunit;

namespace SqsPoller.Tests.Unit;

public class SqsPollerHostedServiceTimeoutTests
{
    private const string TimeoutLogText = "was not processed within MessageProcessingTimeout";
    private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan Grace = TimeSpan.FromMilliseconds(100);
    // Upper bound for any single wait in these tests; far above Timeout + Grace.
    private static readonly TimeSpan TestGuard = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task HandleMessage_ConsumerIgnoresTokenAndNeverCompletes_AbandonsItReleasesSlotAndDoesNotDelete()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        string? timedOutMessageId = null;
        bool? abandoned = null;
        var config = TimeoutConfig();
        config.OnHandleMessageTimeout = (id, a) => { timedOutMessageId = id; abandoned = a; };
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>())
            .Returns(new TaskCompletionSource<bool>().Task);
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);
        var semaphore = new SemaphoreSlim(1);
        await semaphore.WaitAsync();

        //Act
        await hostedService.HandleMessage(TestMessage(), CancellationToken.None, semaphore, "")
            .WaitAsync(TestGuard);

        //Assert
        semaphore.CurrentCount.ShouldBe(1);
        await sqsClient.DidNotReceive().DeleteMessageAsync(Arg.Any<DeleteMessageRequest>(), Arg.Any<CancellationToken>());
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(1);
        timedOutMessageId.ShouldBe("test-message-id");
        abandoned.ShouldBe(true);
    }

    [Fact]
    public async Task HandleMessage_ConsumerHonoursToken_TimesOutWithoutAbandoningAndDoesNotDelete()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        bool? abandoned = null;
        var config = TimeoutConfig();
        config.MessageProcessingAbandonGracePeriod = TestGuard;
        config.OnHandleMessageTimeout = (_, a) => abandoned = a;
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.Delay(System.Threading.Timeout.Infinite, ci.Arg<CancellationToken>()));
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);
        var semaphore = new SemaphoreSlim(0);

        //Act
        await hostedService.HandleMessage(TestMessage(), CancellationToken.None, semaphore, "")
            .WaitAsync(TestGuard);

        //Assert
        semaphore.CurrentCount.ShouldBe(1);
        await sqsClient.DidNotReceive().DeleteMessageAsync(Arg.Any<DeleteMessageRequest>(), Arg.Any<CancellationToken>());
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(1);
        CountLogs(logger, LogLevel.Error, "Failed to handle message").ShouldBe(0);
        abandoned.ShouldBe(false);
    }

    [Fact]
    public async Task HandleMessage_ConsumerCompletesInTime_DeletesMessage()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        var timeoutReported = false;
        var config = TimeoutConfig();
        config.OnHandleMessageTimeout = (_, _) => timeoutReported = true;
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);

        //Act
        await hostedService.HandleMessage(TestMessage(), CancellationToken.None, new SemaphoreSlim(0), "")
            .WaitAsync(TestGuard);

        //Assert
        await sqsClient.Received(1).DeleteMessageAsync(
            Arg.Is<DeleteMessageRequest>(r => r.ReceiptHandle == "test-receipt-handle"), Arg.Any<CancellationToken>());
        timeoutReported.ShouldBeFalse();
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(0);
    }

    [Fact]
    public async Task HandleMessage_ShutdownCancelsConsumer_NotReportedAsTimeoutAndNotDeleted()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        var timeoutReported = false;
        var config = TimeoutConfig();
        config.MessageProcessingTimeout = TestGuard;
        config.OnHandleMessageTimeout = (_, _) => timeoutReported = true;
        var consumerStarted = new TaskCompletionSource<bool>();
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                consumerStarted.TrySetResult(true);
                return Task.Delay(System.Threading.Timeout.Infinite, ci.Arg<CancellationToken>());
            });
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);
        using var stopping = new CancellationTokenSource();
        var semaphore = new SemaphoreSlim(0);

        //Act
        var handling = hostedService.HandleMessage(TestMessage(), stopping.Token, semaphore, "");
        await consumerStarted.Task.WaitAsync(TestGuard);
        stopping.Cancel();
        await handling.WaitAsync(TestGuard);

        //Assert
        semaphore.CurrentCount.ShouldBe(1);
        timeoutReported.ShouldBeFalse();
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(0);
        // Unchanged pre-timeout behaviour on shutdown: the cancellation goes to the regular handler.
        CountLogs(logger, LogLevel.Error, "Failed to handle message").ShouldBe(1);
        await sqsClient.DidNotReceive().DeleteMessageAsync(Arg.Any<DeleteMessageRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleMessage_AbandonedConsumerFailsLater_ExceptionIsObserved()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        var config = TimeoutConfig();
        var hung = new TaskCompletionSource<bool>();
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>()).Returns(hung.Task);
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);
        await hostedService.HandleMessage(TestMessage(), CancellationToken.None, new SemaphoreSlim(0), "")
            .WaitAsync(TestGuard);

        //Act
        hung.SetException(new InvalidOperationException("late failure"));

        //Assert
        await WaitUntil(() => CountLogs(logger, LogLevel.Debug, "Abandoned consumer of message") == 1);
    }

    [Fact]
    public async Task ExecuteAsync_SingleSlotAndHungConsumer_NextMessageIsStillProcessed()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        var receives = new Queue<Message>(new[]
        {
            new Message { MessageId = "hung", ReceiptHandle = "hung-rh" },
            new Message { MessageId = "next", ReceiptHandle = "next-rh" }
        });
        sqsClient.ReceiveMessageAsync(Arg.Any<ReceiveMessageRequest>(), Arg.Any<CancellationToken>())
            .Returns(async ci =>
            {
                lock (receives)
                {
                    if (receives.Count > 0)
                        return new ReceiveMessageResponse { Messages = new List<Message> { receives.Dequeue() } };
                }
                await Task.Delay(20, ci.Arg<CancellationToken>());
                return new ReceiveMessageResponse { Messages = new List<Message>() };
            });
        var config = TimeoutConfig();
        config.QueueUrl = "http://localhost/queue";
        config.MaxNumberOfParallelism = 1;
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>())
            .Returns(ci => ci.Arg<Message>().MessageId == "hung"
                ? new TaskCompletionSource<bool>().Task
                : Task.CompletedTask);
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);

        //Act
        await hostedService.StartAsync(CancellationToken.None);
        try
        {
            await WaitUntil(() => DeletedReceiptHandles(sqsClient).Contains("next-rh"));
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }

        //Assert
        DeletedReceiptHandles(sqsClient).ShouldNotContain("hung-rh");
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(1);
    }

    [Fact]
    public async Task HandleMessage_TimeoutNotSet_ConsumerGetsCallerTokenAndIsAwaitedWithoutLimit()
    {
        //Arrange
        var sqsClient = Substitute.For<IAmazonSQS>();
        var config = new SqsPollerConfig();
        var hung = new TaskCompletionSource<bool>();
        var consumerResolver = Substitute.For<IConsumerResolver>();
        consumerResolver.Resolve(Arg.Any<Message>(), Arg.Any<CancellationToken>()).Returns(hung.Task);
        var logger = Substitute.For<ILogger<SqsPollerHostedService>>();
        var hostedService = new SqsPollerHostedService(sqsClient, config, consumerResolver, logger);
        using var stopping = new CancellationTokenSource();
        var semaphore = new SemaphoreSlim(0);

        //Act
        var handling = hostedService.HandleMessage(TestMessage(), stopping.Token, semaphore, "");
        var finishedEarly = await Task.WhenAny(handling, Task.Delay(Timeout + Grace + Grace)) == handling;
        hung.SetResult(true);
        await handling.WaitAsync(TestGuard);

        //Assert
        config.MessageProcessingTimeout.ShouldBeNull();
        finishedEarly.ShouldBeFalse();
        await consumerResolver.Received(1).Resolve(Arg.Any<Message>(), stopping.Token);
        await sqsClient.Received(1).DeleteMessageAsync(Arg.Any<DeleteMessageRequest>(), Arg.Any<CancellationToken>());
        semaphore.CurrentCount.ShouldBe(1);
        CountLogs(logger, LogLevel.Error, TimeoutLogText).ShouldBe(0);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_NonPositiveTimeout_Throws(int milliseconds)
    {
        var config = new SqsPollerConfig { MessageProcessingTimeout = TimeSpan.FromMilliseconds(milliseconds) };

        Should.Throw<ArgumentOutOfRangeException>(() => new SqsPollerHostedService(
            Substitute.For<IAmazonSQS>(), config, Substitute.For<IConsumerResolver>(),
            Substitute.For<ILogger<SqsPollerHostedService>>()));
    }

    private static SqsPollerConfig TimeoutConfig() => new SqsPollerConfig
    {
        MessageProcessingTimeout = Timeout,
        MessageProcessingAbandonGracePeriod = Grace
    };

    private static Message TestMessage() => new Message
    {
        MessageId = "test-message-id",
        ReceiptHandle = "test-receipt-handle"
    };

    private static int CountLogs(ILogger logger, LogLevel level, string text) =>
        logger.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ILogger.Log))
            .Select(c => c.GetArguments())
            .Count(args => (LogLevel)args[0]! == level && args[2]?.ToString()?.Contains(text) == true);

    private static List<string> DeletedReceiptHandles(IAmazonSQS sqsClient) =>
        sqsClient.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IAmazonSQS.DeleteMessageAsync))
            .Select(c => c.GetArguments()[0])
            .OfType<DeleteMessageRequest>()
            .Select(r => r.ReceiptHandle)
            .ToList();

    private static async Task WaitUntil(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TestGuard;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time");
            await Task.Delay(10);
        }
    }
}
