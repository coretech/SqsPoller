using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SqsPoller
{
    public class SqsPollerHostedService: BackgroundService
    {
        private readonly IAmazonSQS _amazonSqsClient;
        private readonly SqsPollerConfig _config;
        private readonly IConsumerResolver _consumerResolver;
        private readonly ILogger<SqsPollerHostedService> _logger;

        public SqsPollerHostedService(
            IAmazonSQS amazonSqsClient,
            SqsPollerConfig config,
            IConsumerResolver consumerResolver,
            ILogger<SqsPollerHostedService> logger)
        {
            _amazonSqsClient = amazonSqsClient;
            _consumerResolver = consumerResolver;
            _logger = logger;
            _config = config;

            if (config.MessageProcessingTimeout is { } timeout && timeout <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(config), timeout,
                    "SqsPollerConfig.MessageProcessingTimeout must be positive, or null to disable it.");
            if (config.MessageProcessingAbandonGracePeriod < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(config), config.MessageProcessingAbandonGracePeriod,
                    "SqsPollerConfig.MessageProcessingAbandonGracePeriod must not be negative.");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var queueUrl = !string.IsNullOrEmpty(_config.QueueUrl)
                ? _config.QueueUrl
                : (await _amazonSqsClient.GetQueueUrlAsync(_config.QueueName, stoppingToken)).QueueUrl;
            using var semaphore = new SemaphoreSlim(_config.MaxNumberOfParallelism);
            while (!stoppingToken.IsCancellationRequested)
            {
                await Handle(queueUrl, semaphore, stoppingToken);
            }
        }

        private async Task Handle(string queueUrl, SemaphoreSlim semaphore, CancellationToken cancellationToken)
        {
            using var correlationIdScope = _logger.BeginScope(
                new Dictionary<string, object>
                {
                    ["correlation_id"] = Guid.NewGuid(),
                    ["queue_url"] = queueUrl
                });

            _logger.LogTrace("Start polling messages from a queue: {queue_url}. correlation_id: {correlation_id}");
            var messages = await GetMessages(queueUrl, cancellationToken);
            foreach (var message in messages)
            {
                using var messageIdScope = _logger.BeginScope(
                    new Dictionary<string, object>
                    {
                        ["message_id"] = message.MessageId,
                        ["receipt_handle"] = message.ReceiptHandle
                    });
                _logger.LogTrace(
                    "Start processing the message with id {message_id} and ReceiptHandle {receipt_handle}");

                await semaphore.WaitAsync(cancellationToken);
                HandleMessage(message, cancellationToken, semaphore, queueUrl);
                if (cancellationToken.IsCancellationRequested)
                    return;
            }
        }

        public async Task HandleMessage(Message message, CancellationToken cancellationToken, SemaphoreSlim semaphore, string queueUrl)
        {
            try
            {
                _logger.LogTrace("Handling message {message_id} {receipt_handle}");
                if (_config.MessageProcessingTimeout is { } timeout)
                {
                    if (!await ResolveWithTimeout(message, timeout, cancellationToken))
                        return; // Not deleted: SQS redelivers it after the visibility timeout.
                }
                else
                {
                    await _consumerResolver.Resolve(message, cancellationToken);
                }
                _logger.LogTrace("Message {message_id} {receipt_handle} has been handled");

                _logger.LogTrace("Deleting the message with id {message_id} and ReceiptHandle {receipt_handle}");
                DeleteMessage(queueUrl, message, cancellationToken);
                _logger.LogTrace("Message {message_id} {receipt_handle} has been deleted");
            }
            catch (Exception e)
            {
                if (_config.OnHandleMessageException is null)
                {
                    _logger.Log(_config.ExceptionDefaultMessageLogLevel, e, "Failed to handle message {message_id} {receipt_handle}");
                }
                else
                {
                    _config.OnHandleMessageException(e, message.MessageId);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        /// <summary>
        /// Runs the consumer under a token that fires after <paramref name="timeout"/> or on shutdown.
        /// Returns true when the consumer completed and the message may be deleted, false when it
        /// timed out or was abandoned. Other consumer exceptions, including cancellation on shutdown,
        /// propagate to the caller exactly as without a timeout.
        /// </summary>
        private async Task<bool> ResolveWithTimeout(Message message, TimeSpan timeout, CancellationToken stoppingToken)
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            timeoutCts.CancelAfter(timeout);

            // Task.Run: a consumer that blocks synchronously before its first await would otherwise
            // block this method, and the deadline below could never be observed.
            var consumerTask = Task.Run(() => _consumerResolver.Resolve(message, timeoutCts.Token));

            using var deadlineCts = new CancellationTokenSource();
            var deadline = Task.Delay(timeout + _config.MessageProcessingAbandonGracePeriod, deadlineCts.Token);
            if (await Task.WhenAny(consumerTask, deadline) == consumerTask)
            {
                deadlineCts.Cancel();
                try
                {
                    await consumerTask;
                    return true;
                }
                // Shutdown wins: once the stopping token has fired, the exception propagates to
                // HandleMessage's exception handler and is not reported as a timeout.
                catch (Exception e) when (timeoutCts.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
                {
                    ReportTimeout(message, timeout, abandoned: false, e);
                    return false;
                }
            }

            // The consumer ignored its token for the whole grace period. Stop awaiting it so the
            // semaphore slot is released. It keeps running; observe its eventual exception so it
            // does not surface as an unobserved task exception.
            timeoutCts.Cancel();
            _ = consumerTask.ContinueWith(
                t => _logger.LogDebug(t.Exception, "Abandoned consumer of message {message_id} failed", message.MessageId),
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            if (stoppingToken.IsCancellationRequested)
            {
                _logger.LogDebug("Consumer of message {message_id} ignored shutdown and was abandoned", message.MessageId);
                return false;
            }

            ReportTimeout(message, timeout, abandoned: true, null);
            return false;
        }

        private void ReportTimeout(Message message, TimeSpan timeout, bool abandoned, Exception? exception)
        {
            _logger.LogError(
                exception,
                "Message {message_id} was not processed within MessageProcessingTimeout {timeout} and is left in the queue for redelivery. Consumer abandoned: {abandoned}",
                message.MessageId, timeout, abandoned);
            _config.OnHandleMessageTimeout?.Invoke(message.MessageId, abandoned);
        }

        private async Task DeleteMessage(string queueUrl, Message message, CancellationToken cancellationToken)
        {
            try
            {
                var deleteRequest = new DeleteMessageRequest
                {
                    QueueUrl = queueUrl,
                    ReceiptHandle = message.ReceiptHandle
                };

                await _amazonSqsClient.DeleteMessageAsync(deleteRequest, cancellationToken);
            }
            catch (Exception e)
            {
                if (_config.OnDeleteMessageException is null)
                {
                    _logger.Log(
                        _config.ExceptionDefaultMessageLogLevel,
                        e,
                        "Failed to delete message {message_id} {receipt_handle}");
                }
                else
                {
                    _config.OnDeleteMessageException(e, message.MessageId);
                }
            }
        }

        private async Task<IEnumerable<Message>> GetMessages(
            string queueUrl, CancellationToken cancellationToken)
        {
            try
            {
                var result = await _amazonSqsClient
                    .ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        WaitTimeSeconds = _config.WaitTimeSeconds,
                        MaxNumberOfMessages = _config.MaxNumberOfMessages,
                        MessageAttributeNames = _config.MessageAttributeNames,
                        QueueUrl = queueUrl
                    }, cancellationToken);

                var messagesCount = result.Messages.Count;
                _logger.LogTrace("{count} messages received", messagesCount);
                return result.Messages;
            }
            catch (Exception e)
            {
                if (_config.OnGetMessagesException is null)
                {
                    _logger.Log(_config.ExceptionDefaultMessageLogLevel, e, "Failed to receive messages from the queue");
                }
                else
                {
                    _config.OnGetMessagesException(e);
                }

                return Enumerable.Empty<Message>();
            }
        }
    }
}