# SqsPoller

[![Join the chat at https://gitter.im/SqsPoller/SqsPoller](https://badges.gitter.im/SqsPoller/SqsPoller.svg)](https://gitter.im/SqsPoller/SqsPoller?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A small library that helps ASP.NET Core applications easily consume messages from a SQS queue

# Example

Register SqsPoller with `IServiceCollection`
```
  services.AddSqsPoller(...);
```

Create a message object and consumer that consumes the message

```
  public class UserCreated
  {
      public long Id { get; set; }
      public string FirstName {get; set; }
      public string LastName {get; set; }
      public string Email {get; set; }
  }
  
  public class SendConfirmationMessageConsumer: IConsumer<UserCreated>
  {
      public Task Consume(UserCreated message, CancellationToken cancellationToken)
      {
          // Some code
      }
  }
```

# Constraint

Each message should have a `MessageType` header and value should the name of the type that you are going to consume (i.e. `UserCreated`). 

# Per-message processing timeout

By default a consumer runs for as long as it takes. A consumer that hangs (for example on a dead
connection) keeps its concurrency slot forever, and when all `MaxNumberOfParallelism` slots are stuck
the poller stops processing without any error.

Set `MessageProcessingTimeout` to bound each message:

```
  services.AddSqsPoller(new SqsPollerConfig
  {
      QueueUrl = "...",
      // Keep it below the queue's visibility timeout.
      MessageProcessingTimeout = TimeSpan.FromMinutes(2),
      OnHandleMessageTimeout = (messageId, abandoned) => { /* e.g. increment a metric */ }
  }, new[] { typeof(SendConfirmationMessageConsumer) });
```

When the timeout fires:

* the consumer's `CancellationToken` is cancelled;
* the message is not deleted, so SQS redelivers it after its visibility timeout;
* an error is logged with the message id, and `OnHandleMessageTimeout` is called if set;
* if the consumer ignores the token, it stops being awaited after `MessageProcessingAbandonGracePeriod`
  (default 5 seconds) and its slot is released. It keeps running in the background, so consumers must
  be idempotent.

Shutdown of the host is not reported as a timeout.
