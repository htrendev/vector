The `amqp` sink now implements proper retry logic to prevent data loss during connection failures. When the AMQP broker becomes unavailable, the sink will automatically retry failed requests with exponential backoff (using the Fibonacci sequence), defaulting to a maximum backoff of 10 seconds. This behavior is configurable via the new `request` configuration section.

authors: htrendev

