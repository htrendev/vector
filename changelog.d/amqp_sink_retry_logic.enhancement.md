The `amqp` sink now retries failed requests instead of dropping them, preventing data loss when the connection to the AMQP broker is interrupted. All errors are retried indefinitely with exponential backoff (Fibonacci, capped at 30 seconds by default), so during a broker outage the sink applies backpressure rather than losing events. Retry behavior, concurrency, and timeouts can be tuned via the new `request` configuration section (for example, `request.retry_attempts` bounds the number of attempts).

authors: htrendev
