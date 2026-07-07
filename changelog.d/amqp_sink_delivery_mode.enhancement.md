The `amqp` sink now supports setting the `delivery_mode` message property via `properties.delivery_mode`, with the values `transient` or `persistent`. Setting it to `persistent` makes messages routed to durable queues survive a broker restart. When not specified, the property is not set on published messages, preserving the previous behavior.

authors: htrendev
