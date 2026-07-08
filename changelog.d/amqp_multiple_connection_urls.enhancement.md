The `amqp` sink and source now accept a list of URIs in `connection_string`, in addition to the existing single URI. The sink connects to one server and, when a connection error is detected, fails over to the remaining servers in a round-robin fashion, sweeping all configured servers until a connection succeeds. The source tries each URI in order when it connects at startup. Set the `timeout` query parameter on each URI so that unreachable servers do not delay failover.

authors: htrendev
