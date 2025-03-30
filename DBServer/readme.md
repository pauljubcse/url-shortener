This database server handles URL shortening queries and write operations using PostgreSQL, RabbitMQ, and WebSockets. 
It exposes HTTP endpoints to fetch long URLs by short URL while dynamically registering with a WebSocket-based service registry to report the number of stored URLs, to distirbute load equally among shards.
It consumes messages from RabbitMQ queues for distributed writes and replication. Each shard runs independently, storing a partition of the data.
