This backend server implements a distributed URL shortening service with sharded storage and replication. 
It generates unique short URLs using a Snowflake-inspired ID generator, encodes them in Base62, and routes them to appropriate database shards based on shard IDs. 
The server registers itself with a service registry via WebSocket and listens for updates on the least filled shard. 
It publishes write requests to RabbitMQ for storage and replication, ensuring fault tolerance. 
The `/post` endpoint creates short URLs, while the `/query` endpoint retrieves long URLs by querying the appropriate shard or its replica. 
The system supports high availability and load balancing through active shard tracking and failover mechanisms.
