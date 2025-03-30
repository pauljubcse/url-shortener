This service registry program manages WebSocket-based service registrations, 
monitors backend servers and database shards, 
and updates the load balancer on status changes. 
Backend services register via /register, while database shards use /dbregister, 
both maintaining WebSocket connections for real-time monitoring.
A background process periodically filters inactive shards and sorts them based on row count for load balancing. 
When a service or shard disconnects, the registry notifies the load balancer via /getServices. 
The system efficiently tracks availability, ensuring dynamic service discovery and load distribution.








