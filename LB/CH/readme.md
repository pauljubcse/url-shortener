This load balancer uses consistent hashing for efficient backend selection with low remapping rates, rate limiting via a token bucket algorithm,
and LRU caching to optimize URL lookups. It dynamically receives updates backend servers through a WebSocket-based service registry, ensuring high availability. 
The system efficiently handles URL shortening requests while ensuring scalability and resilience.
