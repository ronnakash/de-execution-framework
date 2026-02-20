1. we need to add a UI that data-api will use
2. we need to add auth to the system so the frontend can communicate with data-api securely. Auth should suport multiple users for a single tenant and all users are assigned to a tenant. Users of each tenant can only access the tenants data
3. we should have a client configuration page where we can change stuff about the client
4. we should have adjustable thresholds for algorithms on a per-client basis that the algos will use when running the algo on a clients data
5. we can set clients as realtime or batch clients. We will only send the clients data to the algos service if it is a realtime client
6