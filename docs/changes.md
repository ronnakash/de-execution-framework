1. we need to add a UI that will use data-api to show the clients events, alerts, errors, duplicate counts, ect
2. we need to add auth to the system so the frontend can communicate with data-api securely. Auth should suport multiple users for a single tenant and all users are assigned to a tenant. Users of each tenant can only access the tenants data
3. we should have a client configuration page where we can change stuff about the client
4. we should have adjustable thresholds for algorithms on a per-client basis that the algos will use when running the algo on a clients data
5. we can set clients as realtime or batch clients. We will only send the clients data to the algos service if it is a realtime client
6. we need to have a way to cache client data in services (like realtime/batch in normalizer to know if to send to algo, thresholds in algos service) and have them update upon change immediately. I think the right approach is to fetch on startup and periodically and also set up redis pub-sub to read updates
7. for the last 2 changes we should add a client configuration service to manage all of that and will have it's own postgres database to store the configrations
8. we need to have a rewrite of the algos. Algos should run on ranges of a configurable size batch of data (usually time range, of about 5-10 minutes) ordered by timestamp. it should be run on a sliding window of data
9. use the new algos logic in 2 ways - the existing algos service and a new algos module that will take a client and time range and run the algos on that (it will take a day usually so each run will also have a sliding window)
10. add e2e tests to all of that stuff
11. our current e2e tests are mixing what I think sould be separate - algo and data logic. We should reorganize our tests such that algos related tests and data related tests are separated.
12. we should also have e2e tests hit the UI and search for inserted events, alerts, ect
13. I think there should be a service that is meant to keep track of incoming data, processed, errors, and duplicate counts and have that data accesible to the client. We should have per-file data if the client uploaded files, and per-day data for each of rest and kafka. They should also be different entries. The service will calculate the counts and store it in it's own dedicated postgresql database and the UI will query it in a dedicated page called data audit
14. I want to have a comprehensive logging framework where we store all of the logs in a database (or something else) (clickhouse???) from all of the apps and be able to look at them and query them. We should add a lot of context to the events like what event is processed and for what tenant ect. I also want to have an internal UI where I can view the logs and filter them
15. I want to add grafana dashboards for each service/module that give us essential data on processing time, event counts, which tenants data are we processing ect
16. we need some task module scheduler/manager that stores all previous module runs (keep in mind that this applies to non-service modules and just for task runs, file-processor should count too). The idea is this - we can track previous task runs and see if they passed/failed, and be able to have scheduled runs. For example, schedule the fetching of currency rates every hour to currency service, or run algos for batch clients on a set time period (hour-wize, dates increment every time) at a configurable hour for the client. We should also be able to have some custom code per-task to fetch args for the runs
17. client configuration should have algo run time
18. client configuration should have available algos for the client
19. e2e tests are running sequentially. Can we modify them to run concurrently/paralelly? I think this will require modifying the tests themselves to either set up a new client each time or maybe have separate execution venues for events and separate algos also by ex_venue
20. when e2es run and the first fails we don't wait for the rest
21. we have separate module and async module definitions in modules/base.py they should be consolidated into one 
22. if I have an error in one of my services (for example, algos tries to insert date as string instead of datetime to postgres) the service crashes. This is not good!!! it should log the error and keep running 
23. we are deleting kafka topics between tests, why would we do that? If there is a good reason we need a better fix
24. services that interact with infra should have comprehensive integration tests!! for example, if we insert data into Postgres or ClickHouse (or redis, minio ect) or fetch data from there, we should have tests that try to do that with real data


write a detailed plan to implement the changes. We should have it executeable step-by-step and have some order that makes sense to do so