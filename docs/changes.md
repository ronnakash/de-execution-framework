1. grafana dashboards havo no data. need ui test to see they always work
2. services that interact with any infra, like kafka, postgres, clickhouse, redis, filesystems ect must have integration testing for each of the interactions it has! this is very important to ensure we are not breaking stuff
3. we need to ensure we have really good e2e test coverage. I want to focus on 3 domains - data algos and ui. We should have comprehensize e2es to see that data ingestion behaves as expected. We should already have pretty good coverage. For algos, we need to test common cases for each algo and also have really strong unit testing for all edge cases. We should also have good UI tests for the sorting and filtering ect. We should have tests for the UI that log in for the client we insert data/generate alerts for by creating a viewer user for it
4. regarding algo e2e test coverage - I see that we have the algos module we run as a service that is up and consuming kafka messages and it has test coverage. However I do not see any unit test coverage and no e2e test coverage for batch algo runs as batch jobs that are triggered manually or by the scheduler service. We need to also have coverage for that flow.
5. I want to ensure that our client configurations make sense. We should have to configure a client as batch or realtime. Batch clients have algos run once a day by the scheduler, on data in a time range defined by their client configuration. Batch client must have that configured. We don't send events for batch clients to the realtime algos service. For that, we need to store the configurations for clients in the normalizer and listen to configuration updates. We need to be able to enable and disable algos for a client via the configuration, and also allow us to override that when doing batch runs manually by explicitly providing the algos to run as command line args
6. do we have a way to scale the app up? for example, do we support multiple of the same service running at the same time? I think this is something we should be able to do. We are creating docker images for each service, so maybe we can orchestrate the system with terraform and kubernetes to be able to have many of the same pod to scale up the system to support a lot of data.
7. we need to have autoscaling for kafka as well. Having more pods than partitions they read from makes no sense
8. harness implementations should be split into separate files and the whole thing should be moved to its own folder
9. wtf is data audit doing? it should not listen to all of the pipelines topics to get message counts. I will plan a redesign later


Data Audit Redesign:
1. data audit should listen to 2 topics - 1 for incoming counts and one for file uploads. rest and kafka starter services publish to the incoming counts and file processor publishes to the file uploads one.
2. incoming counts are for rest and kafka. we should accumulate data in each sevice and publish periodically partitioned by message type and tenant
3. file upload data should contain the incoming file event count and file name, tenant ect
4. we get the processed errors and duplicate count by querying ClickHouse.
5. we need a module to run as a job that calculates the 
6. the scheduler should schedule the calculation job for every 15 minutes.
7. we should have a way to not calculate the same data multiple times by using something like a timestamp high watermark
8. calculation results are stored in postgres
9. when service is queried it just goes to the postgres
10. the service should have a calculate method to trigger a calculation as a batch job (it shouldnt do the calculation itself just trigger a job to do so)
11. events should contain an ingestion method field (file, rest, kafka) to use for the batching
12. separate the calculations for files and "realtime" methods (kafka and rest)
13. we have separate tenants for each e2e test. the main reason is to simulate test isolation. I think this is a bad approach with a lot of downsides. I think I have a solution that also makes a lot of business sense - all of our events have (or should have) a client_id which represents the actual user of the tenant that is doint the trade/transaction. Algos should take that field into account and we should use that field in order to create test isolation by using it to separate alert generations and filter alets/cases/events by their client id as well as the tenant
14. kafka starter service should have topics to read/write back to the client for each client in the system. We should have that configured in the client config service.



make a plan based on my comments. each phase of the plan should have its own plan file we will implement based on