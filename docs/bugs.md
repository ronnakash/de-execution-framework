
Looking at what you did during your last run, you spent a lot of effort into debugging the E2E test. I think we can make some changes to
  make that process simpler and faster. We can add some checks and logs to the test runs and maybe produce some report of the run that we can
  inspect. We can do simple things like checking databases and kafka or somehow signal that messages were produced/consumed (I think it
  makes sense to do so when we add grafana metrics and create some other version of grafana that is a wrapper and exports these metrics to
  us). I think we should change our plan and add that change (after phase 1 because we implemented it and preferably before the current phase
  2 because it has the potential of massively improve debugging the E2E tests). Modify the plan to achieve everything we wanted originally
  and also implement the better test debuggig/result exporting framework. ask me any follow ups you need and let me know what you think we
  should do before updating the plan