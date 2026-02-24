1. tests/e2e_ui/test_pipeline_data.py::test_audit_page_shows_counts
  received count is 0, why?
2. I see way to many velocity algo alerts, this is probably not good
3. existing e2e tests mostly look at the admin ui instead of viewer UI, we need coverage for both and most importatnly the viewer one
4. transaction/order/execution id missing in events explorer
5. I see screenshots in UI tests when the screenshot is unrelated to the step because it doesn't interact with the ui
6. all UI tables should support sorting by clicking on the field we want to sort by. I also want to look into filtering by each field too, should be robust and support data ranges, constatns filtering ect
7. we need to add an additional_fields field to all event types (orders/executions/transactions) that contains a json with values that can be used in future algos.
8. data audit sums make no sense