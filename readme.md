triggers glue jobs based on events in s3

This lambda will receive events from s3 and do a lookup on a dynamodb table based on the incoming source of the event to determine which glue job to run.
