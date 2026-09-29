# Teardown — delete all AWS resources

Run in dependency order. Region ap-south-1. Replace <ACCOUNT_ID> and the
`-484990` bucket suffix with your own if reusing.

```powershell
# ── 1. EventBridge: remove target, delete rule, delete role ──
aws events remove-targets --rule yt-data-pipeline-schedule-dev --ids 1 --region ap-south-1
aws events delete-rule --name yt-data-pipeline-schedule-dev --region ap-south-1
aws iam delete-role-policy --role-name yt-data-pipeline-eventbridge-role-dev --policy-name yt-eventbridge-inline
aws iam delete-role --role-name yt-data-pipeline-eventbridge-role-dev

# ── 2. Step Functions ──
aws stepfunctions delete-state-machine --state-machine-arn arn:aws:states:ap-south-1:<ACCOUNT_ID>:stateMachine:yt-data-pipeline-dev --region ap-south-1

# ── 3. Glue jobs ──
aws glue delete-job --job-name yt-data-pipeline-bronze-to-silver-dev --region ap-south-1
aws glue delete-job --job-name yt-data-pipeline-silver-to-gold-dev --region ap-south-1

# ── 4. Lambda functions ──
aws lambda delete-function --function-name yt-data-pipeline-youtube-ingestion-dev --region ap-south-1
aws lambda delete-function --function-name yt-data-pipeline-json-to-parquet-dev --region ap-south-1
aws lambda delete-function --function-name yt-data-pipeline-live-to-silver-dev --region ap-south-1
aws lambda delete-function --function-name yt-data-pipeline-data-quality-dev --region ap-south-1

# ── 5. Glue databases (deletes their tables too) ──
aws glue delete-database --name yt_pipeline_bronze_dev --region ap-south-1
aws glue delete-database --name yt_pipeline_silver_dev --region ap-south-1
aws glue delete-database --name yt_pipeline_gold_dev --region ap-south-1

# ── 6. S3: empty then delete each bucket ──
aws s3 rm s3://yt-data-pipeline-bronze-ap-south-1-dev-484990 --recursive --region ap-south-1
aws s3 rb s3://yt-data-pipeline-bronze-ap-south-1-dev-484990 --region ap-south-1
aws s3 rm s3://yt-data-pipeline-silver-ap-south-1-dev-484990 --recursive --region ap-south-1
aws s3 rb s3://yt-data-pipeline-silver-ap-south-1-dev-484990 --region ap-south-1
aws s3 rm s3://yt-data-pipeline-gold-ap-south-1-dev-484990 --recursive --region ap-south-1
aws s3 rb s3://yt-data-pipeline-gold-ap-south-1-dev-484990 --region ap-south-1
aws s3 rm s3://yt-data-pipeline-script-ap-south-1-dev-484990 --recursive --region ap-south-1
aws s3 rb s3://yt-data-pipeline-script-ap-south-1-dev-484990 --region ap-south-1

# ── 7. IAM roles (delete inline policy, then role) ──
aws iam delete-role-policy --role-name yt-data-pipeline-lambda-role-dev --policy-name yt-lambda-inline
aws iam delete-role --role-name yt-data-pipeline-lambda-role-dev
aws iam delete-role-policy --role-name yt-data-pipeline-glue-role-dev --policy-name yt-glue-inline
aws iam delete-role --role-name yt-data-pipeline-glue-role-dev
aws iam delete-role-policy --role-name yt-data-pipeline-sfn-role-dev --policy-name yt-sfn-inline
aws iam delete-role --role-name yt-data-pipeline-sfn-role-dev

# ── 8. SNS topic (also removes its subscriptions) ──
aws sns delete-topic --topic-arn arn:aws:sns:ap-south-1:<ACCOUNT_ID>:yt-data-pipeline-alerts-dev --region ap-south-1

# ── 9. (Optional) delete the $5 budget once you're sure nothing bills ──
# aws budgets delete-budget --account-id <ACCOUNT_ID> --budget-name yt-data-pipeline-monthly-5usd
```
```
