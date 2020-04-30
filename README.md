# Related article
https://www.alec.fyi/set-up-google-sheets-apis-and-treat-sheets-like-a-database.html

# Using this


This repo uses the serverless.com Infrastructure-as-code platform (which itself wraps AWS CloudFormation).

To create a CloudFormation Stack (and also subsequently update it), use:
``` 
sls deploy
```

It will be called `s3-sync-cron` (you can edit this in the serverless.yml)

You can test the Lambda locally (be aware it does send an actual email) with:

```
TODO
```

To take down the CloudFormation Stack and associated Lambdas and S3, use:
```
sls remove
```
