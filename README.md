# Related article
https://www.alec.fyi/set-up-google-sheets-apis-and-treat-sheets-like-a-database.html

# Setup

Go through the above article and set up Google Sheets API auth. You'll get a private key and a client email.

Set them as environment variables
```
export GSHEETS_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\nMII"
export GSHEETS_CLIENT_EMAIL=foo@bar.co
```


# Using this


This repo contains a serverless.yml infrastructure-as-code file, which deploys 3 Lambdas

* A GSheet Read Lambda (`gsheet_read_handler`) 
* A GSheet Write Lambda (`gsheet_write_handler`)
* A GSheet -> S3 data sync cron service (`s3_gsheets_sync_handler`)

and a S3 bucket:
 
* `gsheet-backup-bucket-${env:AWS_ACCOUNT_ID}`

Deploys are managed through a CloudFormation Stack (called `gsheet-utilities`)

To create the CloudFormation Stack (and also subsequently update it), use:
``` 
sls deploy
```

You can test the each Lambda locally (be aware the write will change your GSheet):
```
sls invoke local -f gsheet-read -d '{"Gsheet":"1tgTWvAKqX-qOABGtdAZIeJpjOEDro2iDGMS4O8z1fFA", "Tab":"Sheet1"}'

sls invoke local -f gsheet-write -d '{"Gsheet":"1tgTWvAKqX-qOABGtdAZIeJpjOEDro2iDGMS4O8z1fFA", "Tab":"Sheet1","Type":"Overwrite", "Data":[{"col1":"hello","col2":world},{"col1":232,"col2":"mixed type columns are OK"}]}'

# to use the sync, you'll need to set the GSheet ID and Tag as env vars
export GSHEET_ID=44charIDorTheNameItself
export GSHEET_TAB=Sheet2
sls invoke local -f s3-sync
```
All the resources fit easily in the AWS Free Tier and should have no ongoing costs (presuming you stay in the Free Tier, particularly on S3 storage).

To take down the CloudFormation Stack and associated Lambdas and S3, use:
```
sls remove
```
