from utils.util import invoke_lambda

import os
import logging
import json
import pandas as pd
import boto3

############################################################################################


def lambda_handler(event, context):

    tab_data, status = invoke_lambda(
        {
            "Gsheet": os.environ["GSHEET_ID"],
            "Tab": os.environ["GSHEET_TAB"],
            "Drop_Empty_Rows": True,
            "Drop_Empty_Columns": True,
            "Source":"Lambda: S3 Sync"
        },
        "s3-sync-cron-prod-gsheet-read",
        "RequestResponse"
    )
    logging.info(f"Status code of data fetch from GSheet is {status}")
    if status not in [200, 202]:
        return

    s3_object = boto3.resource("s3").Object(
        f"gsheet-backup-bucket-{os.environ['AWS_ACCOUNT_ID']}-1",
        f"AutoSync GSheet: {os.environ['GSHEET_ID']}.csv"
    )
    output = s3_object.put(Body=(bytes(json.dumps(tab_data).encode("UTF-8"))))
    logging.info(f"Status code of S3 Write is {output['ResponseMetadata']['HTTPStatusCode']}")

