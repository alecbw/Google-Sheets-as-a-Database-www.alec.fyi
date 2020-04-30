
import os
import logging
import json
from gspread.models import Cell
import pandas as pd

# Note: the .clear() (overwrite and combine) will freak people out if they are using it during

############################################################################################


def lambda_handler(event, context):
    param_dict, missing_params = validate_params(event,
        required_params=["Data", "Gsheet", "Tab", "Type"],
        optional_params=["Resize", "Drop_Empty_Rows", "Drop_Empty_Columns", "Primary_Key", "Maintain_Column_Order"]
    )
    if missing_params:
        return package_response(f"Missing required params {missing_params}", 422)

    return package_response(f"{param_dict['Type']} successful", 200)



######################## Standard Lambda Helpers ################################################


def validate_params(event, required_params, **kwargs):
    event = standardize_event(event)
    commom_required_params = list(set(event).intersection(required_params))
    commom_optional_params = list(set(event).intersection(kwargs.get("optional_params", [])))

    param_only_dict = {k: v for k, v in event.items() if k in required_params + kwargs.get("optional_params", [])}
    logging.info(f"Total param dict: {param_only_dict}")
    logging.info(f"Found optional params: {commom_optional_params}")

    if commom_required_params != required_params:
        missing_params = [x for x in required_params if x not in event]
        return param_only_dict, missing_params

    return param_only_dict, False


def standardize_event(event):
    if "queryStringParameters" in event:
        event.update(event["queryStringParameters"])
    elif "query" in event:
        event.update(event["query"])

    result_dict = {
        k.title().strip().replace(" ", "_"):(False if v == "false" else v)
        for (k, v) in event.items()
    }
    return result_dict


def package_response(message, status_code, **kwargs):
    return {
        "statusCode": status_code if status_code else "200",
        "body": json.dumps({"data": message}),
        "headers": {"Content-Type": "application/json"},
    }
