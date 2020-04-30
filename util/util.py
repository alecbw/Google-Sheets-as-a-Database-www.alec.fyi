import json
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_params(event, required_params, **kwargs):
    event = standardize_event(event)
    commom_required_params = get_list_overlap(event, required_params)
    commom_optional_params = get_list_overlap(event, kwargs.get("optional_params", []))

    param_only_dict = {k:v for k, v in event.items() if k in required_params+kwargs.get("optional_params", [])}
    logging.info(f"Total param dict: {param_only_dict}")
    logging.info(f"Found optional params: {commom_optional_params}")

    if commom_required_params != required_params:
        missing_params = [x for x in required_params if x not in event]
        return param_only_dict, missing_params

    return param_only_dict, False


def standardize_event(event):
    if event.get("body"):  # POST, synchronous API Gateawy
        body_dict = {x[0]:x[1] for x in [x.split("=") for x in event["body"].split("&")]}
        event = {**event, **body_dict}
    elif event.get("queryStringParameters"):  # GET, synchronous API Gateway
        event.update(event["queryStringParameters"])
    elif event.get("query"):  # GET, async API Gateway
        event.update(event["query"])

    return standardize_dict(event)


def package_response(message, status_code, **kwargs):
    if kwargs.get("log"):
        logging.info(message)
    elif kwargs.get("warn"):
        logging.warning(message)

    return {
        'statusCode': status_code if status_code else '200',
        'body': json.dumps({'data': message}),
        'headers': {'Content-Type': 'application/json'}
    }


def invoke_lambda(params, function_name, invoke_type):

    lambda_client = boto3.client("lambda")
    lambda_response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=invoke_type,
        Payload=json.dumps(params),
    )
    # Async Invoke returns only StatusCode
    if invoke_type.title() == "Event":
        return None, lambda_response.get("StatusCode", 666)

    string_response = lambda_response["Payload"].read().decode("utf-8")
    json_response = json.loads(string_response)

    if not json_response:
        return "Unknown error: no json_response. Called lambda may have timed out.", 500
    elif json_response.get("errorMessage"):
        return json_response.get("errorMessage"), 500

    status_code = int(json_response.get("statusCode"))
    json_body = json.loads(json_response.get("body"))

    if json_response.get("body") and json_body.get("error"):
        return json_body.get("error").get("message"), status_code

    return json_body["data"], status_code
