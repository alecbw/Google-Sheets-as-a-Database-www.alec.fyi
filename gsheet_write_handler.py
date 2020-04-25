from util_gspread import open_gsheet, get_gsheet_tab, create_gsheet_worksheet

from itertools import chain, zip_longest
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


    input_df, existing_df, tab = validate_gsheet_tab(param_dict)
    logging.info(f"input_df shape {input_df.shape}")
    logging.info(f"input_df columns {input_df.columns.tolist()}")

    if param_dict["Type"].title() == "Overwrite":
        overwrite_gsheet_tab(input_df, tab, param_dict)
        return package_response("Overwrite successful", 200)
    elif not isinstance(existing_df, pd.DataFrame):
        logging.info(f"Did an overwrite even though you wanted {param_dict['Type']} because the tab was newly created")
        overwrite_gsheet_tab(input_df, tab, param_dict)
        return package_response("New tab created; write successful", 200)

    ordered_existing_columns = existing_df.columns.values.tolist()
    column_overlap = set(input_df.columns.tolist()).issubset(ordered_existing_columns)

    if not column_overlap:
        return package_response(f"The columns of your input data {input_df.columns.tolist()} are not a subset of the columns of the existing tab {ordered_existing_columns}.", 422)
    elif not "Primary_Key" in param_dict and param_dict["Type"] in ["Append_Uniques", "Combine_Uniques"]:
        return package_response(f"You need a Primary_Key to do {param_dict['Type']}", 422)

    # The following types require column_overlap
    if param_dict["Type"].title() == "Append_All":
        updated_df = append_all(input_df, existing_df, param_dict)

    elif param_dict["Type"].title() == "Append_Uniques":
        updated_df, message = append_uniques(input_df, existing_df, param_dict)
        if message:
            return package_response(message, 200)

    elif param_dict["Type"].title() == "Combine_Uniques":
        updated_df = combine_and_deduplicate(input_df, existing_df, param_dict)

    else:
        return package_response(f"Unsupported Type: {param_dict['Type']} ", 422)

    # Any combination of these options should work
    if param_dict.get("Drop_Empty_Rows"):
        updated_df = drop_empty_rows(updated_df, "Output Dataframe")
    if param_dict.get("Drop_Empty_Columns"):
        updated_df, ordered_existing_columns = drop_empty_columns(updated_df, "Output Dataframe")
    if param_dict.get("Maintain_Column_Order"):
        updated_df = updated_df[ordered_existing_columns]

    set_with_dataframe(tab, updated_df, resize=param_dict.get("Resize", True))

    return package_response(f"{param_dict['Type']} successful", 200)



############################ Tab and DF helpers ###########################################


def drop_empty_rows(df, df_type): # DATSTORES?
    df = df.dropna(how='all', axis=0)
    logging.info(f"Empty rows successfully dropped from {df_type}")
    return df

# This will drop Unnamed columns if they are entirely empty
def drop_empty_columns(df, df_type):
    df = df.dropna(how='all', axis=1)
    logging.info(f"Empty columns successfully dropped from {df_type}")
    return df, df.columns.tolist()


def deduplicate_df(df, column):
    df = df.drop_duplicates(subset=column, keep="last")
    df.reset_index(drop=True, inplace=True)
    return df

######################## Main Operators ################################################


def validate_gsheet_tab(params):
    input_df = pd.DataFrame(params["Data"])
    sh, worksheet_list = open_gsheet(params["Gsheet"])

    if params["Tab"] in worksheet_list:
        tab, existing_lod = get_gsheet_tab(sh, params["Tab"])
        existing_df = pd.DataFrame(existing_lod)
    else:
        logging.info(f"Tab name not found. Making new tab: {params['Tab']}.")
        tab = create_gsheet_worksheet(sh, params["Tab"], rows=input_df.shape[0], columns=input_df.shape[1])
        existing_df = None

    return input_df, existing_df, tab


def overwrite_gsheet_tab(input_df, tab, params):
    if params.get("Drop_Empty_Rows"):
        input_df = drop_empty_rows(input_df, "Input Dataframe")

    tab.clear()
    set_with_dataframe(tab, input_df, resize=params.get("Resize", True))
    logging.info("~ Overwritten successfully. ~ ")
    return True


def append_all(input_df, existing_df, params):
    existing_df = existing_df.append(input_df)
    logging.info(f"~ Matching columns. {len(input_df.index)} new rows added. ~ ")
    return existing_df


# Note: Uniqueness based on Primary_Key, not whether the entire row is unique
def append_uniques(input_df, existing_df, params):
    input_df_net_new = input_df[~input_df[params["Primary_Key"]].isin(existing_df[params["Primary_Key"]])]
    input_df_net_new = deduplicate_df(input_df_net_new, params["Primary_Key"])

    if len(input_df_net_new.index) == 0:
        logging.info(f"~ Matching columns. No unique rows to add. ~ ")
        return None, "Matching columns. No unique rows to add."
    else:
        logging.info(f"~ Matching columns. {len(input_df_net_new.index)} Unique rows added. ~ ")
        return existing_df.append(input_df_net_new), None


def combine_and_deduplicate(input_df, existing_df, params):
    merged_dfs = pd.concat([input_df, existing_df], axis=0, sort=False)
    merged_dfs = deduplicate_df(merged_dfs, params["Primary_Key"])
    logging.info("~ Matching columns. Existing sheet deduplicated and unique rows added. ~ ")
    return merged_dfs


######################## DF -> GSheet Conversion ################################################

def _cellrepr(value, allow_formulas):
    """
    Get a string representation of dataframe value.
    :param :value: the value to represent
    :param :allow_formulas: if True, allow values starting with '='
            to be interpreted as formulas; otherwise, escape
            them with an apostrophe to avoid formula interpretation.
    """
    if pd.isnull(value) is True:
        return ""
    if isinstance(value, float):
        value = repr(value)
    else:
        value = str(value)
    if value.startswith("'") or ((not allow_formulas) and value.startswith('=')):
        value = "'%s" % value
    return value

# From https://github.com/robin900/gspread-dataframe/blob/master/gspread_dataframe.py
def set_with_dataframe(worksheet,
                       dataframe,
                       row=1,
                       col=1,
                       include_index=False,
                       include_column_header=True,
                       resize=False,
                       allow_formulas=True):
    """
    Sets the values of a given DataFrame, anchoring its upper-left corner
    at (row, col). (Default is row 1, column 1.)
    :param worksheet: the gspread worksheet to set with content of DataFrame.
    :param dataframe: the DataFrame.
    :param include_index: if True, include the DataFrame's index as an
            additional column. Defaults to False.
    :param include_column_header: if True, add a header row before data with
            column names. (If include_index is True, the index's name will be
            used as its column's header.) Defaults to True.
    :param resize: if True, changes the worksheet's size to match the shape
            of the provided DataFrame. If False, worksheet will only be
            resized as necessary to contain the DataFrame contents.
            Defaults to False.
    :param allow_formulas: if True, interprets `=foo` as a formula in
            cell values; otherwise all text beginning with `=` is escaped
            to avoid its interpretation as a formula. Defaults to True.
    """
    # x_pos, y_pos refers to the position of data rows only,
    # excluding any header rows in the google sheet.
    # If header-related params are True, the values are adjusted
    # to allow space for the headers.
    y, x = dataframe.shape
    if include_index:
        x += 1
    if include_column_header:
        y += 1
    if resize:
        worksheet.resize(y, x)

    updates = []

    if include_column_header:
        elts = list(dataframe.columns)
        if include_index:
            elts = [ dataframe.index.name ] + elts
        for idx, val in enumerate(elts):
            updates.append(
                (row,
                 col+idx,
                 _cellrepr(val, allow_formulas))
            )
        row += 1

    values = []
    for value_row, index_value in zip_longest(dataframe.values, dataframe.index):
        if include_index:
            value_row = [index_value] + list(value_row)
        values.append(value_row)
    for y_idx, value_row in enumerate(values):
        for x_idx, cell_value in enumerate(value_row):
            updates.append(
                (y_idx+row,
                 x_idx+col,
                 _cellrepr(cell_value, allow_formulas))
            )

    if not updates:
        logging.debug("No updates to perform on worksheet.")
        return

    cells_to_update = [ Cell(row, col, value) for row, col, value in updates ]
    logging.debug("%d cell updates to send", len(cells_to_update))

    resp = worksheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
    logging.debug("Cell update response: %s", resp)


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
