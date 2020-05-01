from utils.util import package_response, validate_params
from utils.util_gspread import open_gsheet, get_gsheet_tab

import os
import logging

import pandas as pd

##############################################



def lambda_handler(event, context):
    param_dict, missing_params = validate_params(event,
        required_params=['Gsheet', 'Tab'],
        optional_params=["Drop_Empty_Columns", "Drop_Empty_Rows"]
    )
    if missing_params:
        return package_response(f"Missing required params {missing_params}", 422)

    result = read_from_gsheet(param_dict)

    if isinstance(result, list):
        return package_response(result, 200)
    else:
        return package_response(result, 500)


# Weird interactions with no column headers
def read_from_gsheet(params):
    sh, worksheet_list = open_gsheet(params["Gsheet"])
    tab, existing_lod = get_gsheet_tab(sh, tab_name=params["Tab"])
    tab_df = pd.DataFrame(existing_lod)

    tab_df_cols = tab_df.columns.values.tolist()

    # Drop cols w/ no header AND no values in any cell in that column
    if params.get("Drop_Empty_Columns"):
        tab_df = tab_df.dropna(how="all", axis=1)
        logging.info("~ Empty columns dropped successfully. ~ ")

    # Drop rows with NaN values in all cells.
    if params.get("Drop_Empty_Rows"):
        tab_df = tab_df.dropna(how='all', axis=0)
        logging.info("~ Empty rows dropped successfully. ~ ")

    # Dedupe columns
    if len(tab_df_cols) != len(set(tab_df_cols)):
        logging.warn(f"You have duplicate column headers in your GSheet")
        return "You have duplicate column headers in your GSheet"

    logging.info(f"~ Successfully read GSheet: {params['Gsheet']} - {params['Tab']} ~ ")
    return tab_df.to_dict("records")
