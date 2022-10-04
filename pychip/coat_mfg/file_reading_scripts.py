import ezsheets, re
import pandas as pd
from pygbmfg.common import _load_credentials, _clear_credentials
from pydb import get_snowflake_connection, get_postgres_connection
import datetime as dt


def read_chip84_ballooning_gsheet(
    sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc",
):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["GEM 84 Ballooning Data"]

    d = {
        "chip_type": sheet1.getColumn(1),
        "wo": sheet1.getColumn(2),
        "day": sheet1.getColumn(3),
        "test_no": sheet1.getColumn(4),
        "qc_operator": sheet1.getColumn(5),
        "qc_date": sheet1.getColumn(6),
        "coating_date": sheet1.getColumn(7),
        "coating_round": sheet1.getColumn(8),
        "replicate": sheet1.getColumn(9),
        "chip_layout": sheet1.getColumn(10),
        "nozzle": sheet1.getColumn(11),
        "tray_temp": sheet1.getColumn(12),
        "hsv_system": sheet1.getColumn(13),
        "gem_reagent_mix": sheet1.getColumn(14),
        "post_hyb_buffer": sheet1.getColumn(15),
        "reducing_reagent_b": sheet1.getColumn(16),
        "gem_enzyme_mix": sheet1.getColumn(17),
        "partioning_oil": sheet1.getColumn(18),
        "next_gem_rtl_gb": sheet1.getColumn(19),
        "failure_mode": sheet1.getColumn(21),
        "base_ggf": sheet1.getColumn(20),
        "norm_critical_pressure": sheet1.getColumn(22),
        "base_ggf_25c": sheet1.getColumn(23),
        "novec_lot": sheet1.getColumn(25),
        "novec_lot_date": sheet1.getColumn(26),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]

    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["norm_critical_pressure"])
    _clear_credentials()
    return df


def read_chip74_ballooning_gsheet(
    sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc",
):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["GEM 74 Ballooning Data"]

    d = {
        "chip_type": sheet1.getColumn(1),
        "wo": sheet1.getColumn(2),
        "day": sheet1.getColumn(3),
        "test_no": sheet1.getColumn(4),
        "qc_operator": sheet1.getColumn(14),
        "qc_date": sheet1.getColumn(15),
        "coating_date": sheet1.getColumn(16),
        "coating_round": sheet1.getColumn(17),
        "replicate": sheet1.getColumn(18),
        "chip_layout": sheet1.getColumn(19),
        "nozzle": sheet1.getColumn(20),
        "run_id": sheet1.getColumn(21),
        "hsv_system": sheet1.getColumn(5),
        "rt_reagent_b": sheet1.getColumn(7),
        "tso": sheet1.getColumn(8),
        "reducing_reagent_b": sheet1.getColumn(9),
        "rt_enzyme_c": sheet1.getColumn(10),
        "pbs": sheet1.getColumn(11),
        "partioning_oil": sheet1.getColumn(12),
        "sc_gb_strip": sheet1.getColumn(13),
        "start_step": sheet1.getColumn(22),
        "failure_mode": sheet1.getColumn(23),
        "short_ggf_mean": sheet1.getColumn(24),
        "short_bif": sheet1.getColumn(25),
        "short_ggf_cv": sheet1.getColumn(26),
        "norm_critical_pressure": sheet1.getColumn(27),
        "ggf_135_to_160": sheet1.getColumn(28),
        "overall_disposition": sheet1.getColumn(29),
        "novec_lot": sheet1.getColumn(31),
        "novec_lot_date": sheet1.getColumn(32),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]

    # clean empty rows
    # first add NaN to all empty cells
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)

    # drop all rows that have a NaN in the norm critical pressure col
    df = df.dropna(subset=["norm_critical_pressure"])

    # shortening pbs lots
    df['pbs'] = df['pbs'].str[:19]

    _clear_credentials()
    return df


def read_chip55_ballooning_gsheet(
    sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc",
):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["GEM 55 Ballooning Data"]

    d = {
        "chip_type": sheet1.getColumn(1),
        "wo": sheet1.getColumn(2),
        "day": sheet1.getColumn(3),
        "test_no": sheet1.getColumn(4),
        "qc_operator": sheet1.getColumn(5),
        "qc_date": sheet1.getColumn(6),
        "coating_date": sheet1.getColumn(7),
        "coating_round": sheet1.getColumn(8),
        "replicate": sheet1.getColumn(10),
        "chip_layout": sheet1.getColumn(11),
        "tray_temp": sheet1.getColumn(12),
        "hsv_system": sheet1.getColumn(13),
        "rt_reagent_b": sheet1.getColumn(14),
        "tso": sheet1.getColumn(15),
        "reducing_reagent_b": sheet1.getColumn(16),
        "rt_enzyme_c": sheet1.getColumn(17),
        "pbs": sheet1.getColumn(18),
        "partioning_oil": sheet1.getColumn(19),
        "sc_gb_strip": sheet1.getColumn(20),
        "failure_mode": sheet1.getColumn(21),
        "base_ggf": sheet1.getColumn(22),
        "critical_pressure": sheet1.getColumn(23),
        "norm_critical_pressure": sheet1.getColumn(24),
        "base_ggf_25c": sheet1.getColumn(25),
        "novec_lot": sheet1.getColumn(27),
        "novec_lot_date": sheet1.getColumn(28),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]

    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["norm_critical_pressure"])

    # shortening pbs lots to 20 chars
    df['pbs'] = df['pbs'].str[:19]

    _clear_credentials()
    return df


def read_chip_yield_gsheet(sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc"):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["Step Coat KPI"]

    d = {
        "date": sheet1.getColumn(1),
        "wo": sheet1.getColumn(5),
        "inventory": sheet1.getColumn(8),
        "failed": sheet1.getColumn(10),
        "consumed": sheet1.getColumn(11),
        "qc": sheet1.getColumn(17),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]

    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["date"])
    _clear_credentials()

    return df


def read_chip_cust_complaint_gsheet(
    sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc",
):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["Chip Complaints"]

    d = {
        "submit_date": sheet1.getColumn(2),
        "type": sheet1.getColumn(7),
        "affected": sheet1.getColumn(19),
        "reason": sheet1.getColumn(20),
    }

    df = pd.DataFrame(d)
    df = df[df["type"] == "Chip"]
    # drop first row
    # df = df.iloc[1:, :]

    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["submit_date"])
    _clear_credentials()

    return df


def read_chip_error_gsheet(sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc"):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["Step Failures"]

    d = {
        "wo": sheet1.getColumn(1),
        "date": sheet1.getColumn(2),
        "process": sheet1.getColumn(3),
        "qty": sheet1.getColumn(5),
        "reason": sheet1.getColumn(11),
    }

    df = pd.DataFrame(d)
    # drop first row
    df = df.iloc[1:, :]

    # clean empty rows
    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(subset=["date"])
    _clear_credentials()

    return df


def read_chip_yield_rev_F(file):

    xlsx = pd.ExcelFile(file, engine="openpyxl")

    # try reading sheet named Daily Production
    try:
        df_prod = pd.read_excel(xlsx, sheet_name="Daily Production", header=None)
    except:
        print(f"Unable to read sheet named: Daily Production")
        return pd.DataFrame()

    df_start = df_prod[df_prod[0] == "Round"].index.tolist()
    df_prod = df_prod[1 + df_start[0] :]
    df_prod = df_prod.reset_index()
    df_prod = df_prod[[0, 1, 2, 3, 4, 5, 6]]
    df_prod = df_prod.dropna()
    df_prod.columns = ["round", "day", "date", "consumed", "failed", "qc", "inventory"]

    # try reading sheet named Summary
    try:
        df_summary = pd.read_excel(xlsx, sheet_name="Summary", header=None)
    except:
        print(f"Unable to read sheet named: Summary")
        return pd.DataFrame()

    pn = df_summary[df_summary[2] == "Part Number: "].iloc[0, 5]
    wo = df_summary[df_summary[2] == "WORK ORDER #:"].iloc[0, 5]

    # print(f"Length of df_prod is {len(df_prod)}")
    # print(f"Length of df_error is {len(df_error)}")

    # df = pd.merge(df_prod, df_error, how="left", on=["date", "round"])

    # print(f"Length of df is {len(df)}")

    df_prod = df_prod.assign(pn=pn, wo=wo)

    return df_prod


def read_chip_error_rev_F(file):

    xlsx = pd.ExcelFile(file, engine="openpyxl")
    # try reading error details
    try:
        df_error = pd.read_excel(xlsx, sheet_name="Failure Mode", header=None)
    except:
        print(f"Unable to read sheet named: Failure Mode")
        return pd.DataFrame()

    df_start = df_error[df_error[1] == "Process"].index.tolist()
    df_error = df_error[1 + df_start[0] :]
    df_error = df_error[[0, 1, 2, 3, 4]]
    df_error.columns = ["date", "process", "round", "qty", "reason"]
    df_error.groupby(["date", "process", "round", "reason"])["qty"].sum().reset_index()

    # try reading sheet named Summary
    try:
        df_summary = pd.read_excel(xlsx, sheet_name="Summary", header=None)
    except:
        print(f"Unable to read sheet named: Summary")
        return pd.DataFrame()

    pn = df_summary[df_summary[2] == "Part Number: "].iloc[0, 5]
    wo = df_summary[df_summary[2] == "WORK ORDER #:"].iloc[0, 5]

    # print(f"Length of df_prod is {len(df_prod)}")
    # print(f"Length of df_error is {len(df_error)}")

    # df = pd.merge(df_prod, df_error, how="left", on=["date", "round"])

    # print(f"Length of df is {len(df)}")

    df_error = df_error.assign(pn=pn, wo=wo)

    return df_error


def read_runout_data(sheet_id="1x2Terv49S-w4rKJB7HOGv6JJQmkhOblKIAlEkR9RtdU"):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["Chip Inventory Forecast"]

    # df = sheet1.getColumns(startColumn=1, stopColumn=sheet1.rowCount)

    df = sheet1.getRows(startRow=1, stopRow=sheet1.rowCount)
    df = pd.DataFrame(df)
    colnames = sheet1.getRow(4)
    df = df.set_axis(colnames, axis=1, inplace=False)
    df = df[df["Category "] == "Projected Inventory w/o production"]

    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df = df.dropna(how="all", axis=1)

    # drop rows if the Item column has NaN
    df = df.dropna(subset=['Item'])

    df = df.drop(columns=["Category ", "Inventory including higher level parts", "Lot"])
    df = df.melt(id_vars=["Item", "Description"], var_name="month", value_name="qty")
    df = df.rename(columns={"Item": "item", "Description": "descrip"})

    # drop rows if the Item column has NaN
    df = df.dropna(subset=['qty'])

    _clear_credentials()
    return df


def read_chip_yield_revEFG(file):

    xlsx = pd.ExcelFile(file, engine="openpyxl")

    # try reading sheet named Daily Production
    try:
        details = pd.read_excel(xlsx, sheet_name="Summary", header=None)
        wo = details[details[2] == "WORK ORDER #:"].iloc[0, 5]
        summary = pd.read_excel(xlsx, sheet_name="Daily Production", header=5)
        summary = summary.drop(
            columns=[
                "HFE-7100 Expiration date (MM/DD/YY)",
                "Novec-1720 Expiration date (MM/DD/YY)",
            ]
        )
        summary = summary.clean_names()
        summary = summary[
            [
                "round",
                "day",
                "date_mm_dd_yy_",
                "#_of_chips_consumed_per_round",
                "#_of_chips_failed_per_round",
                "#_of_chips_routed_to_qc",
                "#_of_coated_chips_to_inventory_per_round",
                "day_1",
                "chips_expiration_date_mm_dd_yy_",
                "hfe_7100_lot_number",
                "hfe_7100_bottle_open_date_mm_dd_yy_",
                "hfe_7100_expiration_date_mm_dd_yy_",
                "novec_1720_lot_number",
                "novec_1720_bottle_open_date_mm_dd_yy_",
                "novec_1720_expiration_date_mm_dd_yy_",
                "water_concentration_g_m^3_per_round",
                "temp_°c_per_round",
            ]
        ]

        summary = summary.rename(
            columns={
                "round": "round",
                "day": "day",
                "date_mm_dd_yy_": "date",
                "#_of_chips_consumed_per_round": "no_of_chips_consumed_per_round",
                "#_of_chips_failed_per_round": "no_of_chips_failed_per_round",
                "#_of_chips_routed_to_qc": "no_of_chips_routed_to_qc",
                "#_of_coated_chips_to_inventory_per_round": "no_of_coated_chips_to_inventory_per_round",
                "day_1": "day_1",
                "chips_expiration_date_mm_dd_yy_": "chips_expiration",
                "hfe_7100_lot_number": "hfe_7100_lot_number",
                "hfe_7100_bottle_open_date_mm_dd_yy_": "hfe_7100_bottle_open_date",
                "hfe_7100_expiration_date_mm_dd_yy_": "hfe_7100_expiration_date",
                "novec_1720_lot_number": "novec_1720_lot_number",
                "novec_1720_bottle_open_date_mm_dd_yy_": "novec_1720_bottle_open_date",
                "novec_1720_expiration_date_mm_dd_yy_": "novec_1720_expiration_date",
                "water_concentration_g_m^3_per_round": "water_concentration_per_round",
                "temp_°c_per_round": "temp_c_per_round",
            }
        )
        summary = summary.dropna(subset=["date"])
        summary = summary[summary["no_of_chips_consumed_per_round"] != 0]
        summary = summary.dropna(subset=["no_of_chips_consumed_per_round"])
        summary = summary.fillna(method="pad")

        summary["hfe_7100_life_remaining"] = (
            summary["hfe_7100_expiration_date"] - summary["date"]
        ).dt.days
        summary["novec_1720_life_remaining"] = (
            summary["novec_1720_expiration_date"] - summary["date"]
        ).dt.days
        summary["bottle_age_7100"] = (
            summary["date"] - summary["hfe_7100_bottle_open_date"]
        ).dt.days
        summary["bottle_age_1720"] = (
            summary["date"] - summary["novec_1720_bottle_open_date"]
        ).dt.days

        summary = summary.assign(wo=wo)
    except:
        print(f"Unable to read sheet named: Daily Production")
        return pd.DataFrame()

    return summary


def read_wo_misses_gsheet():
    snowflake_conn = get_snowflake_connection()
    postgres_conn = get_postgres_connection()

    #last_modified_date = str(date.today() - timedelta(days=10))
    last_modified_date = "2022-04-01"
    print(f"Looking for new data since {last_modified_date} ....")


    query = f"""
    select distinct
    wo_status_name,
    work_order_number, 
    i.item_name, 
    i.item_description, 
    --mfg_area, mfg_process, 
    work_center_name,
    cast(creation_date as date) as creation_date, 
    cast(actual_start_date as date) as actual_start_date, 
    cast(wo_required_date as date) as wo_required_date, 
    cast(actual_completion_date as date) as actual_completion_date, 
    cast(wo_completion_date as date) as wo_completion_date, 
    --cast(closed_date as date) as closed_date,
    planned_start_quantity, completed_quantity,
    datediff('day', wo_required_date, wo_completion_date) as timediff
    --closed_date_skey, actual_completion_date_skey, wo_completion_date_skey
    --*
    from PROD_ENT_PRESENTATION_DB.FACTS.WORK_ORDERS_F f
    join PROD_ENT_PRESENTATION_DB.DIMS.WORK_ORDERS_D d
    on f.wo_skey = d.wo_skey
    join PROD_ENT_PRESENTATION_DB.DIMS.ITEM_D i
    on f.item_skey = i.item_skey
    where 1=1
    and ((closed_date_skey > '20220101' and closed_date_skey <> '99990101') 
         or (actual_completion_date_skey > '20220101' and actual_completion_date_skey <> '99990101') 
         or (wo_completion_date_skey > '20220101' and wo_completion_date_skey <> '99990101'))
    --and released_date_skey < '20221001'
    --and actual_completion_date_skey > '20220802'
    and distribution_center in ('10X Pleasanton', '10X Singapore MFG')
    and wo_status_name not in ('Cancelled')
    --and work_order_number = 'WO164790'
    --and completed_quantity = '0'
    order by cast(creation_date as date)
    ;
    """

    df = pd.read_sql(query, snowflake_conn)
    df['ON_TIME'] = df['TIMEDIFF'].apply(lambda x: 'TRUE' if x <= 3 else 'FALSE')

    _load_credentials()
    ss = ezsheets.Spreadsheet("1XxNS-GYWkhqzooASpQbn-wwH_YlPvxZlZ62xFGzI9Ek")

    sheet = ss['Sheet1']

    #sheet.updateRows(result)

    orig = sheet.getRows()
    df_orig = pd.DataFrame(orig[1:], columns = orig[0])
    nan_value = float("NaN")
    df_orig.replace("", nan_value, inplace=True)
  
    df_orig.dropna(how='all', axis=1, inplace=True)
    df_orig.dropna(how='all', inplace=True)

    # get manually entered mapping from google sheet
    #mapping = df_orig[['ITEM_NAME', 'MFG_AREA', 'MFG_PROCESS']].drop_duplicates(keep = "last")
    #df = df.fillna(mapping)

    area_map = df_orig[['ITEM_NAME', 'MFG_AREA', 'MFG_PROCESS']].drop_duplicates()

    df = df.merge(area_map, how='left', on='ITEM_NAME')

    df_orig = df_orig[['WORK_ORDER_NUMBER', "Reason_Code_1", "Reason_Code_2"]]
    df_orig = df_orig.drop_duplicates()

    ## Get QC data
    query = """
    select clean_wo, status, pass_rate
    from yield.qc_data
    """
    qc = pd.read_sql(query, postgres_conn)

    qc['list'] = qc['pass_rate'].apply(lambda x: re.findall('\\d+.\\d+', x))
    qc['pass_rate'] = qc['list'].apply(lambda x: int(bool(x)) if not bool(x) else x[0]).astype(float)

    # remove WO that do not have dispositions yet
    qc = qc[qc['status'].str.contains('Troubleshooting|Scrap|Rework|Pass|Return to Vendor|Use As Is')]

    # convert status text to int
    qc['status_int'] = qc['status'].apply(lambda x: 1 if x in ('Pass', 'Use As Is') else 0)

    # get status and pass_rate means
    qc = qc.groupby('clean_wo', as_index=False).agg({'pass_rate':'mean', 'status_int':'mean'})

    qc['first_pass'] = qc['pass_rate'].apply(lambda x: "Fail" if x < 100 else "Pass")
    qc['final_pass'] = qc['status_int'].apply(lambda x: "Fail" if x < 1 else "Pass")
    qc['clean_wo'] = qc['clean_wo'].apply(lambda x: "WO" + x)
    qc = qc[['clean_wo', 'first_pass', 'final_pass']]
    qc = qc.rename(columns={"clean_wo": "WORK_ORDER_NUMBER"})

    df_orig = df_orig.merge(qc, on = "WORK_ORDER_NUMBER", how = 'left')

    final = df.merge(df_orig, on = 'WORK_ORDER_NUMBER', how= 'left')
    #final = df_orig.merge(df, on = "WORK_ORDER_NUMBER", how = "outer")
    #df_orig['WORK_ORDER_NUMBER'] = df_orig['WORK_ORDER_NUMBER'].astype('string')

    final = final[['WO_STATUS_NAME',
                'WORK_ORDER_NUMBER', 
               'ITEM_NAME', 
               'ITEM_DESCRIPTION',
               'MFG_AREA',
               'MFG_PROCESS',
               'WORK_CENTER_NAME',
               'CREATION_DATE',
               'ACTUAL_START_DATE',
               'WO_REQUIRED_DATE',
               'ACTUAL_COMPLETION_DATE',
               'WO_COMPLETION_DATE',
               'PLANNED_START_QUANTITY',
               'COMPLETED_QUANTITY',
               'TIMEDIFF',
               'ON_TIME',
               'Reason_Code_1',
               'Reason_Code_2',
               'first_pass',
               'final_pass'
                ]]

    final['CREATION_DATE'] = final['CREATION_DATE'].apply(lambda x: dt.date.strftime(x, "%Y-%m-%d"))
    final['ACTUAL_START_DATE'] = final['ACTUAL_START_DATE'].apply(lambda x: dt.date.strftime(x, "%Y-%m-%d"))
    final['WO_REQUIRED_DATE'] = final['WO_REQUIRED_DATE'].apply(lambda x: dt.date.strftime(x, "%Y-%m-%d"))
    final['ACTUAL_COMPLETION_DATE'] = final['ACTUAL_COMPLETION_DATE'].apply(lambda x: dt.date.strftime(x, "%Y-%m-%d"))
    final['WO_COMPLETION_DATE'] = final['WO_COMPLETION_DATE'].apply(lambda x: dt.date.strftime(x, "%Y-%m-%d"))
    final = final.fillna('')

    result = final.values.tolist()
    sheet.updateRows(result, startRow=2)

    _clear_credentials()

    return final