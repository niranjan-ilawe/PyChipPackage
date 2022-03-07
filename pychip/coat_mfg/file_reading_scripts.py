import ezsheets
import pandas as pd
from pygbmfg.common import _load_credentials, _clear_credentials


def read_chip_ballooning_gsheet(
    sheet_id="1j1o12cNHxU1CUnsv9u-l07PyY6onZa5IrGmAL3WcVTc",
):

    _load_credentials()
    ss = ezsheets.Spreadsheet(sheet_id)
    sheet1 = ss["Updated Ballooning Data"]

    d = {
        "wo": sheet1.getColumn(1),
        "day": sheet1.getColumn(2),
        "test_no": sheet1.getColumn(3),
        "qc_operator": sheet1.getColumn(4),
        "qc_date": sheet1.getColumn(5),
        "coating_date": sheet1.getColumn(6),
        "coating_round": sheet1.getColumn(7),
        "replicate": sheet1.getColumn(9),
        "chip_layout": sheet1.getColumn(10),
        "tray_temp": sheet1.getColumn(11),
        "hsv_system": sheet1.getColumn(12),
        "rt_reagent_b": sheet1.getColumn(13),
        "tso": sheet1.getColumn(14),
        "reducing_reagent_b": sheet1.getColumn(15),
        "rt_enzyme_b": sheet1.getColumn(16),
        "pbs": sheet1.getColumn(17),
        "partioning_oil": sheet1.getColumn(18),
        "sc_gb_strip": sheet1.getColumn(19),
        "failure_mode": sheet1.getColumn(20),
        "base_ggf": sheet1.getColumn(21),
        "critical_pressure": sheet1.getColumn(22),
        "norm_critical_pressure": sheet1.getColumn(23),
        "base_ggf_25c": sheet1.getColumn(24),
        "novec_lot": sheet1.getColumn(26),
        "novec_lot_date": sheet1.getColumn(27),
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