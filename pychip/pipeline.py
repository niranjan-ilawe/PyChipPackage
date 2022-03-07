from termcolor import colored
from pydb import get_postgres_connection, batch_upload_df

from pychip.coat_mfg.file_reading_scripts import (
    read_chip_ballooning_gsheet,
    read_chip_error_gsheet,
    read_chip_yield_gsheet,
)

from pychip.coat_mfg.df_creation_scripts import get_coat_yield_data, get_coat_error_data


def run_chip_pipeline(days=3):
    print(colored("******* Chip Pipeline Starting *******", "green"))

    conn = get_postgres_connection(
        service_name="cpdda-postgres", username="cpdda", db_name="cpdda"
    )

    print("------ Getting Coating GSheet Yield Data ------")
    try:
        df = read_chip_yield_gsheet()
        print(colored("---- Uploading Coating GSheet Yield Data ----"))
        batch_upload_df(
            conn=conn, df=df, tablename="yield.chip_mfg_g", insert_type="refresh"
        )
    except:
        print(colored("---- Skipping Chip GSheet Yield Data ----", "yellow"))

    print("------ Getting Coating GSheet Error Data ------")
    try:
        df = read_chip_error_gsheet()
        print(colored("---- Uploading Coating GSheet Error Data ----"))
        batch_upload_df(
            conn=conn, df=df, tablename="yield.chip_mfg_error_g", insert_type="refresh"
        )
    except:
        print(colored("---- Skipping Chip GSheet Error Data ----", "yellow"))

    print("------ Getting Coating GSheet Ballooning Data ------")
    try:
        df = read_chip_ballooning_gsheet()
        print(colored("---- Uploading Coating GSheet Ballooning Data ----"))
        batch_upload_df(
            conn=conn,
            df=df,
            tablename="yield.chip_mfg_ballooning_g",
            insert_type="refresh",
        )
    except:
        print(colored("---- Skipping Chip GSheet Ballooning Data ----", "yellow"))

    print("------ Getting Coating Yield Data ------")
    try:
        df = get_coat_yield_data(days=days)
        print(colored("---- Uploading Coating Yield Data ----"))
        batch_upload_df(
            conn=conn,
            df=df,
            tablename="test.stg_chip_yield",
            insert_type="refresh",
        )
    except:
        print(colored("---- Skipping Coating Yield Data ----", "yellow"))

    print("------ Getting Coating Error Data ------")
    try:
        df = get_coat_error_data(days=days)
        print(colored("---- Uploading Coating Error Data ----"))
        batch_upload_df(
            conn=conn,
            df=df,
            tablename="test.stg_chip_error",
            insert_type="refresh",
        )
        cur = conn.cursor()
        cur.execute(f"call yield.sp_upload_chip_mfg_data(1)")
        conn.commit()
    except:
        print(colored("---- Skipping Coating Error Data ----", "yellow"))
