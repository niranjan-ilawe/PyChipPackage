from datetime import date, timedelta

from pybox import box_create_df_from_files, get_box_client

from pychip.coat_mfg.file_reading_scripts import (
    read_chip_yield_rev_F,
    read_chip_error_rev_F,
    read_chip_yield_revEFG,
)


def get_coat_yield_data(days=3):

    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    # Get Chip Yield Data from production folder
    df1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="122824311729",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=read_chip_yield_rev_F,
    )

    df1 = df1.drop_duplicates(subset=["wo", "day", "round"])

    # Get Chip Yield Data from WIP folder
    df2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="122823632848",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=read_chip_yield_rev_F,
    )

    df2 = df2.drop_duplicates(subset=["wo", "day", "round"])

    df = df1.append(df2)

    return df


def get_coat_error_data(days=3):

    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    # Get Chip Error Data from production folder
    df1 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="122824311729",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=read_chip_error_rev_F,
    )

    df1 = df1.dropna()

    # Get Chip Error Data from WIP folder
    df2 = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="122823632848",
        file_extension="xlsx",
        file_pattern="",
        file_parsing_functions=read_chip_error_rev_F,
    )

    df2 = df2.dropna()

    df = df1.append(df2)

    return df


def get_chip_mfg_data(days=3):
    last_modified_date = str(date.today() - timedelta(days=days))
    print(f"Looking for new data since {last_modified_date} ....")

    client = get_box_client()

    # Get Chip Error Data from production folder
    df = box_create_df_from_files(
        box_client=client,
        last_modified_date=last_modified_date,
        box_folder_id="159830870681",
        file_extension="xlsx",
        file_pattern="Coated",
        file_parsing_functions=read_chip_yield_revEFG,
    )

    df = df.drop_duplicates()
