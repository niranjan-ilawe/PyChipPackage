pychip
--------
``v0.0.9000``

Package contains all the relevant scripts to pull manufacturing and QC data from all the Chip Manufacturing process areas. 
The flagship functions of this package are the ``run_chip_pipeline`` scripts that will read the files from Box or relevant locations, transform them into dataframes,
and push the dataframes to the CPPDA postgres database.

The scripts are structured in the following way

pygbmfg
    * coat_mfg
        - file_reading_scripts
        - df_creation_scripts

and so on for other areas.

The flagship function is run as follows

    >>> from pychip.pipeline import run_chip_pipeline
    >>> run_chip_pipeline(days=3)

This defaults to pulling the last 3 days worth of data. For pulling older data

    >>> run_chip_pipeline(days=90)

pulls the last 90 days worth of data.

You can also access individual manufacturing or QC data as follows

    >>> from pygbmfg.coat_mfg.df_creation_scripts import get_coat_yield_data
    >>> from pydb import get_postgres_connection, batch_upload_df
    >>> conn = get_postgres_connection(service_name="cpdda-postgres", username="cpdda", db_name="cpdda")
    >>> df = get_coat_yield_data(days=3)
    >>> batch_upload_df(
            conn=conn,
            df=df,
            tablename="test.stg_chip_yield",
            insert_type="refresh"
        )

As above you can access any of the individual functions and run them independently or run the entire pipeline with the flagship function.

Notes::
~~~~~~~~~~~~~
The package assumes that you have access to the Box files the package references. And that you have installed the pybox and pydb packages.
It also assumes that credentials to access the databases are stored using the keyring package since keyring is used to retrieve these from 
your local env.