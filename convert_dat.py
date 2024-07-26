import polars as pl
import pandera.polars as pa
import numpy as np


def read_fixed_width_file(file_path, col_names_and_widths, *, skip_rows=0, width):
    """
    Reads a fixed-width file into a dataframe.
    Reads all values as strings (as indicated by function name).
    Strips all values of leading/trailing whitespaces.

    Args:
            col_names_and_widths: A dictionary where the keys are the column names and the values are the widths of the columns.
    """

    # Source: adapted from https://github.com/pola-rs/polars/issues/3151#issuecomment-1397354684

    df = pl.read_csv(
        file_path,
        has_header=False,
        skip_rows=skip_rows,
        new_columns=["full_str"],
    )

    # transform col_names_and_widths into a Dict[cols name, Tuple[start, width]]
    slices = {}
    start = 0
    for col_name in col_names_and_widths.keys():
        slices[col_name] = (start, width)
        start += width

    df = df.with_columns(
        [
            pl.col("full_str")
            .str.slice(slice_tuple[0], slice_tuple[1])
            .str.strip_chars()
            .alias(col)
            for col, slice_tuple in slices.items()
        ]
    ).drop(["full_str"])

    df = df.with_columns(datetime=pl.concat_str('DATE', 'TIME', separator='T').str.strptime(pl.Datetime))

    return df


def convert(infile, width=26):
    """
    Reads in a file in fwf and saves it as a PostGreSQL database. User must provide their db password and name as env vars

    args:
        if_table_exists: what to do if the table exists in the sql database. See pandas.to_sql for options
        table_name: name for the output table
    """
    with open(infile) as f:
        header = f.readline()

    # TODO: Do I need to make these all required or does strict do that?
    cols = {
        "DATE": pa.Column(str),
        "TIME": pa.Column(str),
        "ALARM_STATUS": pa.Column(float),
        "CH4": pa.Column(float),
        "CH4_dry": pa.Column(float),
        "CO2": pa.Column(float),
        "CO2_dry": pa.Column(float),
        "CavityPressure": pa.Column(float),
        "CavityTemp": pa.Column(float),
        "DasTemp": pa.Column(float),
        "EPOCH_TIME": pa.Column(float),
        "EtalonTemp": pa.Column(float),
        "FRAC_DAYS_SINCE_JAN1": pa.Column(float),
        "FRAC_HRS_SINCE_JAN1": pa.Column(float),
        "H2O": pa.Column(float),
        "INST_STATUS": pa.Column(float), # bool?
        "JULIAN_DAYS": pa.Column(float),
        "MPVPosition": pa.Column(float), # ???
        "OutletValve": pa.Column(float),
        "WarmBoxTemp": pa.Column(float),
        "solenoid_valves": pa.Column(float),
        "species": pa.Column(float),
        "datetime": pa.Column(pl.Datetime)
    }
    schema = pa.DataFrameSchema(cols, coerce=True, strict=False)

    df = read_fixed_width_file(infile, cols, skip_rows=1, width=width)

    try:
        schema.validate(df, inplace=True)
    except pa.errors.SchemaError as exc:
        print(exc)

    return df


if __name__ == "__main__":
    convert("/Users/sbessey/Documents/picarro/data/Data_Backup/2023DecToCurrent/UserData/DataLog_User/2023/11/03/CFADS2652-20231103-162000Z-DataLog_User.dat")

# 0.015 seconds
# TODO: how to check that bools/ints are bools/ints if they're 0-padded
