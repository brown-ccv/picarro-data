import polars as pl
import argparse
import os

db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")

parser = argparse.ArgumentParser()
parser.add_argument("infile")
args = parser.parse_args()


def read_fixed_width_file_as_strs(file_path, col_names_and_widths, *, skip_rows=0):
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
    for col_name, width in col_names_and_widths.items():
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

    return df


def main(if_table_exists="replace", table_name="test"):
    """
    Reads in a file in fwf and saves it as a PostGreSQL database. User must provide their db password and name as env vars

    args:
        if_table_exists: what to do if the table exists in the sql database. See pandas.to_sql for options
        table_name: name for the output table
    """
    with open(args.infile) as f:
        header = f.readline()

    cols = {
        "DATE": 26,
        "TIME": 26,
        "ALARM_STATUS": 26,
        "CH4": 26,
        "CH4_dry": 26,
        "CO2": 26,
        "CO2_dry": 26,
        "CavityPressure": 26,
        "CavityTemp": 26,
        "DasTemp": 26,
        "EPOCH_TIME": 26,
        "EtalonTemp": 26,
        "FRAC_DAYS_SINCE_JAN1": 26,
        "FRAC_HRS_SINCE_JAN1": 26,
        "H2O": 26,
        "INST_STATUS": 26,
        "JULIAN_DAYS": 26,
        "MPVPosition": 26,
        "OutletValve": 26,
        "WarmBoxTemp": 26,
        "solenoid_valves": 26,
        "species": 26,
    }

    assert header.split() == list(
        cols.keys()
    ), "column mismatch"  # make sure we have the right columns
    assert (
        len(header) - 1 == len(cols) * 26
    ), "column size mismatch"  # make sure the length of the header is what's expected

    df = read_fixed_width_file_as_strs(args.infile, cols, skip_rows=1)

    df.write_database(
        table_name=table_name,
        connection=f"postgresql://postgres:{db_password}@localhost:5432/{db_name}",
        if_table_exists=if_table_exists,
    )


if __name__ == "__main__":
    main()

#  0.015 seconds


# for tomorrow: try to enforce a schema!
