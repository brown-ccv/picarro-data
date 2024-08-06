import polars as pl
from typing import List


def read_fixed_width_file(file_path: str, col_names: List[str], *, skip_rows: int=0, width: int) -> pl.DataFrame:
    """
    Reads a fixed-width file into a dataframe.
    Reads all values as strings (as indicated by function name).
    Strips all values of leading/trailing whitespaces.

    Args:
            file_path: Path to the fwf to be read
            col_names: Names of the df columns
            skip_rows: Number of rows to skip at file start
            width: Length of the fixed-width
    """

    # Source: adapted from https://github.com/pola-rs/polars/issues/3151#issuecomment-1397354684

    df = pl.read_csv(
        file_path,
        has_header=False,
        skip_rows=skip_rows,
        new_columns=["full_str"],
    )

    slices = {}
    start = 0
    for col_name in col_names:
        slices[col_name] = (start, width)
        start += width

    df = df.with_columns(
        [
            pl.col("full_str")
            .str.slice(slice_tuple[0], slice_tuple[1])
            .str.strip_chars()
            .alias(col)
            .str.replace(".000000", "")
            for col, slice_tuple in slices.items()
        ]
    ).drop(["full_str"])

    df = df.with_columns(
        datetime=pl.concat_str("DATE", "TIME", separator="T").str.strptime(pl.Datetime),
        ALARM_STATUS=pl.col("ALARM_STATUS").cast(int) == 1,
    )

    return df


def convert(infile: str, width: int=26) -> pl.DataFrame:
    """
    Reads in a file in fwf and returns a polars dataframe

    args:
        if_table_exists: what to do if the table exists in the sql database. See pandas.to_sql for options
        table_name: name for the output table
    """
    with open(infile) as f:
        header = f.readline().split()

    return read_fixed_width_file(infile, header, skip_rows=1, width=width)
