import polars as pl
from typing import List


def read_fixed_width_file(
    file_path: str, col_names: List[str], *, skip_rows: int = 0, width: int
) -> pl.DataFrame:
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
            for col, slice_tuple in slices.items()
        ]
    ).drop(["full_str"])

    return df


def convert(infile: str, width: int = 26) -> pl.DataFrame:
    """
    Reads in a file in fwf and returns a polars dataframe

    args:
        if_table_exists: what to do if the table exists in the sql database. See pandas.to_sql for options
        table_name: name for the output table
    """
    with open(infile) as f:
        header = f.readline().split()

    return read_fixed_width_file(infile, header, skip_rows=1, width=width)


def aggregate_df(data, threshold=0.5):
    """
    Returns a dataframe aggregated from every second to every minute
    args:
        data: the dataframe to aggregate
        threshold: percent of full data to require for hour to be aggregated
    """

    # add hour and filter to only good data (no alarm status, not warming up)
    data = (
        data.with_columns(hour=pl.col("TIME").str.split(":").list.head(1).explode())
        .cast(
            {
                pl.selectors.by_name(
                    "CH4",
                    "CH4_dry",
                    "CO2",
                    "CO2_dry",
                    "CavityPressure",
                    "CavityTemp",
                    "DasTemp",
                    "EtalonTemp",
                    "H2O",
                    "INST_STATUS",
                    "WarmBoxTemp",
                    "solenoid_valves",
                    "species",
                    "ALARM_STATUS",
                ): pl.Float32
            }
        )
        .filter(
            (pl.col("CH4") != 0)
            & (pl.col("CH4_dry") != 0)
            & (pl.col("CO2") != 0)
            & (pl.col("CO2_dry") != 0)
            & (pl.col("CavityPressure") != 0)
            & (pl.col("CavityTemp") != 0)
            & (pl.col("DasTemp") != 0)
            & (pl.col("EtalonTemp") != 0)
            & (pl.col("H2O") != 0)
            & (pl.col("INST_STATUS") != 0)
            & (pl.col("WarmBoxTemp") != 0)
            & (pl.col("solenoid_valves") == 0)
            & (pl.col("species") != 0.0)
            & (pl.col("ALARM_STATUS") == 0)
        )
    )

    # aggregate data by hour. Note: percent timepoints may be over 1
    return data.group_by("DATE", "hour", "ALARM_STATUS", "INST_STATUS").agg(
        pl.len() / 3600,  # percent of timepoints printed
        pl.col("CH4").cast(float).mean(),
        pl.col("CH4_dry").cast(float).mean(),
        pl.col("CO2").cast(float).mean(),
        pl.col("CO2_dry").cast(float).mean(),
        pl.col("CavityPressure").cast(float).mean(),
        pl.col("CavityTemp").cast(float).mean(),
        pl.col("DasTemp").cast(float).mean(),
        pl.col("EtalonTemp").cast(float).mean(),
        pl.col("H2O").cast(float).mean(),
    )
