"""Concatenates .dat files from a directory.

This module provides functionality to read and concatenate multiple .dat files
from a directory into a single polars DataFrame.

Typical usage:
    df = concat_dat.concat_dat_files("/path/to/dat/files")
"""
from pathlib import Path
import polars as pl
import logging
import convert_dat

logger = logging.getLogger("picarro")


def concat_dat_files(directory: str) -> pl.DataFrame:
    """Concatenates .dat files from a directory into a single DataFrame.

    Args:
        directory: directory where .dat files are stored

    Returns:
        A polars DataFrame containing the concatenated data from all .dat files
    
    Raises:
        ValueError: If no valid data files are found to concatenate
    """
    logger.info("Reading and concatenating .dat files")
    filenames = Path(directory).iterdir()

    # read all files
    dfs = []
    for filename in filenames:
        if not filename.match("backup_copy"):
            dfs.append(convert_dat.convert(filename))

    # concatenate all dataframes
    try:
        df = pl.concat(dfs)
    except ValueError:
        logger.error("cannot concatenate empty dataframes")
        raise

    return df