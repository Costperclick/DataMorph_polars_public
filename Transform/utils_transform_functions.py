"""
A tookit containing utils functions for transformations
"""
import polars as pl


def timestamp_dataframe(df:pl.DataFrame, date_str:str) -> pl.DataFrame:
    """
    Add a 'date' column to the dataframe as Date (%d-%m-%Y format)
    :param df: the pl.DataFrame to timestamp.
    :param date_str: a string written matching the "%d-%m-%Y" format.
    :return: a timestamp dataframe.
    """
    try:
        df = df.with_columns(
            date = pl.lit(date_str).str.to_date(format='%d-%m-%Y')
        )
        return df
    except Exception as e:
        print(f"ERROR during timestamp_dataframe: {e}")
        raise

