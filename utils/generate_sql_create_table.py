"""
Generate the SQL CREATE TABLE based on the pl.DataFrame structure
"""
import polars as pl


def generate_sql_create_table(df: pl.DataFrame)->str:
    """
    Generates a SQL CREATE TABLE based on the pl.DataFrame structure. Will have to edit but hey at least you've got all
    columns names.
    :param df: pl.DataFrame
    :return: Nothing, just prints.
    """

    df_schema = df.schema
    base_string = """CREATE TABLE table_name \n"""

    type_mapping = {
        pl.Int64: "BIGINT",
        pl.Int32: "INTEGER",
        pl.Int8: "TINYINT",
        pl.Float64: "DOUBLE PRECISION",
        pl.Float32: "FLOAT",
        pl.String: "TEXT",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "DATETIME",
    }

    for column_name, type in df_schema.items():
        print(column_name, type)
        base_string += f"""{column_name} ({type}),\n"""
    print(base_string)

