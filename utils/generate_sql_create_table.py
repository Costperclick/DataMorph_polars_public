"""
Generate the SQL CREATE TABLE based on the pl.DataFrame structure
"""
import polars as pl


def generate_sql_create_table(df: pl.DataFrame)->str:

    df_schema = df.schema
    base_string = """CREATE TABLE"""

    for column_name, type in df_schema.items():
        print(column_name, type)
        base_string += f"""{column_name} {type},"""
    return base_string


data = [
    {
        "shop_id": 14945,
        "shop_label": "YAMAHA - STARTER MOTOS",
        "franchise_label": "Yamaha Motor",
        "shop_status": "actif",
        "completion_rate": 100.0,
        "has_display_name": 1,
        "has_google_description": 1,
        "has_facebook_description": 1,
        "has_services": 1,
        "has_zip_code": 1,
        "has_logo": 1
    },
    {
        "shop_id": 15008,
        "shop_label": "YAMAHA - PARADISE",
        "franchise_label": "Yamaha Motor",
        "shop_status": "résilié",
        "completion_rate": 87.5,
        "has_display_name": 1,
        "has_google_description": 0,
        "has_facebook_description": 0,
        "has_services": 1,
        "has_zip_code": 0,
        "has_logo": 1
    }
]

df = pl.DataFrame(data)
foo = generate_sql_create_table(df)
print(foo)

