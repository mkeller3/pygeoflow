"""Geoflow - Preperation"""

from pygeoflow import utilities

def where_filter(
    cur,
    node: object,
    current_node: object,
    sql_filter: str=None
):
    """
    This method will create a table that is the output of filtering another table.
    """
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    fields = utilities.get_table_columns(
        cur=cur,
        table=table,
        schema=schema
    )

    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
    SELECT {fields}, "{table}".geom
    FROM "{schema}"."{table}" 
    """
    if sql_filter:
        statement += f"""WHERE {sql_filter}"""
    statement += ";"
    return statement

def create_column(
    node: object,
    column_name: str,
    column_type: str,
    expression: str
):
    """
    Method to create a new column in a table based off of an expression.
    """
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    statement = f"""
    ALTER TABLE "{schema}"."{table}" 
    ADD COLUMN IF NOT EXISTS "{column_name}" {column_type};

    UPDATE "{schema}"."{table}"
    SET "{column_name}" = {expression};
    """
    return statement

def drop_column(
    node: object,
    column_name: str
):
    """
    Method to drop a column in a table.
    """
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    statement = f"""
    ALTER TABLE "{schema}"."{table}" 
    DROP COLUMN IF EXISTS "{column_name}";
    """
    return statement

def find_and_replace(
    node: object,
    column_name: str,
    old_value,
    new_value
):
    """
    Method to find and replace values in a column
    """
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    statement = f"""
    UPDATE "{schema}"."{table}"
    SET "{column_name}" = REPLACE('{column_name}', {old_value}, {new_value});
    """
    return statement

def normalize(
    cur,
    node: object,
    current_node: object,
    column: str,
    decimals: int=2
):
    """
    Method to create a new column that will normalize a numerical column.
    """
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    schema = node["output_table_schema"]

    fields = utilities.get_table_columns(
        cur=cur,
        table=table,
        schema=schema
    )

    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS
    SELECT ROUND(ROUND(({column} - min_value),{decimals}) / ROUND((max_value - min_value),{decimals}),{decimals}) AS normalized, {fields}, "{schema}"."{table}".geom
    FROM "{schema}"."{table}", (SELECT MIN({column}) AS min_value, MAX({column}) AS max_value FROM "{schema}"."{table}") AS min_max;
    """
    return statement

def rename_column(
    node: object,
    column_name: str,
    new_column_name: str
):
    """
    Method to rename a column
    """
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    statement = f"""
    ALTER TABLE "{schema}"."{table}"
    RENAME COLUMN "{column_name}" TO "{new_column_name}";
    """
    return statement
