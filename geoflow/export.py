"""Geoflow - Export"""

def save_as_table(
    node: object,
    current_node: object
):
    """
    Method to create a new table
    """
    new_table_name = current_node["output_table_name"]
    schema = node["output_table_schema"]
    table = node["output_table_name"]
    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS
    SELECT *
    FROM "{schema}"."{table}";
    """
    return statement
