
def save_as_table(
    node: object,
    current_node: object
):
    """
    Method to create a new table
    """
    new_table_name = current_node["output_table_name"]
    scheme = node["output_table_scheme"]
    table = node["output_table_name"]
    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS
    SELECT *
    FROM "{scheme}"."{table}";
    """
    return statement