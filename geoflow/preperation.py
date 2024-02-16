from geoflow import utilities

def where_filter(
    cur,
    conn,
    node: object,
    current_node: object,
    sql_filter: str=None
):
    """
    This method will create a table that is the output of filtering another table.
    """
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    fields = utilities.get_table_columns(
        cur=cur,
        table=table
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT {fields} "{table}".geom
        FROM "{table}" 
    """
    if sql_filter:
        statement += f"""WHERE {sql_filter}"""
    statement += ";"
    return statement

# TODO cast
def cast(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
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
    statement = f"""
    ALTER TABLE {table} 
    ADD COLUMN IF NOT EXISTS '{column_name}' {column_type};

    UPDATE '{table}'
    SET '{column_name}' = {expression};
    """
    return statement

# TODO drop_column
def drop_column(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO find_and_replace
def find_and_replace(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

def normalize(
    cur,
    conn,
    node: object,
    current_node: object,
    column: str,
    decimals: int=2
):
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]

    fields = utilities.get_table_columns(
        cur=cur,
        table=table
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
    CREATE TABLE {new_table_name} AS
    SELECT ROUND(ROUND(({column} - min_value),{decimals}) / ROUND((max_value - min_value),{decimals}),{decimals}) AS normalized, {fields} "{table}".geom
    FROM {table}, (SELECT MIN({column}) AS min_value, MAX({column}) AS max_value FROM {table}) AS min_max;
    """
    return statement

# TODO rename_column
def rename_column(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO select_distinct
def select_distinct(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement
