from geoflow import utilities

def where_filter(
    cur,
    conn,
    node: object,
    current_node: object,
    sql_filter: str=None
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

# TODO create_column
def create_column(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
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

# TODO normalize
def normalize(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO normalize
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
