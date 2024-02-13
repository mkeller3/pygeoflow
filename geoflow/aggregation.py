from geoflow import utilities

def count_number_of_rows_in_table(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    new_table_name = current_node["output_table_name"]
    table = node_a["output_table_name"]
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT COUNT(*)
        FROM "{table}";
    """
    return statement

# TODO group_by
def group_by(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO count_points_in_polygons
def count_points_in_polygons(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO geometry_extraction
def geometry_extraction(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    # ST_DUMP
    statement = "test"
    return statement