from geoflow import utilities

def spatial_join(
    cur,
    conn,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table that joins polygon data to point data.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT {a_fields} {b_fields} a.geom
        FROM "{table_a}" AS a, "{table_b}" AS b
        WHERE ST_Intersects(a.geom, b.geom);
    """
    return statement

def intersects(
    cur,
    conn,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table find any data within a set of polygons.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        new_table_name="a"
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        new_table_name="b"
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT {a_fields} {b_fields} b.geom
        FROM "{table_a}" AS a, "{table_b}" AS b
        WHERE ST_INTERSECTS(a.geom, b.geom);   
    """

    return statement

# TODO join
def join(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO difference
def difference(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO union
def union(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement
