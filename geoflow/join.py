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

def join(
    cur,
    conn,
    node_a: object,
    node_b: object,
    current_node: object,
    join_type: str
):
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    column_a = node_a["join_column"]
    column_b = node_b["join_column"]

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
        SELECT {a_fields} {b_fields}
        FROM "{table_a}" AS a
        {join_type} "{table_b}" AS b
        ON a.{column_a} = b.{column_b};   
    """
    return statement

def difference(
    node_a: object,
    node_b: object,
    current_node: object
):
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT ST_Difference(a.geom,b.geom) as geom
        FROM "{table_a}" AS a, "{table_b}" AS b   
    """
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
