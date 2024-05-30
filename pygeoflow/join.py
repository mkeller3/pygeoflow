"""Geoflow - Join"""

from pygeoflow import utilities

def spatial_join(
    cur,
    node_a: object,
    node_b: object,
    current_node: object,
    spatial_predicate: str
):
    """
    This method will create a table that joins polygon data to point data.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    schema_a = node_a["output_table_schema"]
    table_b = node_b["output_table_name"]
    schema_b = node_b["output_table_schema"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        schema=schema_a
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        schema=schema_b
    )

    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT {a_fields}, {b_fields}, a.geom
        FROM "{schema_a}"."{table_a}" AS a, "{schema_b}"."{table_b}" AS b
        WHERE {spatial_predicate}(a.geom, b.geom);
    """
    return statement

def intersects(
    cur,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table find any data within a set of polygons.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    schema_a = node_a["output_table_schema"]
    table_b = node_b["output_table_name"]
    schema_b = node_b["output_table_schema"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        schema=schema_a,
        new_table_name="a"
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        schema=schema_b,
        new_table_name="b"
    )

    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT {a_fields}, {b_fields}, b.geom
        FROM "{schema_a}"."{table_a}" AS a, "{schema_b}"."{table_b}" AS b
        WHERE ST_INTERSECTS(a.geom, b.geom);   
    """

    return statement

def join(
    cur,
    node_a: object,
    node_b: object,
    current_node: object,
    join_type: str
):
    """
    Method to join two tables together based off
    of a matching column.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    schema_a = node_a["output_table_schema"]
    table_b = node_b["output_table_name"]
    schema_b = node_b["output_table_schema"]
    column_a = node_a["join_column"]
    column_b = node_b["join_column"]

    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        schema=schema_a,
        new_table_name="a"
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        schema=schema_b,
        new_table_name="b"
    )

    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT {a_fields}, {b_fields}
        FROM "{schema_a}"."{table_a}" AS a
        {join_type} "{schema_b}"."{table_b}" AS b
        ON a.{column_a} = b.{column_b};   
    """
    return statement

def difference(
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    Method to find the difference in geometry
    of two tables.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    schema_a = node_a["output_table_schema"]
    table_b = node_b["output_table_name"]
    schema_b = node_b["output_table_schema"]
    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT ST_Difference(a.geom,b.geom) as geom
        FROM "{schema_a}"."{table_a}" AS a, "{schema_b}"."{table_b}" AS b;
    """
    return statement

def union(
    current_node: object,
    tables: list
):
    """
    Method to join multiple tables together with matching
    columns
    """
    new_table_name = current_node["output_table_name"]
    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS
    """
    for count, table in enumerate(tables):
        statement += f"""SELECT * FROM {table}"""
        if (count + 1) < len(tables):
            statement += " UNION ALL "
    statement += ";"
    return statement
