"""Geoflow - Aggregation"""

# TODO group_by
# def group_by(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement


def points_in_polygons(
    node_a: object,
    node_b: object,
    current_node: object,
    spatial_predicate: str,
    join_type: str,
    statistics: list
):
    """
    Method to compute statistics on points inside a set of polygons
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    schema_a = node_a["output_table_schema"]
    table_b = node_b["output_table_name"]
    schema_b = node_b["output_table_schema"]

    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
    SELECT """

    for statistic in statistics:
        statement += f"""{statistic['type']}({statistic['column']}),"""

    statement += f""" polygons.*
    FROM "{schema_a}"."{table_a}" polygons
    {join_type} "{schema_b}"."{table_b}" points
    ON {spatial_predicate}(polygons.geom, points.geom)
    GROUP BY polygons.gid;
    """
    return statement


def dump_geometry(
    node: object,
    current_node: object,
    geometry_type: str
):
    """
    Method to split a single multi geometry into multiple single geometries.
    """
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    schema = node["output_table_schema"]

    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}"
        geom GEOMETRY({geometry_type}, 4326)
    );

    WITH geometry_collection AS (
        SELECT geom
        FROM "{schema}"."{table}"
    )
    INSERT INTO "geoflow"."{new_table_name}" (geom)
    SELECT (ST_Dump(geom)).geom 
    FROM geometry_collection;
    """
    return statement
