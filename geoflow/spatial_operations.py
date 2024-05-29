"""Geoflow - Spatial Operations"""

from geoflow import utilities

def get_closest_point_to_polygons(
    cur,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table that find the closest point to each polygon.
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
        new_table_name="points"
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        schema=schema_b,
        new_table_name="polygons"
    )

    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT polygons.gid, {a_fields}, {b_fields}, points.dist * 62.1371192 as distance_in_miles, polygons.geom
        FROM "{schema_b}"."{table_b}" polygons
        CROSS JOIN LATERAL (
            SELECT {a_fields} polygons.geom <-> points.geom AS dist, points.geom
            FROM "{schema_a}"."{table_a}" AS points
            ORDER BY dist
            LIMIT 1
        ) points;
    """
    return statement

def clip(
    cur,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table that clips a dataset to within a polygon dataset.
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

    statement = f"""
        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}_within" AS 
        SELECT {a_fields}, a.geom
        FROM "{schema_a}"."{table_a}" AS a, "{schema_b}"."{table_b}" AS b
        WHERE ST_Within(a.geom, b.geom);

        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}_intersects" AS 
        SELECT {a_fields}, ST_Intersection(a.geom, b.geom) as geom
        FROM "{schema_a}"."{table_a}" AS a, "{schema_b}"."{table_b}" AS b
        WHERE ST_Intersects(a.geom, ST_Boundary(b.geom));

        CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
        SELECT * 
        FROM "geoflow"."{new_table_name}_within"
        UNION
        SELECT * 
        FROM "geoflow"."{new_table_name}_intersects";

        DROP TABLE IF EXISTS "geoflow"."{new_table_name}_within";
        DROP TABLE IF EXISTS "geoflow"."{new_table_name}_intersects";
    """

    return statement

def centroid(
    cur,
    node: object,
    current_node: object
):
    """
    Method to create a table that finds the centroid of each polygon.
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
        SELECT {fields}, ST_Centroid(geom) as geom
        FROM "{schema}"."{table}";
    """
    return statement

def buffer(
    cur,
    node: object,
    distance_in_meters: float,
    current_node: object
):
    """
    This method will create a table that buffers points by a set distance.
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
        SELECT {fields},
        ST_Transform(ST_Buffer(ST_Transform(geom, 3857), {distance_in_meters}), 4326) as geom
        FROM "{schema}"."{table}";
    """
    return statement

def boundary(
    node: object,
    current_node: object
):
    """
    Method that is used to create a new table that contains the bounding box of the entire table.
    """
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    schema = node["output_table_schema"]
    statement = f"""
    CREATE TABLE IF NOT EXISTS "geoflow"."{new_table_name}" AS 
    SELECT ST_Envelope(ST_Extent(geom)) as geom
    FROM "{schema}"."{table}";
    """
    return statement

def bounding_box(
    cur,
    node: object,
    current_node: object
):
    """
    Method that is used to create a new table that contains the bounding box of each row.
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
    SELECT {fields}, ST_Envelope(geom) as geom
    FROM "{schema}"."{table}";
    """
    return statement

def generate_points(
    node: object,
    current_node: object,
    number_of_points: int
):
    """
    Method to generate random points within a set of polygons
    """
    
    new_table_name = current_node["output_table_name"]
    table = node["output_table_name"]
    schema = node["output_table_schema"]

    statement = f"""
    CREATE TABLE "geoflow"."{new_table_name}" 
        geom GEOMETRY(Point, 4326)
    );

    WITH geometry_collection AS (
        SELECT ST_GeneratePoints(geom, {number_of_points}) AS geom
        FROM "{schema}"."{table}"
    )
    INSERT INTO "geoflow"."{new_table_name}" (geom)
    SELECT (ST_Dump(geom)).geom 
    FROM geometry_collection;
    """

    return statement

# TODO distance
# def distance(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO concave_hull
# def concave_hull(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO convex_hull
# def convex_hull(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO simplify
# def simplify(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO dissolve
# def dissolve(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO intersect
# def intersect(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO erase
# def erase(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement

# TODO disjoint
# def disjoint(
#     cur,
#     conn,
#     node_a: object,
#     current_node: object
# ):
#     statement = "test"
#     return statement
