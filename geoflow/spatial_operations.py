from geoflow import utilities

def get_closest_point_to_polygons(
    cur,
    conn,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table that find the closest point to each polygon.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        new_table_name="points"
    )
    b_fields = utilities.get_table_columns(
        cur=cur,
        table=table_b,
        new_table_name="polygons"
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT polygons.gid, {a_fields} {b_fields} points.dist * 62.1371192 as distance_in_miles, polygons.geom
        FROM {table_b} polygons
        CROSS JOIN LATERAL (
            SELECT {a_fields} polygons.geom <-> points.geom AS dist, points.geom
            FROM {table_a} AS points
            ORDER BY dist
            LIMIT 1
        ) points
    """
    return statement

def clip(
    cur,
    conn,
    node_a: object,
    node_b: object,
    current_node: object
):
    """
    This method will create a table that clips a dataset to within a polygon dataset.
    """
    new_table_name = current_node["output_table_name"]
    table_a = node_a["output_table_name"]
    table_b = node_b["output_table_name"]
    a_fields = utilities.get_table_columns(
        cur=cur,
        table=table_a,
        new_table_name="a"
    )
    utilities.drop_table(
        cur=cur,
        conn=conn,
        node=current_node
    )
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}_within" AS 
        SELECT {a_fields} a.geom
        FROM "{table_a}" AS a, "{table_b}" AS b
        WHERE ST_Within(a.geom, b.geom);

        CREATE TABLE IF NOT EXISTS "{new_table_name}_intersects" AS 
        SELECT {a_fields} ST_Intersection(a.geom, b.geom) as geom
        FROM "{table_a}" AS a, "{table_b}" AS b
        WHERE ST_Intersects(a.geom, ST_Boundary(b.geom));

        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT * 
        FROM "{new_table_name}_within"
        UNION
        SELECT * 
        FROM "{new_table_name}_intersects";

        DROP TABLE IF EXISTS "{new_table_name}_within";
        DROP TABLE IF EXISTS "{new_table_name}_intersects";
    """

    return statement

def centroid(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    """
    This method will create a table that finds the centroid of each polygon.
    """
    new_table_name = current_node["output_table_name"]
    table = node_a["output_table_name"]
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
        SELECT {fields} ST_Centroid(geom) as geom
        FROM "{table}";
    """
    return statement

def buffer(
    cur,
    conn,
    node: object,
    distance_in_meters: float,
    current_node: object
):
    """
    This method will create a table that buffers points by a set distance.
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
        SELECT {fields}
        ST_Transform(ST_Buffer(ST_Transform(geom, 3857), {distance_in_meters}), 4326) as geom
        FROM "{table}";
    """
    return statement

# TODO bbox
def bbox(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO generate_points
def generate_points(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO distance
def distance(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO concave_hull
def concave_hull(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO convex_hull
def convex_hull(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO simplify
def simplify(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO dissolve
def dissolve(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO intersect
def intersect(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement

# TODO erase
def erase(
    cur,
    conn,
    node_a: object,
    current_node: object
):
    statement = "test"
    return statement
