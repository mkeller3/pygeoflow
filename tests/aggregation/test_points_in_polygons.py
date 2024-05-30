from pygeoflow import aggregation

def test_points_in_polygons():
    node_a = {
        "output_table_name": "polygons_table",
        "output_table_schema": "public"
    }
    node_b = {
        "output_table_name": "points_table",
        "output_table_schema": "public"
    }
    current_node = {
        "output_table_name": "output_table"
    }
    spatial_predicate = "ST_Contains"
    join_type = "LEFT JOIN"
    statistics = [
        {"type": "COUNT", "column": "points.id"},
        {"type": "SUM", "column": "points.value"}
    ]
        
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."output_table" AS 
    SELECT COUNT(points.id),SUM(points.value), polygons.*
    FROM "public"."polygons_table" polygons
    LEFT JOIN "public"."points_table" points
    ON ST_Contains(polygons.geom, points.geom)
    GROUP BY polygons.gid;
    """
        
    result = aggregation.points_in_polygons(node_a, node_b, current_node, spatial_predicate, join_type, statistics)

    assert result == expected_statement