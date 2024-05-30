from pygeoflow import aggregation

def test_dump_geometry():
    # Define the mock inputs
    node = {
        "output_table_name": "input_table",
        "output_table_schema": "public",
        "geometry_type": "Point"
    }
    current_node = {
        "output_table_name": "output_table"
    }

    # Expected SQL statement
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."output_table"
        geom GEOMETRY(Point, 4326)
    );

    WITH geometry_collection AS (
        SELECT geom
        FROM "public"."input_table"
    )
    INSERT INTO "geoflow"."output_table" (geom)
    SELECT (ST_Dump(geom)).geom 
    FROM geometry_collection;
    """

    # Call the function
    actual_statement = aggregation.dump_geometry(node, current_node,node["geometry_type"])

    # Assert the expected output
    assert expected_statement.strip() == actual_statement.strip()
