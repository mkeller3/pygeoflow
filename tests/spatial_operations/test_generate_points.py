from pygeoflow.spatial_operations import generate_points

def test_generate_points():
    # Define the mock inputs
    node = {
        "output_table_name": "input_table",
        "output_table_schema": "public"
    }
    current_node = {
        "output_table_name": "output_table"
    }
    number_of_points = 10
    
    # Expected SQL statement
    expected_statement = """
    CREATE TABLE "geoflow"."output_table" 
        geom GEOMETRY(Point, 4326)
    );

    WITH geometry_collection AS (
        SELECT ST_GeneratePoints(geom, 10) AS geom
        FROM "public"."input_table"
    )
    INSERT INTO "geoflow"."output_table" (geom)
    SELECT (ST_Dump(geom)).geom 
    FROM geometry_collection;
    """
        
    # Call the function
    actual_statement = generate_points(node, current_node, number_of_points)
    
    # Assert the expected output
    assert expected_statement.strip() == actual_statement.strip()