from pygeoflow.spatial_operations import boundary

def test_boundary():
    node = {"output_table_name": "polygon_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "output_table","output_table_schema": "geoflow"}
    statement = boundary(node, current_node)
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."output_table" AS 
    SELECT ST_Envelope(ST_Extent(geom)) as geom
    FROM "geoflow"."polygon_table";
    """
    assert statement.strip() == expected_statement.strip()