from geoflow.spatial_operations import boundary

def test_boundary():
    node = {"output_table_name": "polygon_table"}
    current_node = {"output_table_name": "output_table"}
    statement = boundary(node, current_node)
    expected_statement = """
    CREATE TABLE output_table AS 
    SELECT ST_Envelope(ST_Extent(geom)) as geom
    FROM polygon_table;
    """
    assert statement.strip() == expected_statement.strip()