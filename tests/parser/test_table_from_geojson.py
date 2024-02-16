from geoflow.parser import table_from_geojson

def test_table_from_geojson():
    geojson = '{"type": "Point", "coordinates": [125.6, 10.1]}'
    current_node = {"output_table_name": "test_table"}
    statement = table_from_geojson(geojson, current_node)
    expected_statement = """
        CREATE TABLE test_table AS 
        SELECT ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [125.6, 10.1]}') as geom
    """
    assert statement.strip() == expected_statement.strip()