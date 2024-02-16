from unittest.mock import Mock, patch
from geoflow.spatial_operations import buffer

@patch('geoflow.utilities.get_table_columns')
def test_buffer(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node = {"output_table_name": "point_table"}
    distance_in_meters = 100
    current_node = {"output_table_name": "output_table"}
    statement = buffer(cur, node, distance_in_meters, current_node)
    expected_statement = """
        CREATE TABLE output_table AS 
        SELECT point_table.gid,
        ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 100), 4326) as geom
        FROM point_table;
    """
    assert statement.strip() == expected_statement.strip()