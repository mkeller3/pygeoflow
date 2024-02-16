from unittest.mock import Mock, patch
from geoflow.spatial_operations import centroid

@patch('geoflow.utilities.get_table_columns')
def test_centroid(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "polygon_table"}
    current_node = {"output_table_name": "output_table"}
    statement = centroid(cur, node_a, current_node)
    expected_statement = """
        CREATE TABLE output_table AS 
        SELECT polygon_table.gid, ST_Centroid(geom) as geom
        FROM polygon_table;
    """
    assert statement.strip() == expected_statement.strip()