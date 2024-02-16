from unittest.mock import Mock, patch
from geoflow.join import intersects

@patch('geoflow.utilities.get_table_columns')
def test_intersects(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "node_a_table"}
    node_b = {"output_table_name": "node_b_table"}
    current_node = {"output_table_name": "current_node_table"}
    statement = intersects(cur, node_a, node_b, current_node)
    expected_statement = """
        CREATE TABLE current_node_table AS 
        SELECT node_a_table.gid, node_b_table.gid, b.geom
        FROM node_a_table AS a, node_b_table AS b
        WHERE ST_INTERSECTS(a.geom, b.geom);
    """
    assert statement.strip() == expected_statement.strip()