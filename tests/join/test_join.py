from unittest.mock import Mock, patch
from geoflow.join import join

@patch('geoflow.utilities.get_table_columns')
def test_join(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "node_a_table", "join_column": "a_id"}
    node_b = {"output_table_name": "node_b_table", "join_column": "b_id"}
    current_node = {"output_table_name": "current_node_table"}
    join_type = "INNER JOIN"
    statement = join(cur, node_a, node_b, current_node, join_type)
    expected_statement = """
        CREATE TABLE current_node_table AS 
        SELECT node_a_table.gid, node_b_table.gid
        FROM node_a_table AS a
        INNER JOIN node_b_table AS b
        ON a.a_id = b.b_id;   
    """
    assert statement.strip() == expected_statement.strip()