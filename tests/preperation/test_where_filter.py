from unittest.mock import Mock, patch
from pygeoflow.preperation import where_filter

@patch('pygeoflow.utilities.get_table_columns')
def test_where_filter(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, schema ,new_table_name=None: f'{table}.gid'
    cur = Mock()
    node = {"output_table_name": "node_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "current_node_table","output_table_schema": "geoflow"}
    sql_filter = "column_name = 'value'"
    statement = where_filter(cur, node, current_node, sql_filter)
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS 
    SELECT node_table.gid, "node_table".geom
    FROM "geoflow"."node_table" 
    WHERE column_name = 'value';
    """
    assert statement.strip() == expected_statement.strip()