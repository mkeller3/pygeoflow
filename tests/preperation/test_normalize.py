from unittest.mock import Mock, patch
from pygeoflow.preperation import normalize

@patch('pygeoflow.utilities.get_table_columns')
def test_normalize(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, schema ,new_table_name=None: f'{table}.gid'
    cur = Mock()
    node = {"output_table_name": "node_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "current_node_table","output_table_schema": "geoflow"}
    column = "column_to_normalize"
    decimals = 2
    statement = normalize(cur, node, current_node, column, decimals)
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS
    SELECT ROUND(ROUND((column_to_normalize - min_value),2) / ROUND((max_value - min_value),2),2) AS normalized, node_table.gid, "geoflow"."node_table".geom
    FROM "geoflow"."node_table", (SELECT MIN(column_to_normalize) AS min_value, MAX(column_to_normalize) AS max_value FROM "geoflow"."node_table") AS min_max;
    """
    assert statement.strip() == expected_statement.strip()