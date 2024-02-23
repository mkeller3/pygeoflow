from unittest.mock import Mock, patch
from geoflow.join import spatial_join

@patch('geoflow.utilities.get_table_columns')
def test_spatial_join(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, schema ,new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "node_a_table","output_table_schema": "geoflow"}
    node_b = {"output_table_name": "node_b_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "current_node_table","output_table_schema": "geoflow"}
    spatial_predicate = "ST_Contains"
    statement = spatial_join(cur, node_a, node_b, current_node, spatial_predicate)
    expected_statement = """
        CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS 
        SELECT node_a_table.gid, node_b_table.gid, a.geom
        FROM "geoflow"."node_a_table" AS a, "geoflow"."node_b_table" AS b
        WHERE ST_Contains(a.geom, b.geom);
    """
    assert statement.strip() == expected_statement.strip()