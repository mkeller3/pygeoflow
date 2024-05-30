from unittest.mock import Mock, patch
from pygeoflow.spatial_operations import bounding_box

@patch('geoflow.utilities.get_table_columns')
def test_bounding_box(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, schema ,new_table_name=None: f'{table}.gid'
    cur = Mock()
    node = {"output_table_name": "polygon_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "output_table","output_table_schema": "geoflow"}
    statement = bounding_box(cur, node, current_node)
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."output_table" AS 
    SELECT polygon_table.gid, ST_Envelope(geom) as geom
    FROM "geoflow"."polygon_table";
    """
    assert statement.strip() == expected_statement.strip()