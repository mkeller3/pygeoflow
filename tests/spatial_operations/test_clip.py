from unittest.mock import Mock, patch
from pygeoflow.spatial_operations import clip

@patch('geoflow.utilities.get_table_columns')
def test_clip(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, schema ,new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "data_table","output_table_schema": "geoflow"}
    node_b = {"output_table_name": "polygon_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "output_table","output_table_schema": "geoflow"}
    statement = clip(cur, node_a, node_b, current_node)

    expected_statement = """
        CREATE TABLE IF NOT EXISTS "geoflow"."output_table_within" AS 
        SELECT data_table.gid, a.geom
        FROM "geoflow"."data_table" AS a, "geoflow"."polygon_table" AS b
        WHERE ST_Within(a.geom, b.geom);

        CREATE TABLE IF NOT EXISTS "geoflow"."output_table_intersects" AS 
        SELECT data_table.gid, ST_Intersection(a.geom, b.geom) as geom
        FROM "geoflow"."data_table" AS a, "geoflow"."polygon_table" AS b
        WHERE ST_Intersects(a.geom, ST_Boundary(b.geom));

        CREATE TABLE IF NOT EXISTS "geoflow"."output_table" AS 
        SELECT * 
        FROM "geoflow"."output_table_within"
        UNION
        SELECT * 
        FROM "geoflow"."output_table_intersects";

        DROP TABLE IF EXISTS "geoflow"."output_table_within";
        DROP TABLE IF EXISTS "geoflow"."output_table_intersects";
    """
    assert statement.strip() == expected_statement.strip()