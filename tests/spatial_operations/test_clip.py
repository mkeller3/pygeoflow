from unittest.mock import Mock, patch
from geoflow.spatial_operations import clip

@patch('geoflow.utilities.get_table_columns')
def test_clip(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "data_table"}
    node_b = {"output_table_name": "polygon_table"}
    current_node = {"output_table_name": "output_table"}
    statement = clip(cur, node_a, node_b, current_node)
    expected_statement = """
        CREATE TABLE "output_table_within" AS 
        SELECT data_table.gid, a.geom
        FROM data_table AS a, polygon_table AS b
        WHERE ST_Within(a.geom, b.geom);

        CREATE TABLE "output_table_intersects" AS 
        SELECT data_table.gid, ST_Intersection(a.geom, b.geom) as geom
        FROM data_table AS a, polygon_table AS b
        WHERE ST_Intersects(a.geom, ST_Boundary(b.geom));

        CREATE TABLE output_table AS 
        SELECT * 
        FROM "output_table_within"
        UNION
        SELECT * 
        FROM "output_table_intersects";

        DROP TABLE IF EXISTS "output_table_within";
        DROP TABLE IF EXISTS "output_table_intersects";
    """
    assert statement.strip() == expected_statement.strip()