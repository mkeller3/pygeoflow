from unittest.mock import Mock, patch
from geoflow.spatial_operations import get_closest_point_to_polygons

@patch('geoflow.utilities.get_table_columns')
def test_get_closest_point_to_polygons(mock_get_table_columns):
    mock_get_table_columns.side_effect = lambda cur, table, new_table_name=None: f'{table}.gid'
    cur = Mock()
    node_a = {"output_table_name": "points_table"}
    node_b = {"output_table_name": "polygons_table"}
    current_node = {"output_table_name": "output_table"}
    statement = get_closest_point_to_polygons(cur, node_a, node_b, current_node)
    expected_statement = """
        CREATE TABLE output_table AS 
        SELECT polygons.gid, points_table.gid, polygons_table.gid, points.dist * 62.1371192 as distance_in_miles, polygons.geom
        FROM polygons_table polygons
        CROSS JOIN LATERAL (
            SELECT points_table.gid polygons.geom <-> points.geom AS dist, points.geom
            FROM points_table AS points
            ORDER BY dist
            LIMIT 1
        ) points
    """
    assert statement.strip() == expected_statement.strip()