from pygeoflow.join import difference

def test_difference():
    node_a = {"output_table_name": "node_a_table","output_table_schema": "geoflow"}
    node_b = {"output_table_name": "node_b_table","output_table_schema": "geoflow"}
    current_node = {"output_table_name": "current_node_table","output_table_schema": "geoflow"}
    statement = difference(node_a, node_b, current_node)
    expected_statement = """
        CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS 
        SELECT ST_Difference(a.geom,b.geom) as geom
        FROM "geoflow"."node_a_table" AS a, "geoflow"."node_b_table" AS b; 
    """
    assert statement.strip() == expected_statement.strip()