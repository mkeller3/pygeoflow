from geoflow.join import difference

def test_difference():
    node_a = {"output_table_name": "node_a_table"}
    node_b = {"output_table_name": "node_b_table"}
    current_node = {"output_table_name": "current_node_table"}
    statement = difference(node_a, node_b, current_node)
    expected_statement = """
        CREATE TABLE current_node_table AS 
        SELECT ST_Difference(a.geom,b.geom) as geom
        FROM node_a_table AS a, node_b_table AS b   
    """
    assert statement.strip() == expected_statement.strip()