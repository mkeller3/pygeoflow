from pygeoflow.join import union

def test_union():
    current_node = {"output_table_name": "current_node_table","output_table_schema": "geoflow"}
    tables = ["table1", "table2", "table3"]
    statement = union(current_node, tables)
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS
    SELECT * FROM table1 UNION ALL SELECT * FROM table2 UNION ALL SELECT * FROM table3;"""
    assert statement.strip() == expected_statement.strip()