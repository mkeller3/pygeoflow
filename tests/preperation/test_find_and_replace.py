from geoflow.preperation import find_and_replace

def test_find_and_replace():
    node = {"output_table_name": "node_table"}
    column_name = "column_to_replace"
    old_value = "old"
    new_value = "new"
    statement = find_and_replace(node, column_name, old_value, new_value)
    expected_statement = """
    UPDATE 'node_table' 
    SET 'column_to_replace' = REPLACE('column_to_replace', old, new)     
    """
    assert statement.strip() == expected_statement.strip()