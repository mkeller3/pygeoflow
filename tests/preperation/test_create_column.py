from geoflow.preperation import create_column

def test_create_column():
    node = {"output_table_name": "node_table"}
    column_name = "new_column"
    column_type = "INTEGER"
    expression = "column_a + column_b"
    statement = create_column(node, column_name, column_type, expression)
    expected_statement = """
    ALTER TABLE node_table 
    ADD COLUMN IF NOT EXISTS 'new_column' INTEGER;

    UPDATE node_table
    SET 'new_column' = column_a + column_b;
    """
    assert statement.strip() == expected_statement.strip()