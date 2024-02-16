from geoflow.preperation import rename_column

def test_rename_column():
    node = {"output_table_name": "node_table"}
    column_name = "old_column"
    new_column_name = "new_column"
    statement = rename_column(node, column_name, new_column_name)
    expected_statement = """
    ALTER TABLE node_table
    RENAME COLUMN 'old_column' TO 'new_column';
    """
    assert statement.strip() == expected_statement.strip()