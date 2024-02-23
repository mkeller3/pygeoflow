from geoflow.preperation import rename_column

def test_rename_column():
    node = {"output_table_name": "node_table","output_table_schema": "geoflow"}
    column_name = "old_column"
    new_column_name = "new_column"
    statement = rename_column(node, column_name, new_column_name)
    expected_statement = """
    ALTER TABLE "geoflow"."node_table"
    RENAME COLUMN "old_column" TO "new_column";
    """
    assert statement.strip() == expected_statement.strip()