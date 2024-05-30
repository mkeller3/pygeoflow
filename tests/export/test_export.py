from pygeoflow import export

def test_save_as_table():
    # Define test data
    node = {"output_table_name": "node_table","output_table_scheme": "geoflow"}
    current_node = {"output_table_name": "current_node_table","output_table_scheme": "geoflow"}

    # Call the function with test data
    statement = export.save_as_table(node, current_node)

    # Define the expected statement based on the test data
    expected_statement = """
    CREATE TABLE IF NOT EXISTS "geoflow"."current_node_table" AS
    SELECT *
    FROM "geoflow"."node_table";
    """

    # Assert that the returned statement matches the expected statement
    assert statement.strip() == expected_statement.strip()