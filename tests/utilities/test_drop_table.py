from unittest.mock import Mock
from pygeoflow.utilities import drop_table

def test_drop_table():
    # Mocking cursor and connection objects
    cur = Mock()
    conn = Mock()

    # Define test data
    node = {"output_table_name": "test_table","output_table_schema": "public"}

    # Call the function with test data
    drop_table(cur, conn, node)

    # Assert that the cur.execute method was called with the correct SQL statement
    cur.execute.assert_called_once_with('DROP TABLE IF EXISTS "public"."test_table";')

    # Assert that the conn.commit method was called
    conn.commit.assert_called_once()
