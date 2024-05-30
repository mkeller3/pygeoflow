from unittest.mock import Mock
from pygeoflow.utilities import get_table_columns

def test_get_table_columns_without_new_table_name():
    # Mocking cursor object
    cur = Mock()

    # Mocking fetchall method to return sample data
    cur.fetchall.return_value = [{'column_name': 'column1'}, {'column_name': 'column2'}, {'column_name': 'column3'}]

    # Define test data
    table = "test_table"

    schema = "test"

    # Call the function with test data
    result = get_table_columns(cur, table, schema)

    # Assert that the result is as expected
    expected_result = '"test_table"."column1","test_table"."column2","test_table"."column3"'
    assert result == expected_result

def test_get_table_columns_with_new_table_name():
    # Mocking cursor object
    cur = Mock()

    # Mocking fetchall method to return sample data
    cur.fetchall.return_value = [{'column_name': 'column1'}, {'column_name': 'column2'}, {'column_name': 'column3'}]

    # Define test data
    table = "test_table"
    schema = "test"
    new_table_name = "new_table"

    # Call the function with test data
    result = get_table_columns(cur, table, schema, new_table_name)

    # Assert that the result is as expected
    expected_result = '"new_table"."column1","new_table"."column2","new_table"."column3"'
    assert result == expected_result

def test_get_table_columns_return_as_list():
    # Mocking cursor object
    cur = Mock()

    # Mocking fetchall method to return sample data
    cur.fetchall.return_value = [{'column_name': 'column1'}, {'column_name': 'column2'}, {'column_name': 'column3'}]

    # Define test data
    table = "test_table"

    schema = "test"

    # Call the function with test data
    result = get_table_columns(cur, table, schema, return_as_string=False)

    # Assert that the result is as expected
    expected_result = ['"test_table"."column1"', '"test_table"."column2"', '"test_table"."column3"']
    assert result == expected_result

def test_get_table_columns_return_as_list_no_values():
    # Mocking cursor object
    cur = Mock()

    # Mocking fetchall method to return sample data
    cur.fetchall.return_value = []

    # Define test data
    table = "test_table"

    schema = "test"

    # Call the function with test data
    result = get_table_columns(cur, table, schema, return_as_string=False)

    # Assert that the result is as expected
    expected_result = []
    assert result == expected_result


def test_get_table_columns_return_no_values():
    # Mocking cursor object
    cur = Mock()

    # Mocking fetchall method to return sample data
    cur.fetchall.return_value = []

    # Define test data
    table = "test_table"

    schema = "test"

    # Call the function with test data
    result = get_table_columns(cur, table, schema)

    # Assert that the result is as expected
    expected_result = ""
    assert result == expected_result