from pygeoflow.utilities import standardize_table

def test_standardize_table():
    # Define test data
    node = {"output_table_name": "test_table","output_table_schema": "public"}

    # Call the function with test data
    result = standardize_table(node)

    # Assert that the result is as expected
    expected_statement = """
    DROP INDEX IF EXISTS test_table_geom_idx;
    ALTER TABLE "public"."test_table" DROP COLUMN IF EXISTS "gid";
    CREATE INDEX test_table_geom_idx ON "public"."test_table" USING GIST (geom);
    ALTER TABLE "public"."test_table" ADD COLUMN gid SERIAL PRIMARY KEY;
    """
    assert result.strip() == expected_statement.strip()