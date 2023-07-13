
def drop_table(
    cur,
    conn,
    node: object
):
    table = node["output_table_name"]
    cur.execute(f"""
        DROP TABLE IF EXISTS "{table}"
    """)
    conn.commit()

def create_default_columns(
    cur,
    conn,
    node: object
):
    table = node["output_table_name"]
    cur.execute(f"""
        DROP INDEX IF EXISTS "{table}_geom_idx"
    """)
    cur.execute(f"""
        ALTER TABLE {table} DROP COLUMN IF EXISTS "gid";
    """)
    conn.commit()
    cur.execute(f"""
        CREATE INDEX "{table}_geom_idx"
        ON "{table}"
        USING GIST (geom);
    """)
    cur.execute(f"""
        ALTER TABLE "{table}" 
        ADD COLUMN gid SERIAL PRIMARY KEY;
    """)
    conn.commit()


def get_table_columns(
    cur,
    table: str,
    new_table_name: str=None
) -> str:
    """
    Method to return a list of columns for a table.
    """


    sql_field_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table}'
    AND column_name != 'geom'
    AND column_name != 'gid';
    """

    cur.execute(sql_field_query)

    db_fields = cur.fetchall()

    fields = []

    for field in db_fields:
        if new_table_name:
            column_name = field['column_name']
            fields.append(f'"{new_table_name}"."{column_name}"')
        else:
            fields.append(f'''"{table}"."{field['column_name']}"''')

    if len(fields) == 0:
        return ""

    string_fields = ','.join(fields)

    return f"{string_fields},"
