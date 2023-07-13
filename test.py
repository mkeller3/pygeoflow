from geoflow import workflow

current_workflow = [
    {
        "type": "source",
        "input_table_name": "us_grids",
        "output_table_name": "us_grids",
        "columns": "*",
        "sql_filter": None
    },
    {
        "type": "source",
        "input_table_name": "states",
        "output_table_name": "states",
        "columns": "*",
        "sql_filter": None
    },
    {
        "type": "source",
        "input_table_name": "congressional_districts",
        "output_table_name": "congressional_districts",
        "columns": "*",
        "sql_filter": None
    },
    {
        "type": "source",
        "input_table_name": "starbucks",
        "output_table_name": "starbucks",
        "columns": "*",
        "sql_filter": None
    },
    {
        "type": "analysis",
        "analysis": "filter",
        "output_table_name": "refgwbsszuhviifkxcerbfyocfluvggeusakcowtdukgvvcfic",
        "columns": "*",
        "sql_filter": "state_abbr = 'MN'",
        "node_a": 1,
        "save_table": False
    },
    {
        "type": "analysis",
        "analysis": "intersects",
        "output_table_name": "mfvxdhboqflfxswvwbwvsidggnxlinxyfhlxgjhwgstarxcds",
        "node_a": 4,
        "node_b": 0,
        "save_table": False
    },
    {
        "type": "analysis",
        "analysis": "clip",
        "output_table_name": "lcvpwnzenhqznrhzurtliikihoiakaqymlpsdpjmvlvpyptqvh",
        "node_a": 5,
        "node_b": 4,
        "save_table": False
    },
    {
        "type": "analysis",
        "analysis": "centroids",
        "output_table_name": "klmmmefselcboykxqxyscaoovrqocvwwvdxfksagvhgsvaizxy",
        "node_a": 6,
        "save_table": True
    },
    {
        "type": "analysis",
        "analysis": "intersects",
        "output_table_name": "wbvajlmffozvbidyqrmnnpehewvdbjksckwssuvpfpaodnrznh",
        "node_a": 2,
        "node_b": 7,
        "save_table": True
    },
    {
        "type": "analysis",
        "analysis": "closest_point_to_polygons",
        "output_table_name": "vehazphcwtalbrtvxucqxqkkgjkuwpensjrsjnuzlqktutyhek",
        "node_a": 3,
        "node_b": 5,
        "save_table": True
    }
]

workflow.Worflow(
    db_host="localhost",
    db_password="postgres",
    db_database="data",
    db_user="postgres",
    workflow=current_workflow
).run()

# workflow.Worflow(
#     db_host="localhost",
#     db_password="postgres",
#     db_database="data",
#     db_user="postgres",
#     workflow=current_workflow
# ).run_step(current_workflow[5], 5)