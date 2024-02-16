from geoflow import utilities

def table_from_geojson(
    geojson: object,
    current_node: object
):
    new_table_name = current_node["output_table_name"]
    statement = f"""
        CREATE TABLE IF NOT EXISTS "{new_table_name}" AS 
        SELECT ST_GeomFromGeoJSON('{geojson}') as geom
    """
    return statement