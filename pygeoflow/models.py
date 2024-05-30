"""Geoflow - Models"""

from typing import List, Literal, Any

from pydantic import BaseModel

SPATIAL_PREICATES = Literal['ST_CONTAINS','ST_INTERSECTS','ST_WITHIN','ST_OVERLAPS','ST_TOUCHES']

class NodeModel(BaseModel):
    """
    Model used for validation of a node in the workflow
    """

    type: str
    data: object
    id: str

    class Config:
        """config"""
        populate_by_name = True
        extra = 'allow'

class EdgeModel(BaseModel):
    """
    Model used for validation of a edge in the workflow
    """

    source: str
    target: str

    class Config:
        """config"""
        populate_by_name = True
        extra = 'allow'

class WorkflowModel(BaseModel):
    """
    Model used for validation of workflow
    """

    nodes: List[NodeModel]
    edges: List[EdgeModel]
    workflow_id: str
    clear_temporary_tables: bool

class GenericNode(BaseModel):
    """
    Model used for generic node
    """
    output_table_name: str
    output_table_schema: str

    class Config:
        """config"""
        populate_by_name = True
        extra = 'allow'

class WhereFilterModel(BaseModel):
    """
    Model used for validation of where filter
    """
    node: GenericNode
    current_node: GenericNode
    sql_filter: str

class IntersectsModel(BaseModel):
    """
    Model used for validation of intersects
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode

class BufferModel(BaseModel):
    """
    Model used for validation of buffer
    """
    node: GenericNode
    current_node: GenericNode
    distance_in_meters: float

class ClipModel(BaseModel):
    """
    Model used for validation of clip
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode

class CentroidModel(BaseModel):
    """
    Model used for validation of centroid
    """
    node: GenericNode
    current_node: GenericNode

class ClosestPointToPolygonsModel(BaseModel):
    """
    Model used for validation of closest point to polygons
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode

class NormalizeModel(BaseModel):
    """
    Model used for validation of normalize
    """
    node: GenericNode
    current_node: GenericNode
    column: str
    decimals: int

class SaveTableModel(BaseModel):
    """
    Model used for validation of saving table
    """
    node: GenericNode
    current_node: GenericNode

class JoinModel(BaseModel):
    """
    Model used for validation of joining data
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode
    join_type: Literal['INNER JOIN','LEFT JOIN','RIGHT JOIN','FULL OUTER JOIN']

class DifferenceModel(BaseModel):
    """
    Model used for validation of difference
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode

class UnionModel(BaseModel):
    """
    Model used for validation of union
    """
    current_node: GenericNode
    tables: list

class TableFromGeojsonModel(BaseModel):
    """
    Model used for validation of creating table from geojson
    """
    current_node: GenericNode
    geojson: object

class CreateColumnModel(BaseModel):
    """
    Model used for validation of creating a new column
    """
    node: GenericNode
    column_name: str
    column_type: str
    expression: str

class DropColumnModel(BaseModel):
    """
    Model used for validation of dropping a column
    """
    node: GenericNode
    column_name: str

class FindAndReplaceModel(BaseModel):
    """
    Model used for validation of finding and replacing a
    value in a column in a table.
    """
    node: GenericNode
    column_name: str
    old_value: Any
    new_value: Any

class RenameColumnModel(BaseModel):
    """
    Model used for validation of renaming column.
    """
    node: GenericNode
    column_name: str
    new_column_name: str

class BoundingBoxModel(BaseModel):
    """
    Model used for validation of bounding box.
    """
    node: GenericNode
    current_node: GenericNode

class StatisticsModel(BaseModel):
    """
    Model used for validation of statistics.
    """
    column: str
    type: Literal['AVG','MIN','MAX','SUM','COUNT']


class PointsInPolygonsModel(BaseModel):
    """
    Model used for validation of points in polygons.
    """
    node_a: GenericNode
    node_b: GenericNode
    current_node: GenericNode
    spatial_predicate: SPATIAL_PREICATES
    join_type: Literal['INNER JOIN','LEFT JOIN','RIGHT JOIN','FULL OUTER JOIN']="LEFT JOIN"
    statistics: List[StatisticsModel]

class DumpGeometryModel(BaseModel):
    """
    Model used for validation of dumping geometry.
    """
    node_a: GenericNode
    current_node: GenericNode
    geometry_type: Literal['Point','Polygon','Line']

class GeneratePointsModel(BaseModel):
    """
    Model used for validation of generating points.
    """
    node_a: GenericNode
    current_node: GenericNode
    number_of_points: int