
from typing import List, Literal

from pydantic import BaseModel

class NodeModel(BaseModel):
    """
    Model used for validation of a node in the workflow
    """

    type: str
    data: object
    id: str

    class Config:
        populate_by_name = True
        extra = 'allow'

class EdgeModel(BaseModel):
    """
    Model used for validation of a edge in the workflow
    """

    source: str
    target: str

    class Config:
        populate_by_name = True
        extra = 'allow'
 
class WorkflowModel(BaseModel):
    """
    Model used for validation of workflow
    """

    nodes: List[NodeModel]
    edges: List[EdgeModel]

class GenericNode(BaseModel):
    """
    Model used for generic node
    """
    output_table_name: str

    class Config:
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