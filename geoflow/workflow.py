import time
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import ValidationError

from geoflow import utilities
from geoflow import preperation
from geoflow import join
from geoflow import spatial_operations
from geoflow import export
from geoflow import models
from geoflow import logger

class Worflow():
    """
    This class provides you the ability to execute a workflow
    """

    def __init__(
        self,
        db_host: str,
        db_user: str,
        db_password: str,
        db_database: str,
        workflow: object
    ):
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_database = db_database
        self.workflow = workflow
        self.workflow_stats = {
            "total_time": 0
        }    

    def sort_workflow(
        self,
        nodes: list,
        edges: list
    ):
        """
        Method that is used to sort the nodes in the correct order
        to process for an entire workflow
        """
        graph = defaultdict(list)
        in_degree = {node['id']: 0 for node in nodes}

        # Build the graph and calculate in-degrees
        for edge in edges:
            source_id = edge['source']
            target_id = edge['target']
            graph[source_id].append(target_id)
            in_degree[target_id] += 1

        # Find nodes with no incoming edges
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order = []

        # Perform topological sorting
        while queue:
            node_id = queue.pop(0)
            order.append(node_id)
            for neighbor_id in graph[node_id]:
                in_degree[neighbor_id] -= 1
                if in_degree[neighbor_id] == 0:
                    queue.append(neighbor_id)

        # Check for cycles
        if len(order) != len(nodes):
            raise ValueError("The graph contains a cycle.")
        
        nodes_ordered = []
        for node_id in order:
            nodes_ordered.append(node_id)

        return nodes_ordered

    def run_step(
        self, 
        step: object,
        index: int
    ):
        """
        Method called to run a single step within a workflow
        """
        conn = psycopg2.connect(f"host={self.db_host} dbname={self.db_database} user={self.db_user} password={self.db_password} options='-c statement_timeout=3600000'")
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if step["data"]["type"] == "analysis":
            start_time=time.time()
            edges = [edge["source"] for edge in self.workflow['edges'] if edge["target"] == step["id"]]
            source_nodes = [node for node in self.workflow['nodes'] if "id" in node and node["id"] in edges]
            if step["data"]["analysis"] == "intersects":
                try:
                    models.IntersectsModel(
                        node_a=source_nodes[0]["data"],
                        node_b=source_nodes[1]["data"],
                        current_node=step["data"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = join.intersects(
                    cur=cur,
                    conn=conn,
                    node_a=source_nodes[0]["data"],
                    node_b=source_nodes[1]["data"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == "buffer":
                try:
                    models.BufferModel(
                        node=source_nodes[0]["data"],
                        current_node=step["data"],
                        distance_in_meters=step["data"]["distance_in_meters"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = spatial_operations.buffer(
                    cur=cur,
                    conn=conn,
                    node=source_nodes[0]["data"],
                    distance_in_meters=step["data"]["distance_in_meters"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == 'filter':
                
                try:
                    models.WhereFilterModel(
                        node=source_nodes[0]["data"],
                        current_node=step["data"],
                        sql_filter=step["data"]["sql_filter"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = preperation.where_filter(
                    cur=cur,
                    conn=conn,
                    node=source_nodes[0]["data"],
                    sql_filter=step["data"]["sql_filter"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == 'clip':
                try:
                    models.ClipModel(
                        node_a=source_nodes[0]["data"],
                        node_b=source_nodes[1]["data"],
                        current_node=step["data"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = spatial_operations.clip(
                    cur=cur,
                    conn=conn,
                    node_a=source_nodes[0]["data"],
                    node_b=source_nodes[1]["data"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == 'centroids':
                try:
                    models.CentroidModel(
                        node_a=source_nodes[0]["data"],
                        current_node=step["data"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = spatial_operations.centroid(
                    cur=cur,
                    conn=conn,
                    node_a=source_nodes[0]["data"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == 'closest_point_to_polygons':
                try:
                    models.ClosestPointToPolygonsModel(
                        node_a=source_nodes[0]["data"],
                        node_b=source_nodes[1]["data"],
                        current_node=step["data"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = spatial_operations.get_closest_point_to_polygons(
                    cur=cur,
                    conn=conn,
                    node_a=source_nodes[0]["data"],
                    node_b=source_nodes[1]["data"],
                    current_node=step["data"]
                )
            elif step["data"]["analysis"] == 'normalize':
                try:
                    models.NormalizeModel(
                        node=source_nodes[0]["data"],
                        current_node=step["data"],
                        column=step["data"]["column"],
                        decimals=step["data"]["decimals"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = preperation.normalize(
                    cur=cur,
                    conn=conn,
                    node=source_nodes[0]["data"],
                    current_node=step["data"],
                    column=step["data"]["column"],
                    decimals=step["data"]["decimals"]
                )
            elif step["data"]["analysis"] == 'save_table':
                try:
                    models.SaveTableModel(
                        node=source_nodes[0]["data"],
                        current_node=step["data"]
                    )
                except ValidationError as exception:
                    raise ValidationError(exception) from exception
                statement = export.save_as_table(
                    node=source_nodes[0]["data"],
                    current_node=step["data"]
                )
            else:
                raise ValueError("No Analysis Found")
            cur.execute(statement)
            conn.commit()
            utilities.standardize_table(
                cur=cur,
                conn=conn,
                node=self.workflow['nodes'][index]["data"]
            )

            end_time=time.time()-start_time
            self.workflow_stats[index] = {
                "new_table_name": self.workflow['nodes'][index]["data"]["output_table_name"],
                "process_time": end_time,
                "analysis_type": step["data"]["analysis"]
            }
            logger.logger.debug(self.workflow_stats[index])
        cur.close()
        conn.close()

    def sort_nodes_by_id_order(
        self,
        nodes: list,
        id_order: list
    ):
        """
        Method that is used to sort the nodes in order of runtime.
        """
        order_dict = {node_id: index for index, node_id in enumerate(id_order)}

        sorted_nodes = sorted(nodes, key=lambda x: order_dict[x['id']])
        return sorted_nodes

    def run_workflow(self):
        """
        Method to run an entire workflow from start to finish.
        """

        workflow_start_time = time.time()

        try:
            models.WorkflowModel(
                nodes=self.workflow['nodes'],
                edges=self.workflow['edges']
            )
        except ValidationError as exception:
            raise ValidationError(exception) from exception
        
        ordered_nodes = self.sort_workflow(
            nodes=self.workflow['nodes'],
            edges=self.workflow['edges']
        )
        
        self.workflow['nodes'] = self.sort_nodes_by_id_order(
            nodes=self.workflow['nodes'],
            id_order=ordered_nodes
        )

        for index, step in enumerate(self.workflow['nodes']):
            self.run_step(
                step=step,
                index=index
            )

        conn = psycopg2.connect(f"host={self.db_host} dbname={self.db_database} user={self.db_user} password={self.db_password}")
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for step in self.workflow:
            if 'save_table' in step and step['save_table'] is False:
                utilities.drop_table(
                    cur=cur,
                    conn=conn,
                    node=step
                )
        cur.close()
        conn.close()

        self.workflow_stats['total_time'] = time.time()-workflow_start_time
        logger.logger.info(self.workflow_stats)
