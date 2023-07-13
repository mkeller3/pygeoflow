import time
import json

import psycopg2
from psycopg2.extras import RealDictCursor
from geoflow import utilities
from geoflow import analysis

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

    def run_step(self, step, index):
        conn = psycopg2.connect(f"host={self.db_host} dbname={self.db_database} user={self.db_user} password={self.db_password}")
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if step["type"] == "analysis":
            start_time=time.time()
            if step["analysis"] == "intersects":
                statement = analysis.intersects(
                    cur=cur,
                    conn=conn,
                    node_a=self.workflow[step["node_a"]],
                    node_b=self.workflow[step["node_b"]],
                    current_node=step
                )
            elif step["analysis"] == "buffer":
                statement = analysis.buffer(
                    cur=cur,
                    conn=conn,
                    node=self.workflow[step["node_a"]],
                    distance_in_meters=step["distance_in_meters"],
                    current_node=step
                )
            elif step["analysis"] == 'filter':
                statement = (analysis.sql_filter(
                    cur=cur,
                    conn=conn,
                    node=self.workflow[step["node_a"]],
                    sql_filter=step["sql_filter"],
                    current_node=step
                ))
            elif step["analysis"] == 'clip':
                statement = (analysis.clip(
                    cur=cur,
                    conn=conn,
                    node_a=self.workflow[step["node_a"]],
                    node_b=self.workflow[step["node_b"]],
                    current_node=step
                ))
            elif step["analysis"] == 'centroids':
                statement = (analysis.centroid(
                    cur=cur,
                    conn=conn,
                    node_a=self.workflow[step["node_a"]],
                    current_node=step
                ))
            elif step["analysis"] == 'closest_point_to_polygons':
                statement = (analysis.get_closest_point_to_polygons(
                    cur=cur,
                    conn=conn,
                    node_a=self.workflow[step["node_a"]],
                    node_b=self.workflow[step["node_b"]],
                    current_node=step
                ))
            else:
                print("No analysis found")
            cur.execute(statement)
            conn.commit()
            utilities.create_default_columns(
                cur=cur,
                conn=conn,
                node=self.workflow[index]
            )

            end_time=time.time()-start_time
            self.workflow_stats[index] = {
                "new_table_name": self.workflow[index]["output_table_name"],
                "process_time": end_time,
                "analysis_type": step["analysis"]
            }
        cur.close()
        conn.close()


    def run(self):

        workflow_start_time = time.time()

        for index, step in enumerate(self.workflow):
            self.run_step(step, index)

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


        print(json.dumps(self.workflow_stats, indent=4))
