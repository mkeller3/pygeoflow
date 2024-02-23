import unittest
from unittest.mock import MagicMock
from geoflow import workflow

class TestWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize objects required for testing
        self.workflow_data = {
            "nodes": [
                {"data": {"output_table_name": "table1"}},
                {"data": {"output_table_name": "table2"}},
                {"data": {"output_table_name": "table3"}}
            ]
        }
        self.workflow_instance = workflow.Workflow("localhost", "user", "password", "database", self.workflow_data)
        self.index = 1
        self.step = {"data": {"analysis": "some_analysis"}}
        self.end_time = 100
        self.logger_mock = MagicMock()
        self.workflow_instance.logger = self.logger_mock

    def test_update_workflow_stats(self):
        self.workflow_instance.update_workflow_stats(self.index, self.step, self.end_time)
        expected_stats = {
            'total_time': 0,
            1: {
                'new_table_name': 'table2',
                'process_time': 100,
                'analysis_type':
                'some_analysis'
            }
        }
        self.assertEqual(self.workflow_instance.workflow_stats, expected_stats)