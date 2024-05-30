import unittest
from pygeoflow import workflow

class TestWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize objects required for testing
        self.nodes = [
            {"id": 1, "runtime": 10},
            {"id": 2, "runtime": 5},
            {"id": 3, "runtime": 8}
        ]
        self.id_order = [2, 3, 1]
        self.workflow_instance = workflow.Workflow("localhost", "user", "password", "database", None)

    def test_sort_nodes_by_id_order(self):
        expected_sorted_nodes = [
            {"id": 2, "runtime": 5},
            {"id": 3, "runtime": 8},
            {"id": 1, "runtime": 10}
        ]
        result = self.workflow_instance.sort_nodes_by_id_order(self.nodes, self.id_order)
        self.assertEqual(result, expected_sorted_nodes)