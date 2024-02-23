import unittest
from geoflow import workflow

class TestWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize objects required for testing
        self.nodes = [
            {"id": 1},
            {"id": 2},
            {"id": 3}
        ]
        self.edges = [
            {"source": 1, "target": 2},
            {"source": 2, "target": 3}
        ]
        self.workflow_instance = workflow.Workflow("localhost", "user", "password", "database", None)

    def test_sort_workflow(self):
        expected_order = [1, 2, 3]
        result = self.workflow_instance.sort_workflow(self.nodes, self.edges)
        self.assertEqual(result, expected_order)

    def test_sort_workflow_with_cycle(self):
        # Introduce a cycle in the graph
        self.edges.append({"source": 3, "target": 1})
        with self.assertRaises(ValueError):
            self.workflow_instance.sort_workflow(self.nodes, self.edges)