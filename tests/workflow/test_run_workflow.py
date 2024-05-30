import unittest
from unittest.mock import MagicMock, patch
from pygeoflow import workflow

class TestWorkflow(unittest.TestCase):
    def setUp(self):
        # Initialize objects required for testing
        self.workflow_data = {
            "nodes": [
                {"data": {"type": "analysis"}},
                {"data": {"type": "analysis"}}
            ],
            "edges": [{"source": 0, "target": 1}],
            "workflow_id": "workflow_id",
            "clear_temporary_tables": True
        }
        self.workflow_instance = workflow.Workflow("localhost", "user", "password", "database", self.workflow_data)
        self.logger_mock = MagicMock()
        self.workflow_instance.logger = self.logger_mock

    @patch('pygeoflow.models.WorkflowModel')
    @patch('pygeoflow.workflow.Workflow.sort_workflow')
    @patch('pygeoflow.workflow.Workflow.sort_nodes_by_id_order')
    @patch('pygeoflow.workflow.Workflow.run_step')
    @patch('pygeoflow.logger.logger')
    def test_run_workflow(self, logger_mock, run_step_mock, sort_nodes_mock, sort_workflow_mock, workflow_model_mock):
        # Mocking dependencies
        mock_workflow_model_instance = MagicMock()
        workflow_model_mock.return_value = mock_workflow_model_instance
        sort_workflow_mock.return_value = [0, 1]
        sort_nodes_mock.return_value = [
            {"data": {"type": "analysis"}},
            {"data": {"type": "analysis"}}
        ]

        # Run the method
        self.workflow_instance.run_workflow()

        # Assertions
        workflow_model_mock.assert_called_once_with(
            nodes=self.workflow_data['nodes'],
            edges=self.workflow_data['edges'],
            workflow_id=self.workflow_data['workflow_id'],
            clear_temporary_tables=self.workflow_data['clear_temporary_tables']
        )
        sort_workflow_mock.assert_called_once_with(
            nodes=self.workflow_data['nodes'],
            edges=self.workflow_data['edges']
        )
        sort_nodes_mock.assert_called_once_with(
            nodes=self.workflow_data['nodes'],
            id_order=[0, 1]
        )
        run_step_mock.assert_called_with(step=self.workflow_data['nodes'][0], index=1)
        self.assertEqual(logger_mock.info.call_count, 1)  # Assuming logger.info is called once
