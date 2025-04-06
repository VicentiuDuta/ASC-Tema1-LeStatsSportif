import os
import unittest
from app.data_ingestor import DataIngestor
import json
from deepdiff import DeepDiff

class TestWebServer(unittest.TestCase):
    """
    Test cases for the data ingestor module.
    """
    
    def setUp(self):
        """
        Set up the test case.
        """
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
        self.input_test_dir = os.path.join(os.path.dirname(__file__), '../tests')
        os.environ["TESTING"] = "1"        
    def tearDown(self):
        """
        Tear down the test case.
        """
        self.input_test_dir = None
        os.environ["TESTING"] = "0"
    
    def test_states_mean(self):
        """
        Test the states_mean method.
        """

        # get input json file
        with open(os.path.join(self.input_test_dir, 'states_mean', 'input', 'in-1.json'), 'r') as f:
            input = json.load(f)
        
        # get expected json file
        with open(os.path.join(self.input_test_dir, 'states_mean', 'output', 'out-1.json'), 'r') as f:
            expected = json.load(f)
        
        
        # get question value from input json
        question = input['question']
        result = self.data_ingestor.states_mean(question)
        diff = DeepDiff(result, expected)
        self.assertEqual(diff, {}, "The result is not equal to the expected value.")


    
