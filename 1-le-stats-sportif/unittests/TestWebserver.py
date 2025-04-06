import os
import unittest
import numpy as np
from unittests.data_ingestor import DataIngestor

class TestWebserver(unittest.TestCase):
    """
    Test cases for the data ingestor module.
    """
    
    def setUp(self):
        """
        Set up the test case.
        """
        self.data_ingestor = DataIngestor("./test.csv")
        
    def tearDown(self):
        """
        Tear down the test case.
        """
        pass
    
    def test_states_mean(self):
        """
        Test the states_mean method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        
        expected = {
           "Massachusetts": 31.4,
           "New Hampshire": 35.3,
           "Rhode Island": 19.7,
           "Washington": 40.3,
           "Vermont": 37.9
        }
        
        result = self.data_ingestor.states_mean(question)
        result_python = {k: float(v) for k, v in result.items()}
        self.assertEqual(result_python, expected, "The result is not equal to the expected value.")
    
    def test_state_mean(self):
        """
        Test the state_mean method.
        """
        question = "Percent of adults aged 18 years and older who have obesity"
        state = "Ohio"
        
        expected = {"Ohio": 29.4}
        
        result = self.data_ingestor.state_mean(question, state)
        result = {state: float(result[state])}
        self.assertEqual(result, expected, "The result is not equal to the expected value.")
        
    def test_best5(self):
        """
        Test the best5 method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        
        expected = {
            "Massachusetts": 31.4,
            "New Hampshire": 35.3,
            "Rhode Island": 19.7,
            "Washington": 40.3,
            "Vermont": 37.9
        }
        
        result = self.data_ingestor.best5(question)
        result_python = {k: float(v) for k, v in result.items()}
        self.assertEqual(result_python, expected, "The result is not equal to the expected value.")
        
    def test_worst5(self):
        """
        Test the worst5 method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        
        expected = {
            "Massachusetts": 31.4,
            "New Hampshire": 35.3,
            "Rhode Island": 19.7,
            "Washington": 40.3,
            "Vermont": 37.9
        }
        
        result = self.data_ingestor.worst5(question)
        result_python = {k: float(v) for k, v in result.items()}
        self.assertEqual(result_python, expected, "The result is not equal to the expected value.")
    
    def test_global_mean(self):
        """
        Test the global_mean method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        
        expected = {"global_mean": 32.92}
        
        result = self.data_ingestor.global_mean(question)
        result = {"global_mean": float(result["global_mean"])}
        self.assertEqual(result["global_mean"], expected["global_mean"], 
                        "The global_mean result is not equal to the expected value.")
    
    def test_diff_from_mean(self):
        """
        Test the diff_from_mean method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        
        expected = {
            "Massachusetts": 1.5200000000000031,
            "New Hampshire": -2.3799999999999955,
            "Rhode Island": 13.220000000000002,
            "Vermont": -4.979999999999997,
            "Washington": -7.3799999999999955
        }
        
        result = self.data_ingestor.diff_from_mean(question)
        result_python = {k: float(v) for k, v in result.items()}    
        
        for state in expected:
            self.assertEqual(result_python[state], expected[state],
                          f"The diff_from_mean for {state} is not equal to the expected value.")
    
    def test_state_diff_from_mean(self):
        """
        Test the state_diff_from_mean method.
        """
        question = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        state = "Washington"
        
        expected = {"Washington": -7.3799999999999955}  
        
        result = self.data_ingestor.state_diff_from_mean(question, state)
        result = {state: float(result[state])}
        
        self.assertEqual(result[state], expected[state],
                        f"The state_diff_from_mean for {state} is not equal to the expected value.")

if __name__ == '__main__':
    unittest.main()