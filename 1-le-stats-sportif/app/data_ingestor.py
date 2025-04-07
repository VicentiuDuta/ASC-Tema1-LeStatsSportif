"""
This module handles the ingestion and processing of nutritional and health data from CSV files.
It provides various statistical calculations on the data, such as mean values by state,
best and worst performers, and comparisons to global means.
"""
import pandas as pd

class DataIngestor:
    """
    Processes and analyzes nutritional and physical activity data from a CSV file.
    
    This class provides methods to calculate various statistics based on the dataset,
    including state means, global means, and rankings of states based on different
    health metrics.
    """
    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path)
        required_columns = ['Question', 'LocationDesc', 'Data_Value',
                            'StratificationCategory1', 'Stratification1']
        for col in required_columns:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column: {col} in CSV file.")


        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity '
            'aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic '
            'activity (or an equivalent combination)',

            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity '
            'aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic '
            'physical activity and engage in muscle-strengthening activities on 2 or more '
            'days a week',

            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity '
            'aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic '
            'activity (or an equivalent combination)',

            'Percent of adults who engage in muscle-strengthening activities on 2 or more '
            'days a week',
        ]

    def states_mean(self, question):
        """
        Calculate the mean value for each state for a specific question.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary mapping state names to their mean values, sorted by value
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        # Filter the DataFrame for the specific question
        filtered_data = self.df[self.df['Question'] == question]
        # Group by 'LocationDesc' and calculate the mean of 'Data_Value'
        states_mean = filtered_data.groupby('LocationDesc')['Data_Value'].mean()
        # Sort the results
        states_mean = states_mean.sort_values()

        # Convert to a dictionary and return
        states_mean_dict = states_mean.to_dict()
        return states_mean_dict

    def state_mean(self, question, state):
        """
        Calculate the mean value for a specific state and question.
        
        Args:
            question (str): The health metric question to analyze
            state (str): The state name to calculate the mean for
            
        Returns:
            dict: Dictionary with the state name as key and its mean value
            
        Raises:
            ValueError: If the question or state is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")

        # Filter the DataFrame for the specific question and state
        filtered_data = self.df[(self.df['Question'] == question) &
                                (self.df['LocationDesc'] == state)]
        if filtered_data.empty:
            raise ValueError(f"No data found for question '{question}' in state '{state}'.")

        # Calculate the mean of 'Data_Value'
        mean_value = filtered_data['Data_Value'].mean()
        return {
            state: mean_value
        }

    def global_mean(self, question):
        """
        Calculate the global mean value for a specific question across all states.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary with 'global_mean' as key and the mean value
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        # Filter the DataFrame for the specific question
        filtered_data = self.df[self.df['Question'] == question]
        global_mean = filtered_data['Data_Value'].mean()
        return {
            "global_mean": global_mean
        }

    def diff_from_mean(self, question):
        """
        Calculate the difference between the global mean and each state's mean for a question.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary mapping state names to their difference from the global mean
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        global_mean = self.global_mean(question)['global_mean']
        states_mean = self.states_mean(question)
        # Calculate the difference from the global mean for each state
        differences = {state: global_mean - state_mean for state, state_mean in states_mean.items()}
        return differences

    def state_diff_from_mean(self, question, state):
        """
        Calculate the difference between the global mean and a specific state's mean.
        
        Args:
            question (str): The health metric question to analyze
            state (str): The state name to calculate the difference for
            
        Returns:
            dict: Dictionary with the state name as key and its difference from the global mean
            
        Raises:
            ValueError: If the question or state is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")
        # Calculate the difference from the global mean for a specific state
        global_mean = self.global_mean(question)['global_mean']
        state_mean = self.state_mean(question, state)[state]
        return {
            state: global_mean - state_mean
        }

    def mean_by_category(self, question):
        """
        Calculate mean values grouped by stratification categories for all states.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary mapping tuples of (location, category, stratification) to mean values
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        # Filter the DataFrame for the specific question
        filtered_data = self.df[self.df['Question'] == question]
        grouped = filtered_data.groupby(['LocationDesc',
                                         'StratificationCategory1', 'Stratification1'])
        means = grouped['Data_Value'].mean()

        # Convert the result to a dictionary with the desired structure
        result_dict = {
            str((location, cat1, cat2)): value
            for (location, cat1, cat2), value in means.items()
        }

        return result_dict

    def state_mean_by_category(self, question, state):
        """
        Calculate mean values grouped by stratification categories for a specific state.
        
        Args:
            question (str): The health metric question to analyze
            state (str): The state name to calculate the means for
            
        Returns:
            dict: Nested dictionary with state as outer key and (category, stratification) 
                                                                    tuples as inner keys
            
        Raises:
            ValueError: If the question or state is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")

        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")

        # Filter the DataFrame for the specific question and state
        filtered_data = self.df[(self.df['Question'] == question) &
                                (self.df['LocationDesc'] == state)]
        grouped = filtered_data.groupby(['StratificationCategory1', 'Stratification1'])
        means = grouped['Data_Value'].mean()

        # Convert the result to a dictionary with the desired structure
        result_dict = {
            state: {
                str((cat1, cat2)): value
                for (cat1, cat2), value in means.items()
            }
        }

        return result_dict


    def best5(self, question):
        """
        Get the top 5 performing states for a specific question.
        
        For positive metrics (like exercise), returns states with highest values.
        For negative metrics (like obesity), returns states with lowest values.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary of the top 5 states and their values
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        # Calculate the mean for each state and return the top 5
        states_mean = self.states_mean(question)
        if question in self.questions_best_is_max:
            return dict(sorted(states_mean.items(), key=lambda item: item[1], reverse=True)[:5])

        return dict(sorted(states_mean.items(), key=lambda item: item[1])[:5])

    def worst5(self, question):
        """
        Get the 5 worst performing states for a specific question.
        
        For positive metrics (like exercise), returns states with lowest values.
        For negative metrics (like obesity), returns states with highest values.
        
        Args:
            question (str): The health metric question to analyze
            
        Returns:
            dict: Dictionary of the 5 worst states and their values
            
        Raises:
            ValueError: If the question is not found in the dataset
        """
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        # Calculate the mean for each state and return the bottom 5
        states_mean = self.states_mean(question)
        if question in self.questions_best_is_max:
            return dict(sorted(states_mean.items(), key=lambda item: item[1])[:5])

        return dict(sorted(states_mean.items(), key=lambda item: item[1], reverse=True)[:5])
