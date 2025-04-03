import os
import json
import pandas as pd

class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
        self.df = pd.read_csv(csv_path)
        required_columns = ['Question', 'LocationDesc', 'Data_Value', 'StratificationCategory1', 'Stratification1']
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
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
    
    def states_mean(self, question):
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
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")
        
        # Filter the DataFrame for the specific question and state
        filtered_data = self.df[(self.df['Question'] == question) & (self.df['LocationDesc'] == state)]
        if filtered_data.empty:
            raise ValueError(f"No data found for question '{question}' in state '{state}'.")
        
        # Calculate the mean of 'Data_Value'
        mean_value = filtered_data['Data_Value'].mean()
        return mean_value
    
    def global_mean(self, question):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        # Filter the DataFrame for the specific question
        filtered_data = self.df[self.df['Question'] == question]
        global_mean = filtered_data['Data_Value'].mean()
        return global_mean

    def diff_from_mean(self, question):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        global_mean = self.global_mean(question)
        state_means = self.states_mean(question)
        # Calculate the difference from the global mean for each state
        differences = {state: global_mean - state_mean for state, state_mean in state_means.items()}
        return differences
        
    def state_diff_from_mean(self, question, state):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")
        # Calculate the difference from the global mean for a specific state
        global_mean = self.global_mean(question)
        state_mean = self.state_mean(question, state)
        return global_mean - state_mean
        
    def mean_by_category(self, question):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        # Filter the DataFrame for the specific question
        filtered_data = self.df[self.df['Question'] == question]
        grouped = filtered_data.groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])
        means = grouped['Data_Value'].mean()
        
        # Convert the result to a dictionary with the desired structure
        result_dict = {
            str((location, cat1, cat2)): value
            for (location, cat1, cat2), value in means.items()
        }
        
        return result_dict
    
    def state_mean_by_category(self, question, state):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        
        if state not in self.df['LocationDesc'].unique():
            raise ValueError(f"State '{state}' not found in the dataset.")
        
        # Filter the DataFrame for the specific question and state
        filtered_data = self.df[(self.df['Question'] == question) & (self.df['LocationDesc'] == state)]
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
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        # Calculate the mean for each state and return the top 5
        states_mean = self.states_mean(question)
        if question in self.questions_best_is_max:
            return dict(sorted(states_mean.items(), key=lambda item: item[1], reverse=True)[:5])
        else:
            return dict(sorted(states_mean.items(), key=lambda item: item[1])[:5])
    
    def worst5(self, question):
        if question not in self.df['Question'].unique():
            raise ValueError(f"Question '{question}' not found in the dataset.")
        # Calculate the mean for each state and return the bottom 5
        states_mean = self.states_mean(question)
        if question in self.questions_best_is_max:
            return dict(sorted(states_mean.items(), key=lambda item: item[1])[:5])
        else:
            return dict(sorted(states_mean.items(), key=lambda item: item[1], reverse=True)[:5])
            
        
        
        
        
