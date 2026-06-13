import os
import json
import pandas as pd

def train_churn_model(**kwargs):
    print("Training churn model...")
    
    # Create dummy data
    data = {
        'tenure': [12, 24, 36, 48, 60],
        'monthly_charges': [50, 60, 70, 80, 90],
        'churn': [0, 0, 0, 0, 0]
    }
    df = pd.DataFrame(data)
    
    # Save dummy data
    df.to_csv('/opt/airflow/include/data/churn.csv', index=False)
    print("Churn model trained successfully.")


def predict_churn_model(**kwargs):
    print("Predicting churn model...")
    
    # Load dummy data
    df = pd.read_csv('/opt/airflow/include/data/churn.csv')
    
    # Save dummy data
    df.to_csv('/opt/airflow/include/data/churn_predictions.csv', index=False)
    print("Churn model predicted successfully.")


if __name__ == "__main__":
    train_churn_model() 
    predict_churn_model() 