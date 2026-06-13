import os
import pandas as pd
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score
import mlflow

# Path for local data storage
DATA_DIR = '/opt/airflow/include/data/mlops'

def prepare_data():
    """Loads the breast cancer dataset and splits it into train/test."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Load dataset
    data = load_breast_cancer()
    X = pd.DataFrame(data.data, columns=data.feature_names)
    y = pd.Series(data.target, name='target')
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Save to disk
    X_train.to_csv(os.path.join(DATA_DIR, 'X_train.csv'), index=False)
    X_test.to_csv(os.path.join(DATA_DIR, 'X_test.csv'), index=False)
    y_train.to_csv(os.path.join(DATA_DIR, 'y_train.csv'), index=False)
    y_test.to_csv(os.path.join(DATA_DIR, 'y_test.csv'), index=False)
    
    print("Data preparation completed.")

def train_and_log_model(n_estimators=100, max_depth=None):
    """Trains a Random Forest and logs it to MLflow."""
    # Ensure MLflow knows where the tracking server is
    # This is set in docker-compose.yaml as MLFLOW_TRACKING_URI
    
    mlflow.set_experiment("Breast_Cancer_Classification")
    
    # Load train data
    X_train = pd.read_csv(os.path.join(DATA_DIR, 'X_train.csv'))
    y_train = pd.read_csv(os.path.join(DATA_DIR, 'y_train.csv')).squeeze()
    
    with mlflow.start_run():
        # Enable autologging
        mlflow.sklearn.autolog()
        
        # Train model
        rf = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
        rf.fit(X_train, y_train)
        
        print(f"Model trained with n_estimators={n_estimators}, max_depth={max_depth}")

def evaluate_model():
    """Evaluates the latest model on the test set."""
    # Load test data
    X_test = pd.read_csv(os.path.join(DATA_DIR, 'X_test.csv'))
    y_test = pd.read_csv(os.path.join(DATA_DIR, 'y_test.csv')).squeeze()
    
    # In a real scenario, we'd query the MLflow Model Registry for the "Production" model
    # Here we just find the last run for simplicity, or we can use the latest logged model.
    # We will search the experiment runs
    experiment = mlflow.get_experiment_by_name("Breast_Cancer_Classification")
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], order_by=["start_time desc"], max_results=1)
    latest_run_id = runs.iloc[0].run_id
    
    # Load model
    logged_model = f'runs:/{latest_run_id}/model'
    loaded_model = mlflow.sklearn.load_model(logged_model)
    
    # Predict and evaluate
    y_pred = loaded_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    
    print(f"Evaluation Results -> Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}")
    
    # We can also log these test metrics to the same run
    with mlflow.start_run(run_id=latest_run_id):
        mlflow.log_metric("test_accuracy", accuracy)
        mlflow.log_metric("test_precision", precision)
        mlflow.log_metric("test_recall", recall)

if __name__ == "__main__":
    # For local testing without Airflow
    os.environ['MLFLOW_TRACKING_URI'] = 'http://localhost:5000'
    prepare_data()
    train_and_log_model(n_estimators=50)
    evaluate_model()
