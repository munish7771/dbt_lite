import argparse
from databricks import sql
from .core import load_models, build_dag, execute_model
from .config import DATABRICKS_CONFIG

def main():
    print("DBT Lite CLI started")  # Confirm it's running
    parser = argparse.ArgumentParser(description="DBT Lite: Run SQL models on Databricks")
    parser.add_argument("--models-dir", default="models", help="Directory containing SQL models")
    args = parser.parse_args()

    models = load_models(args.models_dir)
    dag = build_dag(models)
    sorted_models = list(nx.topological_sort(dag))

    model_map = {m.name: m for m in models}
    with sql.connect(**DATABRICKS_CONFIG) as conn:
        for name in sorted_models:
            model = model_map[name]
            print(f"Running {model.name}...")
            execute_model(model, conn)

    print("All models executed successfully.")