# core.py
import os
import networkx as nx
from databricks import sql

class Model:
    def __init__(self, name, materialized, sql_body, depends_on):
        self.name = name
        self.materialized = materialized
        self.sql = sql_body
        self.depends_on = depends_on

def parse_model_file(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    meta = {"depends_on": []}
    sql_lines = []
    for line in lines:
        if line.startswith("--"):
            if "name:" in line:
                meta["name"] = line.split("name:")[1].strip()
            elif "materialized:" in line:
                meta["materialized"] = line.split("materialized:")[1].strip()
            elif "depends_on:" in line:
                deps = line.split("depends_on:")[1].strip()
                meta["depends_on"] = [d.strip() for d in deps.split(",") if d.strip()]
        else:
            sql_lines.append(line)
    return Model(meta["name"], meta["materialized"], ''.join(sql_lines), meta["depends_on"])

def load_models(directory):
    models = []
    for f in os.listdir(directory):
        if f.endswith(".sql"):
            models.append(parse_model_file(os.path.join(directory, f)))
    return models

def build_dag(models):
    dag = nx.DiGraph()
    for m in models:
        dag.add_node(m.name, model=m)
        for dep in m.depends_on:
            dag.add_edge(dep, m.name)
    return dag

def execute_model(model, conn):
    ddl = f"CREATE OR REPLACE {'VIEW' if model.materialized == 'view' else 'TABLE'} {model.name} AS {model.sql}"
    with conn.cursor() as cursor:
        cursor.execute(ddl)
