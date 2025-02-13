#from google.cloud import bigquery
from src.big_query_interaction import bq_interactor as gcp
import json
import sys



if __name__ == '__main__':
    # leggo i parametri da linea di comando
    config = sys.argv[1]

    # leggo il file di configurazione
    with open(config, 'r') as file:
        config_file = json.load(file)
    
    # imposto i parametri necessari
    sa = config_file['general']['json_auth']
    bucket_name = config_file['general']['bucket_name']
    project_id = config_file['general']['project_id']

    on_prem_ddl = config_file.get("execute_ddl", {})
    if 'ddl_postgres_path' in on_prem_ddl:
        file_path_pgres = config_file['execute_ddl']['ddl_postgres_path']
    if 'ddl_mysql_path' in on_prem_ddl:
        file_path_mysql = config_file['execute_ddl']['ddl_mysql_path']


    # Autentizazione tramite service account
    gcp.set_credential(sa)

    # leggo le DDL 
    with open(file_path_pgres, "r") as ddl_file:
        ddl_content = ddl_file.read()
    # eseguo il deploy su BigQuery delle DDL Postgres
    gcp.execute_ddl(ddl_content, project_id)

    # leggo le DDL 
    with open(file_path_mysql, "r") as ddl_file:
        ddl_content = ddl_file.read()
    # eseguo il deploy su BigQuery delle DDL Postgres
    gcp.execute_ddl(ddl_content, project_id)
    
