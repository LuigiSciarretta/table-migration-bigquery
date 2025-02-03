from google.cloud import bigquery
from src.big_query_interaction import bq_interactor as gcp
import re
import json
import sys



if __name__ == '__main__':
    config = sys.argv[1]

    with open(config, 'r') as file:
        config_file = json.load(file)
    
    sa = config_file['general']['json_auth']
    bucket_name = config_file['general']['bucket_name']
    project_id = config_file['general']['project_id']

    on_prem_ddl = config_file.get("execute_ddl", {})
    if 'ddl_postgres_path' in on_prem_ddl:
        file_path = config_file['execute_ddl']['ddl_postgres_path']

    #
    gcp.set_credential(sa)

    with open(file_path, "r") as ddl_file:
        ddl_content = ddl_file.read()
    
    gcp.execute_ddl(ddl_content, project_id)
    
