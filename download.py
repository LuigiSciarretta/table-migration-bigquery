from src.big_query_interaction import bq_interactor as gcp
import sys
import json


if __name__ == "__main__":
    # leggo i parametri da linea di comando
    config = sys.argv[1]

    # leggo il file di configurazione JSON
    with open(config, 'r') as file:
        config_file = json.load(file)
    
    # imposto i parametri necessari
    sa = config_file['general']['json_auth']
    bucket_name = config_file['general']['bucket_name']
    
    bucket_folder_path = config_file['download_folder']['bucket_folder_path']
    local_destination_path = config_file['download_folder']['local_destination_path']


    # autenticazione tramite service account
    gcp.set_credential(sa)

    # scarico localmente gli script DDL tradotti 
    gcp.download_sql_files_on_destination(bucket_name, bucket_folder_path, local_destination_path)
    
