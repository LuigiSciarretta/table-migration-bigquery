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
    local_folder_path = config_file['upload_folder']['local_folder_path']
    gcs_folder_destination = config_file['upload_folder']['gcs_folder_destination']

    # autenticazione tramite service account
    gcp.set_credential(sa)

    # carico cartella contenente script DDL sul bucket
    gcp.upload_all_folder_to_gcs_with_subfolder_param(bucket_name, local_folder_path, gcs_folder_destination)
    