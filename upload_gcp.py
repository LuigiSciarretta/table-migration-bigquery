from src.big_query_interaction import bq_interactor as gcp
import sys
import json

if __name__ == "__main__":

    config = sys.argv[1]

    with open(config, 'r') as file:
        config_file = json.load(file)
    
    sa = config_file['general']['json_auth']
    bucket_name = config_file['general']['bucket_name']
    local_folder_path = config_file['upload_folder']['local_folder_path']
    gcs_folder_destination = config_file['upload_folder']['gcs_folder_destination']


    # Imposta il service account contenuto nel file di configurazione
    gcp.set_credential(sa)
    # Carico cartella sul bucket
    gcp.upload_all_folder_to_gcs_with_subfolder_param(bucket_name, local_folder_path, gcs_folder_destination)
    