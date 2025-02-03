from src.big_query_interaction import bq_interactor as gcp
import sys
import json


if __name__ == "__main__":

    config = sys.argv[1]

    with open(config, 'r') as file:
        config_file = json.load(file)
    
    sa = config_file['general']['json_auth']
    bucket_name = config_file['general']['bucket_name']
    
    bucket_folder_path = config_file['download_folder']['bucket_folder_path']
    local_destination_path = config_file['download_folder']['local_destination_path']


    # Imposto il service account contenuto nel file di configurazione
    gcp.set_credential(sa)

    # Chiamo il metodo di download dei file
    gcp.download_sql_files_on_destination(bucket_name, bucket_folder_path, local_destination_path)
    
