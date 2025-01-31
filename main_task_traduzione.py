from src.big_query_interaction import bq_interactor as gcp
import json
import sys



if __name__ == "__main__":

    config = sys.argv[1]

    with open(config, 'r') as file:
        config_file = json.load(file)
    
    sa = config_file['general']['json_auth']
    project_id = config_file['general']['project_id']
    on_prem_db = config_file.get("translation_task", {})

    if 'mysql' in on_prem_db:
        #variabili per la creazione del task di traduzione per mysql
        gcs_input_path = on_prem_db['mysql']['gcs_input_path']
        gcs_output_path = on_prem_db['mysql']['gcs_output_path']
        origin_dialect = on_prem_db['mysql']['origin_dialect']

    
    # Imposta il service account contenuto nel file di configurazione
    gcp.set_credential(sa)

    # Avvio il task di traduzione
    workflow_state = gcp.create_full_migration_workflow(gcs_input_path, gcs_output_path, project_id, origin_dialect)

    if workflow_state == gcp.bigquery_migration_v2.MigrationWorkflow.State.COMPLETED:
        print("Migration workflow completed successfully")
    else:
        print("Migration workflow did not complete successfully")
    
    



    