from src.big_query_interaction import bq_interactor as gcp
import json
import sys



if __name__ == "__main__":
    # leggo i parametri da linea di comando
    config = sys.argv[1]

    # leggo il file di configurazione JSON
    with open(config, 'r') as file:
        config_file = json.load(file)
    
    # imposto i parametri necessari
    sa = config_file['general']['json_auth']
    project_id = config_file['general']['project_id']
    on_prem_db = config_file.get("translation_task", {})

    if 'mysql' in on_prem_db:
        gcs_input_path = on_prem_db['mysql']['gcs_input_path']
        gcs_output_path = on_prem_db['mysql']['gcs_output_path']
        origin_dialect = on_prem_db['mysql']['origin_dialect']

    
    # autenticazione tramite service account
    gcp.set_credential(sa)

    # avvio il task di traduzione (in questo caso solo per mysql)
    workflow_state = gcp.create_full_migration_workflow(gcs_input_path, gcs_output_path, project_id, origin_dialect)

    if workflow_state == gcp.bigquery_migration_v2.MigrationWorkflow.State.COMPLETED:
        print("Migration workflow completed successfully")
    else:
        print("Migration workflow did not complete successfully")
    
    



    