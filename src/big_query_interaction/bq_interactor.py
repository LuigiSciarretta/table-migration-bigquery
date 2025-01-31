from google.cloud import bigquery_migration_v2
from google.cloud import storage
import os
import time


def set_credential(credential):
    # Imposta le credenziali
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential


def upload_all_folder_to_gcs_with_subfolder_param(bucket_name, local_folder_path, gcs_folder_path, specific_subfolder=None):
    """ 
    Funzione che ricrea su un bucket l'alberatura locale con eventuali file in un bucket GCS. 
    Possibilità di specificare una sottocartella. E' un evoluzione della funzione upload_all_folder_subfolder_and_files_to_gcs
    """
    # Creo un client GCS
    storage_client = storage.Client()

    # Riferimento al bucket GCS
    bucket = storage_client.bucket(bucket_name)

    # Percorso specifico della sottocartella, se specificato
    if specific_subfolder:
        local_folder_path = os.path.join(local_folder_path, specific_subfolder)

    # Carico ogni file nella cartella locale e nelle sottocartelle nel bucket GCS
    for dirpath, dirnames, filenames in os.walk(local_folder_path):
        for dirname in dirnames:
            # Creazione delle sottocartelle nel bucket GCS
            local_dir_path = os.path.join(dirpath, dirname)
            relative_path = os.path.relpath(local_dir_path, local_folder_path)
            gcs_dir_path = os.path.join(gcs_folder_path, relative_path)
            gcs_dir_path = gcs_dir_path.replace("\\", "/")
            blob = bucket.blob(gcs_dir_path + "/")  
            if not blob.exists():
                blob.upload_from_string('')
                print(f"Sottocartella {dirname} creata con successo in {gcs_dir_path}")

        for file_name in filenames:
            local_file_path = os.path.join(dirpath, file_name)
            relative_path = os.path.relpath(local_file_path, local_folder_path)
            gcs_file_path = os.path.join(gcs_folder_path, relative_path)
            gcs_file_path = gcs_file_path.replace("\\", "/")

            blob = bucket.blob(gcs_file_path)

            # check se file è già presente nel bucket
            if blob.exists():
                print(f"Il file {file_name} è già presente nel bucket {bucket_name}.")
            else:
                blob.upload_from_filename(local_file_path)
                print(f"File {file_name} caricato con successo in {gcs_file_path}")






def create_full_migration_workflow(
    gcs_input_path: str, gcs_output_path: str, project_id: str, origin_dialect: str
) -> None:
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
    
    parent = f"projects/{project_id}/locations/eu"
    
    # Construct a BigQuery Migration client object.
    client = bigquery_migration_v2.MigrationServiceClient()

    # Caso per MySQL
    if origin_dialect.lower() == 'mysql':
        # Set the source dialect to MySQL SQL.
        source_dialect = bigquery_migration_v2.Dialect()
        source_dialect.mysql_dialect = bigquery_migration_v2.MySQLDialect()
        
        # Set the target dialect to BigQuery dialect.
        target_dialect = bigquery_migration_v2.Dialect()
        target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()    
        
        # Prepare the config proto.
        translation_config = bigquery_migration_v2.TranslationConfigDetails(
            gcs_source_path=gcs_input_path,
            gcs_target_path=gcs_output_path,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # Prepare the task.
        migration_task = bigquery_migration_v2.MigrationTask(type_="Translation_MySQL2BQ", translation_config_details=translation_config)

        # Prepare the workflow.
        workflow = bigquery_migration_v2.MigrationWorkflow(display_name="Traduzione-MySQL2BQ")

    # Caso per PostgreSQL
    elif origin_dialect.lower() == 'postgresql':
        # Set the source dialect to PostgreSQL SQL.
        source_dialect = bigquery_migration_v2.Dialect()
        source_dialect.postgresql_dialect = bigquery_migration_v2.PostgresqlDialect()
        
        # Set the target dialect to BigQuery dialect.
        target_dialect = bigquery_migration_v2.Dialect()
        target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()
        
        # Prepare the config proto.
        translation_config = bigquery_migration_v2.TranslationConfigDetails(
            gcs_source_path=gcs_input_path,
            gcs_target_path=gcs_output_path,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # Prepare the task.
        migration_task = bigquery_migration_v2.MigrationTask(type_="Translation_Postgres2BQ", translation_config_details=translation_config)

        # Prepare the workflow.
        workflow = bigquery_migration_v2.MigrationWorkflow(display_name="Traduzione-Postgres2BQ")

    else:
        raise ValueError("Dialect non supportato. Usa 'mysql' o 'postgresql' come origine.")

    workflow.tasks["translation-task"] = migration_task  # type: ignore

    # Prepare the API request to create a migration workflow.
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,
    )

    response = client.create_migration_workflow(request=request)

    print("Created workflow:")
    print(response.display_name)

    # Continua a controllare lo stato del workflow fino a quando non è completato
    while True:
        workflow = client.get_migration_workflow(name=response.name)
        print("Current state:")
        print(workflow.state)

        if workflow.state == bigquery_migration_v2.MigrationWorkflow.State.COMPLETED:
            break

        # Aspetta un po' prima di controllare di nuovo lo stato
        time.sleep(2.5)
    
    return workflow.state





def download_folder_subfolder_check_file_exists(bucket_name, subfolder_name, destination_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista di tutti i blob nel bucket con un determinato prefisso (sottocartella)
    blobs = bucket.list_blobs(prefix=subfolder_name)

    for blob in blobs:
        # Calcola il percorso di destinazione locale del file
        destination_file_name = os.path.join(destination_folder, blob.name.replace(subfolder_name, '', 1).lstrip('/'))

        # Creo le sottocartelle se non esistono
        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)

        #check se file è già presente nella cartella
        if os.path.exists(destination_file_name):
            print(f"Il file {destination_file_name} è già presente nella cartella {destination_folder}.")
        else:
            try:
                # Scarica il blob nel file locale
                with open(destination_file_name, "wb") as file_obj:
                    blob.download_to_file(file_obj)
            except PermissionError as e:
                print(f"Errore di permessi: {e}")





def download_sql_files_on_destination(bucket_name, subfolder_name, destination_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista di tutti i blob nel bucket con un determinato prefisso (sottocartella)
    blobs = bucket.list_blobs(prefix=subfolder_name)

    for blob in blobs:
        # Estrai solo il nome del file, ignorando le sottocartelle
        file_name = os.path.basename(blob.name)
        destination_file_name = os.path.join(destination_folder, file_name)

        if file_name.endswith('.sql'):
            # Controlla se il file esiste già
            if os.path.exists(destination_file_name):
                print(f"Il file {file_name} è già presente nella cartella {destination_folder}.")
            else:
                try:
                    # Scarica il blob nel file locale
                    with open(destination_file_name, "wb") as file_obj:
                        blob.download_to_file(file_obj)
                    print(f"Scaricato: {file_name}")
                except PermissionError as e:
                    print(f"Errore di permessi: {e}")
