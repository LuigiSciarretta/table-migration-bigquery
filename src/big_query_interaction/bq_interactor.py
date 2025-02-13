from google.cloud import bigquery_migration_v2
from google.cloud import storage
from google.cloud import bigquery
import os
import re
import time


def set_credential(credential):
    # Imposto le credenziali
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
    
    # BigQuery Migration client 
    client = bigquery_migration_v2.MigrationServiceClient()

    # Caso MySQL
    if origin_dialect.lower() == 'mysql':
        # definisco dialetto sorgente come MySQL SQL
        source_dialect = bigquery_migration_v2.Dialect()
        source_dialect.mysql_dialect = bigquery_migration_v2.MySQLDialect()
        
        # definisco dialetto destinazione come BigQuery SQL
        target_dialect = bigquery_migration_v2.Dialect()
        target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()    
        
        # definisco i dettagli di configurazione 
        translation_config = bigquery_migration_v2.TranslationConfigDetails(
            gcs_source_path=gcs_input_path,
            gcs_target_path=gcs_output_path,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # definisco il task di traduzione
        migration_task = bigquery_migration_v2.MigrationTask(type_="Translation_MySQL2BQ", translation_config_details=translation_config)

        # definisco il workflow di traduzione
        workflow = bigquery_migration_v2.MigrationWorkflow(display_name="Traduzione-MySQL2BQ")

    # Caso PostgreSQL
    elif origin_dialect.lower() == 'postgresql':
        # definisco dialetto sorgente come  PostgreSQL SQL
        source_dialect = bigquery_migration_v2.Dialect()
        source_dialect.postgresql_dialect = bigquery_migration_v2.PostgresqlDialect()
        
        # # definisco dialetto destinazione come BigQuery SQL
        target_dialect = bigquery_migration_v2.Dialect()
        target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()
        
        # definisco i dettagli di configurazione 
        translation_config = bigquery_migration_v2.TranslationConfigDetails(
            gcs_source_path=gcs_input_path,
            gcs_target_path=gcs_output_path,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # definisco il task di traduzione
        migration_task = bigquery_migration_v2.MigrationTask(type_="Translation_Postgres2BQ", translation_config_details=translation_config)

        # definisco il workflow di traduzione
        workflow = bigquery_migration_v2.MigrationWorkflow(display_name="Traduzione-Postgres2BQ")

    else:
        raise ValueError("Dialect non supportato. Usa 'mysql' o 'postgresql' come origine.")

    workflow.tasks["translation-task"] = migration_task  

    # Preparo l'API request per creare il migration workflow
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,
    )

    response = client.create_migration_workflow(request=request)

    print("Created workflow:")
    print(response.display_name)

    # Continuo a controllare lo stato del workflow fino a quando non è completato
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



# Funzione per creare un dataset se non esiste
def create_dataset(dataset_name, project_id):
    client = bigquery.Client()
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)  # Controlla se il dataset esiste
        print(f"Dataset '{dataset_name}' esiste già.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"  # Specifica la location del dataset
        client.create_dataset(dataset, exists_ok=True)
        print(f"Creato il dataset '{dataset_name}'.")



def execute_ddl(ddl_content, project_id):

    
    # Inizializza il client BigQuery
    client = bigquery.Client()

    # Estrai i nomi dei dataset e le istruzioni di creazione
    dataset_table_pattern = re.compile(r"CREATE TABLE (\w+)\.(\w+)")
    matches = dataset_table_pattern.findall(ddl_content)

    # Ottieni i dataset unici
    datasets = set(match[0] for match in matches)

    # Crea i dataset mancanti
    for dataset in datasets:
        print("dataset:", dataset)
        create_dataset(dataset, project_id)

    # Esegui le istruzioni DDL
    ddl_statements = ddl_content.split(";")
    for statement in ddl_statements:
        statement = statement.strip()
        if statement:  # Evita statement vuoti
            try:
                query_job = client.query(statement)
                query_job.result()  # Attende il completamento
                print(f"Eseguita la query: {statement[:50]}...")
            except Exception as e:
                print(f"Errore durante l'esecuzione della query: {statement[:50]}...")
                print(str(e))