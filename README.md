📌 **On prem Table migration on BigQuery**
This project houses the on premises table migration tool on BigQuery written in Python.
In particolar, the actual modules allow to migrate table (DDL) from Postgres and Mysql on Biguqery.


🚀 **Features**
✔️ Onprem Extraction - extraction of DDL on Prem from a database configuration json file
✔️ GCP Interaction for SQL Translation - GCP interaction with API for SQL Translation
✔️ Deploy DDL on Big - Tables deploy after pon BigQuery after the translation


🛠️ **Technologies used**
[✔️] Language: Python / SQL
[✔️] Database: PostgreSQL / MySQL 


📦 **Installing**

    **Clone repo**
    git clone https://github.com/LuigiSciarretta/table-migration-bigquery.git

    **Switch folder**
    cd table-migration-bigquery

    #Install the dependencies in your env
    pip install -r requirements.txt  

📄 **Short guide**
The project is divided into several "main" python ones that deal with separate operations. There is no single centralized main.
The different .py files call modules defined in src that take care of DDL extraction from on prem systems and interaction with Google Cloud Platform via Python API.
The flow involves the following scripts in order:
- onprem_extraction.py
- upload_gcp.py
- sql_translation_task.py
- download.py
- execute_ddl
The first script needs to be passed db_config.json as a parameter, which defines the connection parameters to the on prem DBs.
Subsequent scripts need the bq_config.json as a parameter, which defines the authentication and info needed for interaction with GCP.

