# 📌 On-Prem Table Migration to BigQuery
This project contains a tool for migrating on-premises tables to BigQuery, written in Python.
In particular, the current modules allow the migration of table DDLs from PostgreSQL and MySQL to BigQuery.

# 🚀 Features
✔️ On-Prem Extraction - Extracts DDLs from an on-premises database using a configuration JSON file.

✔️ GCP SQL Translation - Interacts with Google Cloud APIs to translate SQL queries.

✔️ DDL Deployment on BigQuery - Deploys translated tables to BigQuery.

# 🛠️ Technologies Used

[✔️] Language: Python / SQL

[✔️] Databases: PostgreSQL / MySQL

# 📦 **Installation**

    # Clone repo
    git clone https://github.com/LuigiSciarretta/table-migration-bigquery.git

    # Switch folder
    cd table-migration-bigquery

    # Install dependencies in your virtual env
    pip install -r requirements.txt  

# 📄 Quick Guide

The project consists of multiple Python scripts, each handling a separate operation. There is no single centralized main script.
The .py files invoke modules located in the src directory, which handle DDL extraction from on-premises databases and interaction with Google Cloud Platform via its Python API.

The workflow follows this sequence of scripts:

- 1️⃣ onprem_extraction.py
- 2️⃣ upload_gcp.py
- 3️⃣ sql_translation_task.py
- 4️⃣ download.py
- 5️⃣ execute_ddl.py

The first script requires db_config.json as a parameter, which contains connection details for the on-premises databases.
The subsequent scripts require bq_config.json, which contains authentication details and configuration settings for interacting with GCP.

