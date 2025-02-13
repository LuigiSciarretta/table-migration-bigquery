from src.postgres_extractor.postgres_ddl_extractor import PostgresExtractor as pg
from src.mysql_extractor.mysql_ddl_extractor import MySQlExtractor as ms
import json
import sys

def extract_postgres(config, saving_ddl_path):
    """
    Estrazione DDL da PostgreSQL
    """
    pgres = pg(config["database"], config["user"], config["password"], config["host"], config["port"])
    try:
        pgres.create_connection()
        ddls = pgres.estrai_ddl_from_query()
        pgres.close_connection()
        #pg.save_ddl(saving_ddl_path, ddls)
        pg.save_ddl_single_file(saving_ddl_path, ddls)
        print("[INFO] Estratte DDL da PostgreSQL e salvate con successo.")
    except Exception as e:
        print(f"[ERRORE] Impossibile estrarre DDL da PostgreSQL: {e}")


def extract_mysql(config, saving_ddl_path):
    """
    Estrazione DDL da MySQL
    """
    mysql = ms(config["user"], config["password"], config["host"], config["port"])
    try:
        mysql.create_connection()
        ddls = mysql.extract_ddl(config["database"])
        mysql.close_connection()
        #ms.save_ddl(saving_ddl_path, ddls)
        ms.save_ddl_single_file(saving_ddl_path, ddls)
        print("[INFO] Estratte DDL da MySQL e salvate con successo.")
    except Exception as e:
        print(f"[ERRORE] Impossibile estrarre DDL da MySQL: {e}")





if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python main.py <config_file> <saving_ddl_path>")
        sys.exit(1)

    # leggo i parametri da linea di comando
    config_file = sys.argv[1]
    saving_ddl_path = sys.argv[2]

    # leggo il file di configurazione JSON
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"[ERRORE] File di configurazione {config_file} non trovato.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERRORE] File di configurazione non valido: {e}")
        sys.exit(1)

    # estraggo le DDL dai databases Onprem
    databases = config.get("databases", {})
    if "postgresql" in databases:
        print("[INFO] Inizio estrazione DDL per PostgreSQL.")
        extract_postgres(databases["postgresql"], saving_ddl_path)

    if "mysql" in databases:
        print("[INFO] Inizio estrazione DDL per MySQL.")
        extract_mysql(databases["mysql"], saving_ddl_path)
