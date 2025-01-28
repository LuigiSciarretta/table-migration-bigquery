from src.postgres_extractor.postgres_ddl_extractor import PostgresExtractor as pg
from src.mysql_extractor.mysql_ddl_extractor import MySQlExtractor as ms
import json
import sys

if __name__ == '__main__':
    config_file = sys.argv[1]
    saving_ddl_path = sys.argv[2]
    
    #leggo json di configurazione
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Configuro i dettagli di connessione
    DATABASE = config["database"]      # Sostituisci con il nome del tuo database
    USER = config["user"]            # Sostituisci con il nome dell'utente del database
    PASSWORD = config["password"]       # Sostituisci con la password dell'utente
    HOST = config["host"]             # Usa 'localhost' per connetterti al container
    PORT = config["port"]                  # Porta esposta dal container PostgreSQL

    #estrazione pgres
    pgres = pg(DATABASE, USER, PASSWORD, HOST, PORT)
    pgres.create_connection()
    ddls = pgres.estrai_ddl_from_query()
    pgres.close_connection()
    pg.save_ddl(saving_ddl_path, ddls)
    pg.save_ddl_single_file(saving_ddl_path, ddls)

    # #estrazione mysql
    # mysql = ms(USER, PASSWORD, HOST, PORT)
    # mysql.create_connection()
    # ddls = mysql.extract_ddl(DATABASE)
    # mysql.close_connection()
    # #ms.save_ddl(saving_ddl_path, ddls)
    # ms.save_ddl_single_file(saving_ddl_path, ddls)