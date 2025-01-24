import psycopg2
import subprocess
import re
import os

class PostgresExtractor:

    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
    

    def create_connection(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Database connection: SUCCESS")
        except Exception as e:
            print(f"Error during connection: {e}")
            raise
    

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection: CLOSED")

    
    def estrai_ddl_from_dump(self) -> dict: 
        final_dump = {}
        try:
            if not self.connection:
                raise ValueError("There is no connection to db.")

            cursor = self.connection.cursor()
            # Recupera tutte le tabelle nel schema 'public'
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
            """)
            tabelle = [row[0] for row in cursor.fetchall()]
            #print("Tabelle trovate:")
            #for tabella in tabelle:
            #    print(f"- {tabella}")

            #print("\nDDL delle tabelle:")
            for tabella in tabelle:
                #print(f"\n-- DDL per {tabella} --")
                # Usa pg_dump per ottenere la DDL della tabella
                comando = [
                    "pg_dump",
                    "-h", self.host,          # Host (ad esempio 'localhost' per il container)
                    "-p", str(self.port),     # Porta (ad esempio 5432)
                    "-U", self.user,          # Utente del database
                    "-d", self.database,      # Nome del database
                    "-t", f"public.{tabella}",  # Specifica la tabella
                    "--schema-only"      # Solo schema (DDL)
                ]

                # Esegui il comando pg_dump
                risultato = subprocess.run(
                    comando,
                    text=True,
                    capture_output=True,
                    env={"PGPASSWORD": self.password}  # Variabile d'ambiente per la password
                )
                if risultato.returncode == 0:
                    #print(risultato.stdout)  # Stampa la DDL della tabella
                    final_dump[f"{self.database}.{tabella}"] = str(risultato)
                else:
                    print(f"Error to obtain DDL for {tabella}: {risultato.stderr}")

        except Exception as e:
            print(f"Error: {e}")

        return final_dump



    def estrai_ddl_from_query(self) -> list:

        if not self.connection:
                raise ValueError("There is no connection to db.")

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        tables = [table[0] for table in cursor.fetchall()]
        ddls = []
        for table in tables:
            cursor.execute(f"""
                SELECT 'CREATE TABLE ' || table_name || ' (' || 
                string_agg(
                    column_name || ' ' || 
                    CASE 
                        WHEN data_type IN ('character varying', 'character') THEN 
                            data_type || '(' || character_maximum_length || ')'
                        WHEN data_type IN ('numeric', 'decimal') THEN
                            data_type || '(' || numeric_precision || ',' || numeric_scale || ')'
                        WHEN data_type IN ('double precision', 'real') THEN
                            data_type
                        ELSE
                            data_type
                    END, 
                    ', '
                ) || ');'
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                GROUP BY table_name;
            """)
            ddls.append(cursor.fetchone()[0])
        return ddls


    @staticmethod
    def extract_ddl_from_dump(dump_output) -> list:
        dump_output = str(dump_output.values())
        # Lista per contenere le DDL estratte
        ddl_list = []
        #print("dump output in funzione", dump_output)
        # Espressione regolare per cercare le DDL che iniziano con CREATE TABLE e finiscono con il primo punto e virgola
        pattern = r'(CREATE TABLE[\s\S]+?;)'  # Regex per trovare tutto ciò che inizia con "CREATE TABLE" e finisce con ";"

        # Trova tutte le occorrenze del pattern
        ddl_matches = re.findall(pattern, dump_output, re.IGNORECASE)

        # Aggiungi ogni DDL trovata alla lista
        for ddl in ddl_matches:
            ddl_list.append(ddl)  # Rimuovi gli spazi extra ai lati

        return ddl_list


    @staticmethod
    def save_ddl(saving_path: str, ddl:list):
        folder_path = os.path.join(saving_path, 'postgres_ddl')

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        for i, ddl_postgres in enumerate(ddl):
            filename = os.path.join(folder_path,f"ddl_postgres_{i}.sql")
            with open(filename, 'w') as file:
                file.write(ddl_postgres)
                print(f"file {filename} saved with success")









