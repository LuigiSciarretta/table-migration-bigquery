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
            # All 'public' tables
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
            """)
            tabelle = [row[0] for row in cursor.fetchall()]

            for tabella in tabelle:
                # pg_dump command
                comando = [
                    "pg_dump",
                    "-h", self.host,          
                    "-p", str(self.port),     
                    "-U", self.user,          
                    "-d", self.database,      
                    "-t", f"public.{tabella}",  
                    "--schema-only"      
                ]

                # pg_dump command
                risultato = subprocess.run(
                    comando,
                    text=True,
                    capture_output=True,
                    env={"PGPASSWORD": self.password}  
                )
                if risultato.returncode == 0:
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
            ddl = cursor.fetchone()[0]
            ddl = ddl.replace(f"CREATE TABLE {table}", f"CREATE TABLE banca.{table}")
            ddls.append(ddl)
        return ddls


    @staticmethod
    def extract_ddl_from_dump(dump_output) -> list:
        dump_output = str(dump_output.values())
        ddl_list = []
        pattern = r'(CREATE TABLE[\s\S]+?;)'  
        ddl_matches = re.findall(pattern, dump_output, re.IGNORECASE)

        for ddl in ddl_matches:
            ddl_list.append(ddl)  
        return ddl_list


    @staticmethod
    def save_ddl_single_file(saving_path: str, ddl:list):
        folder_path = os.path.join(saving_path, 'postgres_ddl')

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        ddls = "\n\n".join(ddl)

        filename = os.path.join(folder_path,f"ddl_postgres.sql")
        with open(filename, 'w') as file:
            file.write(ddls)
            print(f"file {filename} saved with success")


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









