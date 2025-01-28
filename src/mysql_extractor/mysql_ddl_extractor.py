import mysql.connector
from mysql.connector import Error
from collections import defaultdict
import os


class MySQlExtractor:
    
    def __init__(self, user, password, host, port): #database, 
        #self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
    

    def create_connection(self):
        try:
            self.connection = mysql.connector.connect(
            host = self.host,  
            port = self.port,
            #database = self.database               
            user = self.user,        
            password = self.password)

            if self.connection.is_connected():
                print("Database connection: SUCCESS")  
        except Exception as e:
            print(f"Error during connection: {e}")
            raise


    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection: CLOSED")


    def extract_ddl(self, databases: list):
        final_ddls = []
        try:
            if not self.connection:
                raise ValueError("There is no connection to db.")
            cursor = self.connection.cursor()

            #uso il singolo db
            for db in databases:
                cursor.execute(f"USE {db};")
                
                #ottengo le tabelle
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]

                #ottengo le ddl per tabella
                for table in tables:
                    cursor.execute(f"SHOW CREATE TABLE {table}")
                    result = cursor.fetchone()
                    ddl = result[1]
                    #print(ddl)
                    ddl_with_database = ddl.replace(f"CREATE TABLE `{table}`", f"CREATE TABLE `{db}`.`{table}`")
                    final_ddls.append(ddl_with_database)
        
        except Exception as e:
            print(f"Error: {e}")
        return final_ddls



    @staticmethod
    def save_ddl_single_file(saving_path: str, ddl:list):
        folder_path = os.path.join(saving_path, 'mysql_ddl')

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        ddl = [single_ddl + ';' for single_ddl in ddl]
        ddls = "\n\n".join(ddl)

        filename = os.path.join(folder_path,f"ddl_mysql.sql")
        with open(filename, 'w') as file:
            file.write(ddls)
            print(f"file {filename} saved with success")



    @staticmethod
    def save_ddl(saving_path: str, ddl:list):
        folder_path = os.path.join(saving_path, 'mysql_ddl')

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        for i, ddl_mysql in enumerate(ddl):
            filename = os.path.join(folder_path,f"ddl_mysql_{i}.sql")
            with open(filename, 'w') as file:
                file.write(ddl_mysql)
                print(f"file {filename} saved with success")

