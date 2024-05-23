import mysql.connector
import csv
import psycopg2
import time

class DatabaseConnector:
    def __init__(self):
        pass

    def connect_to_mysql(self):
        try:
            conn = mysql.connector.connect(
                host="35.198.235.20",
                user="root",
                password="123456a@",
                ssl_disabled=False
            )
            if conn.is_connected():
                log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Connected to MySQL database"
                print(log_message)
                return conn
        except mysql.connector.Error as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error connecting to MySQL: {str(e)}"
            print(log_message)
            return None
    
    def connect_to_sqlserver(self):
        pass

    def connect_to_postgresql(self):
        try :
            conn = psycopg2.connect(
                host="34.143.211.214",
                user="postgres",
                password="123456a@",
                sslmode="require",
                dbname="weather_data"
            )
            with conn:
                log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Connected to PostgreSQL database"
                print(log_message)
                return conn
        except psycopg2.Error as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error connecting to PostgreSQL: {str(e)}"
            print(log_message)
            return None
        pass

    def create_database_schema(self, conn, db = "mysql"):
        try:
            cursor = conn.cursor()
            if db == "mysql":
                sql_file = "mysql_scripts.sql"
            elif db == "postgresql":
                sql_file = "postgresql_scripts.sql"
            # Read the SQL script to create the database
            with open(sql_file, "r") as file:
                sql_script = file.read()

            cursor.execute(sql_script)
            cursor.close()
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Created database schema successfully!"
            print(log_message)

        except mysql.connector.Error as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error creating database schema in MySQL: {str(e)}"
            print(log_message)
            conn.close()
        except psycopg2.Error as e:
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error creating database schema in PostgreSQL: {str(e)}"
            print(log_message)
            conn.close()


    def insert_location_data(self, conn, db = "mysql"):
        try:
            conn.reconnect()
            cursor = conn.cursor()
            if db == "mysql":
                cursor.execute("USE weather_data;")
            elif db == "postgresql":
                cursor.execute("SET search_path TO weather_data;")

            # Load location data into db if not exist
            with open("geolocation.csv", "r", encoding="utf-8") as file:
                csv_reader = csv.reader(file)
                next(csv_reader)  # Skip header row
                csv_data = [(row[0], float(row[1]), float(row[2]), row[3], int(row[4])) for row in csv_reader]
            if db == "mysql":
                insert_query = """
                    INSERT INTO locations (Name, Latitude, Longitude, Address, id)
                    VALUES (%s, %s, %s, %s, %s)
                """
            elif db == "postgresql":
                insert_query = """
                    INSERT INTO locations (Name, Latitude, Longitude, Address, id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """

            cursor.executemany(insert_query, csv_data)

            # Commit the transaction
            conn.commit()
            cursor.close()
            log_message = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Insert locations successfully!"
            print(log_message)

        except mysql.connector.Error as e:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error inserting location data in mysql:", str(e))
        except psycopg2.Error as e:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - Error inserting location data in postgresql:", str(e))



# db_connector = Db_connector()
# conn = db_connector.connect_to_mysql()

# if conn:
#     conn.close()
#     print("Connection closed")
