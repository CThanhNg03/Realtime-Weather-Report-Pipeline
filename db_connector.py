import mysql.connector
import csv
import psycopg2

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
                print("Connected to MySQL database")
                return conn
        except mysql.connector.Error as e:
            print("Error connecting to MySQL:", e)
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
                print("Connected to PostgreSQL database")
                return conn
        except psycopg2.Error as e:
            print("Error connecting to PostgreSQL:", e)
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
            print("Create database schema successfully!")

        except mysql.connector.Error as e:
            print("Error creating database schema in mysql:", str(e))
            conn.close()
        except psycopg2.Error as e:
            print("Error creating database schema in postgresql:", str(e))
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
            print("Insert locations successfully!")

        except mysql.connector.Error as e:
            print("Error inserting location data in mysql:", str(e))
        except psycopg2.Error as e:
            print("Error inserting location data in postgresql:", str(e))



# db_connector = Db_connector()
# conn = db_connector.connect_to_mysql()

# if conn:
#     conn.close()
#     print("Connection closed")
