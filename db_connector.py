import mysql.connector

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
        pass

    def create_sql_db(self, conn):
        try:
            cursor = conn.cursor()

            # Read the SQL script to create the database
            with open("sql_scripts.sql", "r") as file:
                sql_script = file.read()
            
            cursor.execute(sql_script)

            # Check for warnings
            for warning in cursor.fetchwarnings():
                print("Warning:", warning[2])
                return
            
            # Load location data into db if not exist
            with open(csv_file, "r") as file:
                    next(file)  # Skip header row
                    csv_data = [line.strip().split(",") for line in file]
            insert_query = """
                INSERT INTO locations (Name, Latitude, Longitude, Address)
                VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(insert_query, csv_data)

            # Commit the transaction
            conn.commit()
            cursor.close()

        except mysql.connector.Error as e:
            print("Error:", e)


# db_connector = Db_connector()
# conn = db_connector.connect_to_mysql()

# if conn:
#     conn.close()
#     print("Connection closed")
