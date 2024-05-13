import mysql.connector

class DataProcessor:
    def __init__(self):
        pass

    def load_data_to_sqldb(self, conn, data):
        try: 
            conn.reconnect()
            cursor = conn.cursor()
            cursor.execute("USE weather_data;")
            
            # Extract relevant fields from JSON and insert into 'current' table
            insert_query = """
                INSERT INTO current (
                    locationID, time, temperature_2m, relative_humidity_2m, dew_point_2m, 
                    apparent_temperature, precipitation_probability, precipitation, rain, 
                    showers, snowfall, snow_depth, cloud_cover, visibility, wind_speed_10m, 
                    wind_direction_10m, wind_gusts_10m, uv_index, uv_index_clear_sky, 
                    is_day, sunshine_duration
                ) VALUES (
                    %(id)s, %(time)s, %(temperature_2m)s, %(relative_humidity_2m)s, 
                    %(dew_point_2m)s, %(apparent_temperature)s, %(precipitation_probability)s, 
                    %(precipitation)s, %(rain)s, %(showers)s, %(snowfall)s, %(snow_depth)s, 
                    %(cloud_cover)s, %(visibility)s, %(wind_speed_10m)s, %(wind_direction_10m)s, 
                    %(wind_gusts_10m)s, %(uv_index)s, %(uv_index_clear_sky)s, %(is_day)s, 
                    %(sunshine_duration)s
                )
            """
            cursor.execute(insert_query, data['current'])

            # Commit the transaction
            conn.commit()
            cursor.close()
        except mysql.connector.Error as e:
            print("Error while inserting:", e)