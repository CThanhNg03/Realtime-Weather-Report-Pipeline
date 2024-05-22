-- Create the weather database
CREATE DATABASE IF NOT EXISTS weather_data;

-- Use the weather database
USE weather_data;

-- Create the locations table
CREATE TABLE IF NOT EXISTS locations (
    id INT PRIMARY KEY,
    Name VARCHAR(255),
    Latitude DECIMAL(10, 8),
    Longitude DECIMAL(11, 8),
    Address VARCHAR(255)
);

-- Create the current weather table
CREATE TABLE IF NOT EXISTS current (
    locationID INT,
    time DATETIME,
    temperature_2m DECIMAL(5,2),
    relative_humidity_2m DECIMAL(5,2),
    dew_point_2m DECIMAL(5,2),
    apparent_temperature DECIMAL(5,2),
    precipitation_probability DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    rain DECIMAL(5,2),
    showers DECIMAL(5,2),
    snowfall DECIMAL(5,2),
    snow_depth DECIMAL(5,2),
    cloud_cover DECIMAL(5,2),
    visibility INT,
    wind_speed_10m DECIMAL(5,2),
    wind_direction_10m INT,
    wind_gusts_10m DECIMAL(5,2),
    uv_index INT,
    uv_index_clear_sky INT,
    is_day INT,
    sunshine_duration INT,
    PRIMARY KEY (locationID, time)
);

-- Create table for daily forcast weather data
CREATE TABLE daily (
    id INT AUTO_INCREMENT PRIMARY KEY,
    temperature_2m_max DECIMAL(5, 2),
    temperature_2m_min DECIMAL(5, 2),
    apparent_temperature_max DECIMAL(5, 2),
    apparent_temperature_min DECIMAL(5, 2),
    sunrise TIME,
    sunset TIME,
    time DATETIME,
    locationID INT,
    time_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the current_2 weather table
CREATE TABLE IF NOT EXISTS current_2 (
    locationID INT,
    time DATETIME,
    temperature_2m DECIMAL(5,2),
    relative_humidity_2m DECIMAL(5,2),
    dew_point_2m DECIMAL(5,2),
    apparent_temperature DECIMAL(5,2),
    precipitation_probability DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    rain DECIMAL(5,2),
    showers DECIMAL(5,2),
    snowfall DECIMAL(5,2),
    snow_depth DECIMAL(5,2),
    cloud_cover DECIMAL(5,2),
    visibility INT,
    wind_speed_10m DECIMAL(5,2),
    wind_direction_10m INT,
    wind_gusts_10m DECIMAL(5,2),
    uv_index INT,
    uv_index_clear_sky INT,
    is_day INT,
    sunshine_duration INT,
    PRIMARY KEY (locationID, time)
);

-- Create table for daily_2 forcast weather data
CREATE TABLE daily_2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    temperature_2m_max DECIMAL(5, 2),
    temperature_2m_min DECIMAL(5, 2),
    apparent_temperature_max DECIMAL(5, 2),
    apparent_temperature_min DECIMAL(5, 2),
    sunrise TIME,
    sunset TIME,
    time DATETIME,
    locationID INT,
    time_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
