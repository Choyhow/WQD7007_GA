import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define column names for the CSV files
column_names_avg_speed = ['latitude', 'longitude', 'avg_speed']
column_names_avg_sensor = [
    'avg_acc_x_dashboard', 'avg_acc_y_dashboard', 'avg_acc_z_dashboard',
    'avg_acc_x_above_suspension', 'avg_acc_y_above_suspension', 'avg_acc_z_above_suspension',
    'avg_acc_x_below_suspension', 'avg_acc_y_below_suspension', 'avg_acc_z_below_suspension',
    'avg_gyro_x_dashboard', 'avg_gyro_y_dashboard', 'avg_gyro_z_dashboard',
    'avg_gyro_x_above_suspension', 'avg_gyro_y_above_suspension', 'avg_gyro_z_above_suspension',
    'avg_gyro_x_below_suspension', 'avg_gyro_y_below_suspension', 'avg_gyro_z_below_suspension',
    'avg_mag_x_dashboard', 'avg_mag_y_dashboard', 'avg_mag_z_dashboard',
    'avg_mag_x_above_suspension', 'avg_mag_y_above_suspension', 'avg_mag_z_above_suspension',
    'avg_temp_dashboard', 'avg_temp_above_suspension', 'avg_temp_below_suspension'
]

# Load the data from URLs with specified column names
avg_speed_by_segment = pd.read_csv('https://raw.githubusercontent.com/Choyhow/WQD7007_GA/main/avg_speed_by_segment.csv', names=column_names_avg_speed)
avg_sensor_readings = pd.read_csv('https://raw.githubusercontent.com/Choyhow/WQD7007_GA/main/avg_sensor_readings.csv', names=column_names_avg_sensor)

# Verify DataFrame for avg_speed_by_segment
print("DataFrame Head (Average Speed by Segment):")
print(avg_speed_by_segment.head())

# Verify DataFrame for avg_sensor_readings
print("DataFrame Head (Average Sensor Readings):")
print(avg_sensor_readings.head())

# Check if DataFrames are empty
if avg_speed_by_segment.empty:
    print("The DataFrame avg_speed_by_segment is empty.")
else:
    print("The DataFrame avg_speed_by_segment is not empty.")

if avg_sensor_readings.empty:
    print("The DataFrame avg_sensor_readings is empty.")
else:
    print("The DataFrame avg_sensor_readings is not empty.")

# Plot the data for average speed by segment
if not avg_speed_by_segment.empty:
    plt.figure(figsize=(10, 6))
    scatter = sns.scatterplot(x='longitude', y='latitude', size='avg_speed', hue='avg_speed', data=avg_speed_by_segment, palette='viridis', legend=False)
    plt.title('Average Speed by Latitude and Longitude')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.colorbar(scatter.collections[0], label='Average Speed (m/s)')
    plt.show()

# Plot the data for average sensor readings
if not avg_sensor_readings.empty:
    avg_sensor_readings_long = pd.melt(avg_sensor_readings)
    plt.figure(figsize=(10, 6))
    sns.barplot(x='variable', y='value', data=avg_sensor_readings_long)
    plt.title('Average Sensor Readings')
    plt.ylabel('Average Value')
    plt.xlabel('Sensor Type')
    plt.xticks(rotation=90)
    plt.show()
