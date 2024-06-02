-- Load the datasets
dataset_gps_mpu_left = LOAD '/user/hdfs/dataset_gps_mpu_left.csv' USING PigStorage(',') AS (
    timestamp: double,
    acc_x_dashboard: double,
    acc_y_dashboard: double,
    acc_z_dashboard: double,
    acc_x_above_suspension: double,
    acc_y_above_suspension: double,
    acc_z_above_suspension: double,
    acc_x_below_suspension: double,
    acc_y_below_suspension: double,
    acc_z_below_suspension: double,
    gyro_x_dashboard: double,
    gyro_y_dashboard: double,
    gyro_z_dashboard: double,
    gyro_x_above_suspension: double,
    gyro_y_above_suspension: double,
    gyro_z_above_suspension: double,
    gyro_x_below_suspension: double,
    gyro_y_below_suspension: double,
    gyro_z_below_suspension: double,
    mag_x_dashboard: double,
    mag_y_dashboard: double,
    mag_z_dashboard: double,
    mag_x_above_suspension: double,
    mag_y_above_suspension: double,
    mag_z_above_suspension: double,
    temp_dashboard: double,
    temp_above_suspension: double,
    temp_below_suspension: double,
    timestamp_gps: double,
    latitude: double,
    longitude: double,
    speed: double
);

dataset_labels = LOAD '/user/hdfs/dataset_labels.csv' USING PigStorage(',') AS (
    timestamp: double,
    latitude: double,
    longitude: double,
    elevation: double,
    accuracy: double,
    bearing: double,
    speed_meters_per_second: double,
    satellites: int,
    provider: chararray,
    hdop: double,
    vdop: double,
    pdop: double,
    geoidheight: double,
    ageofdgpsdata: double,
    dgpsid: double,
    activity: double,
    battery: int,
    annotation: double,
    distance_meters: double,
    elapsed_time_seconds: double
);

-- Extract average speed by route segments (latitude, longitude)
grouped_gps_mpu_left = GROUP dataset_gps_mpu_left BY (latitude, longitude);
avg_speed_by_segment = FOREACH grouped_gps_mpu_left GENERATE
    group.latitude as latitude,
    group.longitude as longitude,
    AVG(dataset_gps_mpu_left.speed) as avg_speed;

-- Extract average sensor readings
avg_sensor_readings = FOREACH (GROUP dataset_gps_mpu_left ALL) GENERATE
    AVG(dataset_gps_mpu_left.acc_x_dashboard) as avg_acc_x_dashboard,
    AVG(dataset_gps_mpu_left.acc_y_dashboard) as avg_acc_y_dashboard,
    AVG(dataset_gps_mpu_left.acc_z_dashboard) as avg_acc_z_dashboard,
    AVG(dataset_gps_mpu_left.acc_x_above_suspension) as avg_acc_x_above_suspension,
    AVG(dataset_gps_mpu_left.acc_y_above_suspension) as avg_acc_y_above_suspension,
    AVG(dataset_gps_mpu_left.acc_z_above_suspension) as avg_acc_z_above_suspension,
    AVG(dataset_gps_mpu_left.acc_x_below_suspension) as avg_acc_x_below_suspension,
    AVG(dataset_gps_mpu_left.acc_y_below_suspension) as avg_acc_y_below_suspension,
    AVG(dataset_gps_mpu_left.acc_z_below_suspension) as avg_acc_z_below_suspension,
    AVG(dataset_gps_mpu_left.gyro_x_dashboard) as avg_gyro_x_dashboard,
    AVG(dataset_gps_mpu_left.gyro_y_dashboard) as avg_gyro_y_dashboard,
    AVG(dataset_gps_mpu_left.gyro_z_dashboard) as avg_gyro_z_dashboard,
    AVG(dataset_gps_mpu_left.gyro_x_above_suspension) as avg_gyro_x_above_suspension,
    AVG(dataset_gps_mpu_left.gyro_y_above_suspension) as avg_gyro_y_above_suspension,
    AVG(dataset_gps_mpu_left.gyro_z_above_suspension) as avg_gyro_z_above_suspension,
    AVG(dataset_gps_mpu_left.gyro_x_below_suspension) as avg_gyro_x_below_suspension,
    AVG(dataset_gps_mpu_left.gyro_y_below_suspension) as avg_gyro_y_below_suspension,
    AVG(dataset_gps_mpu_left.gyro_z_below_suspension) as avg_gyro_z_below_suspension,
    AVG(dataset_gps_mpu_left.mag_x_dashboard) as avg_mag_x_dashboard,
    AVG(dataset_gps_mpu_left.mag_y_dashboard) as avg_mag_y_dashboard,
    AVG(dataset_gps_mpu_left.mag_z_dashboard) as avg_mag_z_dashboard,
    AVG(dataset_gps_mpu_left.mag_x_above_suspension) as avg_mag_x_above_suspension,
    AVG(dataset_gps_mpu_left.mag_y_above_suspension) as avg_mag_y_above_suspension,
    AVG(dataset_gps_mpu_left.mag_z_above_suspension) as avg_mag_z_above_suspension,
    AVG(dataset_gps_mpu_left.temp_dashboard) as avg_temp_dashboard,
    AVG(dataset_gps_mpu_left.temp_above_suspension) as avg_temp_above_suspension,
    AVG(dataset_gps_mpu_left.temp_below_suspension) as avg_temp_below_suspension;

-- Store the results
STORE avg_speed_by_segment INTO '/user/hdfs/output/avg_speed_by_segment' USING PigStorage(',');
STORE avg_sensor_readings INTO '/user/hdfs/output/avg_sensor_readings' USING PigStorage(',');

