import pandas as pd
import numpy as np
import pathlib as Path
import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

#cars w telemetry
target_cars = {
    "barber000" : ['GR86-002-000'],
    "barber78" : ['GR86-004-78']
}

#target lap params
lap_cols = ['lap', 'vehicle_id', 'timestamp']

#target telemetry params
tel_cols = ['telemetry_name', 'telemetry_value', 'vehicle_id', 'timestamp']
target_tel = ['gear', 'speed', 'nmot']

#returns dictionary in pandas format
def read_file(path):
    return pd.read_csv(path, low_memory = False)

#convert to python-readable time
def convert_time(file):
    file['timestamp'] = pd.to_datetime(file['timestamp'], utc=True, errors='coerce')

#sort telemetry by timestamp (works for multiple cars)
def sort_tel_timestamp(file : pd.DataFrame):
    file_sorted = file.sort_values(by = ['vehicle_id', 'timestamp'])
    return file_sorted

#filters lap csv to 3 columns and by vehicle
def lap_filter(file : pd.DataFrame, car_id : str):
    filtered_file = file[lap_cols]
    by_car = filtered_file[filtered_file['vehicle_id'].isin(target_cars[car_id])].copy()
    by_car['chassis'] = by_car['vehicle_id'].str.split('-').str[-2:].str.join('-')
    return by_car

#merge start and end csvs after filtering
def merge_laps(start : pd.DataFrame, end : pd.DataFrame):
    start = start.rename(columns={'timestamp': 'timestamp_start'})
    end = end.rename(columns={'timestamp': 'timestamp_end'})
    laps = start.merge(
        end[['chassis', 'lap', 'timestamp_end']],
        on=['chassis', 'lap'],
        how='inner'
    )
    return laps

#filter telemetry to tel_cols, filter by values in target_tel and car_id
def tel_filter(tel_data : pd.DataFrame, car_id : str):
    tel_filtered = tel_data[tel_cols]
    tel_filtered = tel_filtered[tel_filtered['telemetry_name'].isin(target_tel)]
    tel_filtered = tel_filtered[tel_filtered['vehicle_id'].isin(target_cars[car_id])].copy()
    tel_filtered['chassis'] = tel_filtered['vehicle_id'].str.split('-').str[-2:].str.join('-')
    return tel_filtered

def merge_laps_tel(lap_data : pd.DataFrame, tel_data : pd.DataFrame):
    tel_sorted = tel_data.sort_values('timestamp')
    lap_sorted = lap_data.sort_values('timestamp_start')

    merged = pd.merge_asof(
        tel_sorted,
        lap_sorted[['chassis','lap','timestamp_start','timestamp_end']],
        left_on='timestamp',
        right_on='timestamp_start',
        by='chassis',
        direction='nearest',
        tolerance=pd.Timedelta("2s")
    )

    merged = merged[merged['timestamp'] <= merged['timestamp_end']]
    return merged

def main():
    #raw datasets
    telemetry = [
    read_file('barber_data/barber/R1_barber_telemetry_data.csv'),
    read_file('barber_data/barber/R2_barber_telemetry_data.csv')
    ]

    #(start time, end time)
    lap_times = [
        (read_file('barber_data/barber/R1_barber_lap_start.csv'),
         read_file('barber_data/barber/R1_barber_lap_end.csv')),

        (read_file('barber_data/barber/R2_barber_lap_start.csv'),
         read_file("barber_data/barber/R2_barber_lap_end.csv"))
    ]

    #convert time for python handling
    for tel in telemetry:
        convert_time(tel)
        sort_tel_timestamp(tel)

    for lap_start, lap_end in lap_times:
        convert_time(lap_start)
        convert_time(lap_end)

    #barber lap processor
    barber_r1_lap_start_filter_000 = lap_filter(lap_times[0][0], "barber000")
    barber_r1_lap_end_filter_000 = lap_filter(lap_times[0][1], "barber000")
    barber_r1_laps_000 = merge_laps(barber_r1_lap_start_filter_000, barber_r1_lap_end_filter_000)

    barber_r2_lap_start_filter_000 = lap_filter(lap_times[1][0], "barber000")
    barber_r2_lap_end_filter_000 = lap_filter(lap_times[1][1], "barber000")
    barber_r2_laps_000 = merge_laps(barber_r2_lap_start_filter_000, barber_r2_lap_end_filter_000)

    barber_r1_lap_start_filter_78 = lap_filter(lap_times[0][0], "barber78")
    barber_r1_lap_end_filter_78 = lap_filter(lap_times[0][1], "barber78")
    barber_r1_laps_78 = merge_laps(barber_r1_lap_start_filter_78, barber_r1_lap_end_filter_78)

    barber_r2_lap_start_filter_78 = lap_filter(lap_times[1][0], "barber78")
    barber_r2_lap_end_filter_78 = lap_filter(lap_times[1][1], "barber78")
    barber_r2_laps_78 = merge_laps(barber_r2_lap_start_filter_78, barber_r2_lap_end_filter_78)

    #barber telemetry processor
    barber_r1_tel_filter_000 = tel_filter(telemetry[0], "barber000")
    barber_r2_tel_filter_000 = tel_filter(telemetry[1], "barber000")

    barber_r1_tel_filter_78 = tel_filter(telemetry[0], "barber78")
    barber_r2_tel_filter_78 = tel_filter(telemetry[1], "barber78")

    #merge laps and telemetry data
    barber_merge_r1_000 = merge_laps_tel(barber_r1_laps_000, barber_r1_tel_filter_000)
    barber_merge_r1_000.to_csv("barber_r1_000.csv", index=False)

if __name__ == "__main__":
    main()

        



        

    






