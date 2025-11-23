import pandas as pd
import numpy as np
import pathlib as Path
import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

#metadata
TRACKS = {
    "barber": {
        "races": ["R1", "R2"],
        "cars": {
            "000": ["GR86-002-000"],
            "78":  ["GR86-004-78"]
        }
    }

}

#cars w telemetry
target_cars = {
    "000" : ['GR86-002-000'],
    "78" : ['GR86-004-78']
}

#target lap params
lap_cols = ['lap', 'vehicle_id', 'meta_time']

#target telemetry params
tel_cols = ['telemetry_name', 'telemetry_value', 'vehicle_id', 'meta_time']
target_tel = ['gear', 'speed', 'nmot']

#returns dictionary in pandas format
def read_file(path):
    return pd.read_csv(path, low_memory = False)

#file loader
def load_lap_files(track, race):
    start = read_file(f"{track}_data/{track}/{race}_{track}_lap_start.csv")
    end   = read_file(f"{track}_data/{track}/{race}_{track}_lap_end.csv")
    return start, end

def load_telemetry(track, race):
    return read_file(f"{track}_data/{track}/{race}_{track}_telemetry_data.csv")

#convert to python-readable time
def convert_time(file):
    file['meta_time'] = pd.to_datetime(file['meta_time'], utc=True, errors='coerce')

#sort telemetry by meta_time (works for multiple cars)
def sort_tel_meta_time(file : pd.DataFrame):
    file_sorted = file.sort_values(by = ['vehicle_id', 'meta_time'])
    return file_sorted

#filters lap csv to 3 columns and by vehicle
def lap_filter(file : pd.DataFrame, car_id : str):
    filtered_file = file[lap_cols]
    by_car = filtered_file[filtered_file['vehicle_id'].isin(target_cars[car_id])].copy()
    by_car['chassis'] = by_car['vehicle_id'].str.split('-').str[-2:].str.join('-')
    return by_car

#merge start and end csvs after filtering
def merge_laps(start : pd.DataFrame, end : pd.DataFrame):
    start = start.rename(columns={'meta_time': 'meta_time_start'})
    end = end.rename(columns={'meta_time': 'meta_time_end'})
    laps = start.merge(
        end[['chassis', 'lap', 'meta_time_end']],
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

#merge lap and telemetry data
def merge_laps_tel(lap_data : pd.DataFrame, tel_data : pd.DataFrame):
    tel_sorted = tel_data.sort_values('meta_time')
    lap_sorted = lap_data.sort_values('meta_time_start')

    merged = pd.merge_asof(
        tel_sorted,
        lap_sorted,
        left_on='meta_time',
        right_on='meta_time_start',
        by='chassis',
        direction='nearest',
        tolerance=pd.Timedelta("2s")
    )

    merged = merged[merged['meta_time'] <= merged['meta_time_end']]
    return merged

#parquet and csv processor pipeline
def process_track(track_name, track_cfg):
    races = track_cfg["races"]
    cars = track_cfg["cars"]

    for race in races:
        print(f"Processing {track_name} {race}...")

        #load data
        tel = load_telemetry(track_name, race)
        convert_time(tel)

        lap_start, lap_end = load_lap_files(track_name, race)
        convert_time(lap_start)
        convert_time(lap_end)

        for car_id, chassis_list in cars.items():
            print(f" → Car {car_id}")

            #filter laps and telemetry
            laps_f = lap_filter(lap_start, car_id)
            lape_f = lap_filter(lap_end, car_id)
            laps = merge_laps(laps_f, lape_f)

            tel_f = tel_filter(tel, car_id)

            #merge lap + telemetry
            merged = merge_laps_tel(laps, tel_f)

            #output filenames
            out_parquet = f"data/{track_name}_{race}_{car_id}.parquet"
            out_csv     = f"data/{track_name}_{race}_{car_id}.csv"

            #save
            merged.to_parquet(out_parquet, index=False)
            merged.to_csv(out_csv, index=False)

            print(f"   Saved: {out_parquet}")


def main():
   for track_name, track_cfg in TRACKS.items():
        process_track(track_name, track_cfg)

if __name__ == "__main__":
    main()

        



        

    






