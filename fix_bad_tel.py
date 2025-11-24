import pandas as pd
import json


#the fact i needed this for ONE file is insane work toyota
#i spent 30 min renaming and reorganizing files alone by hand because there was no standard to the file structure
#whoever was involved in data collection needs help
INPUT_FILE = "raw/sebring/R2/sebring_telemetry_R2_bad.csv"
OUTPUT_FILE = "raw/sebring/R2/sebring_telemetry_R2.csv"

#expands the fucked up telemetry column that for some reason has json in it, unlike the other files
def expand_telemetry_row(row):

    try:
        value_list = json.loads(row["value"])
    except:
        return []

    expanded_rows = []
    for item in value_list:
        expanded_rows.append({
            "expire_at": row.get("expire_at", ""),
            "lap": row.get("lap", ""),
            "meta_event": row["meta_event"],
            "meta_session": row["meta_session"],
            "meta_source": row["meta_source"],
            "meta_time": row["meta_time"],
            "original_vehicle_id": row["vehicle_id"],
            "outing": row["outing"],
            "telemetry_name": item["name"],
            "telemetry_value": item["value"],
            "timestamp": row["timestamp"],
            "vehicle_id": row["vehicle_id"],
            "vehicle_number": row["vehicle_id"].split("-")[-1]
        })
    return expanded_rows


def main():
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    if "expire_at" not in df.columns:
        df["expire_at"] = ""

    expanded = []

    for _, row in df.iterrows():
        expanded.extend(expand_telemetry_row(row))

    fixed_df = pd.DataFrame(expanded)

    #ensure correct column order
    correct_order = [
        "expire_at",
        "lap",
        "meta_event",
        "meta_session",
        "meta_source",
        "meta_time",
        "original_vehicle_id",
        "outing",
        "telemetry_name",
        "telemetry_value",
        "timestamp",
        "vehicle_id",
        "vehicle_number"
    ]

    fixed_df = fixed_df[correct_order]

    fixed_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved repaired telemetry CSV to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
