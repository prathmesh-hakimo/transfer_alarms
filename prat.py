import os
import csv
import argparse
from datetime import datetime

def extract_log_info_from_filename(filename):
    try:
        base_name = filename.replace('.log', '')
        parts = base_name.split('_')
        if len(parts) == 3:
            raw_alarm_id, date_part, time_part = parts
            timestamp_str = f"{date_part} {time_part}"
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d %H%M%S")
            return raw_alarm_id, timestamp
        elif len(parts) == 1:
            return parts[0], None  # Only raw_alarm_id
        else:
            raise ValueError("Unexpected filename format.")
    except Exception as e:
        print(f"❌ Skipping invalid filename '{filename}': {e}")
        return None, None

def process_log_folder(folder_path, output_csv='parsed_logs.csv'):
    rows = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.log'):
            raw_alarm_id, timestamp = extract_log_info_from_filename(filename)
            if raw_alarm_id:
                rows.append({
                    'raw_alarm_id': raw_alarm_id,
                    'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else ''
                })

    # Sort by timestamp (empty timestamps go last)
    rows.sort(key=lambda x: x['timestamp'] or '9999-12-31 23:59:59')

    # Write to CSV
    output_path = os.path.join(folder_path, output_csv)
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['raw_alarm_id', 'timestamp'])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Parsed {len(rows)} files. Output written to: {output_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract alarm log info to CSV')
    parser.add_argument('folder_path', help='Path to folder containing .log files')
    args = parser.parse_args()
    process_log_folder(args.folder_path)
