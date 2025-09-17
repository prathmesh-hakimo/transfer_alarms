import sys
sys.path.append('.')
import argparse
from src.database.config import source, destination
from src.database.connection import connect_to_database
from src.services.door_service import fetch_door_data_by_id, insert_door_data_to_destination, get_door_from_destination
from src.services.employee_service import fetch_employee_data_by_id, insert_employee_data_to_destination, get_employee_from_destination
from src.services.media_service import fetch_alarm_media_by_alarm_id, insert_alarm_media_and_get_id
from src.services.ml_service import fetch_ml_output_by_id, fetch_video_tags_by_ml_output_id, insert_ml_output_to_destination, insert_video_tags_to_destination
from src.services.alarm_type_service import get_alarm_type_id, get_alarm_type, get_respective_alarm_type_id_from_destination
from src.services.alarm_update_service import fetch_alarm_updates, insert_alarm_updates
from src.services.alarm_service import prepare_new_raw_alarm_data, insert_raw_alarm
from src.models.state import DuplicationState
from src.utils.logger import get_logger, add_file_handler
import uuid
from datetime import datetime, timezone

def fetch_raw_alarm_by_id(source_connection, raw_alarm_id):
    cursor = source_connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM raw_alarms_v2 WHERE id = %s", (raw_alarm_id,))
    return cursor.fetchone()

def main(original_raw_alarm_id):
    new_camera_id = '259e78d5-6ed1-4853-8b50-ca5413d0e2b4'
    new_tenant_id = 'demo-sales'

    logger = get_logger("alarm_migration")
    add_file_handler(logger, original_raw_alarm_id)
    state = DuplicationState()
    logs = []
    try:
        logger.info("🚀 Starting Alarm Migration Process")
        source_conn = connect_to_database(source)
        dest_conn = connect_to_database(destination)
        if not source_conn or not dest_conn:
            logger.error("❌ Failed to connect to source or destination DB. Exiting.")
            return "Failed to connect to DB"
        # 1️⃣ Fetch original raw alarm
        original_alarm = fetch_raw_alarm_by_id(source_conn, original_raw_alarm_id)
        if not original_alarm:
            logger.error("❌ Raw alarm not found. Exiting.")
            return "Raw alarm not found"
        logger.info("✅ Original alarm fetched.")
        # Extract unique fields for duplicate check
        source_id = original_alarm.get('source_id')
        partition_key = original_alarm.get('partition_key')
        tenant_id = 'demo-sales'
        from src.services.alarm_service import find_existing_raw_alarm, find_conflicting_location_alarm_info
        existing_alarm = find_existing_raw_alarm(dest_conn, source_id, tenant_id, partition_key)
        if existing_alarm:
            logger.warning(
                f"⚠️ Duplicate alarm detected in destination for source_id={source_id}, tenant_id={tenant_id}, partition_key={partition_key}. Skipping migration."
            )
            return "Duplicate alarm detected, skipped."
        # 2️⃣ Handle Door
        # new_door_id = "245c9fd3-c255-411b-acc9-60d1a5aef723"
        new_door_id = None
        door_id = original_alarm.get('door_id')
        if door_id:
            door_data = fetch_door_data_by_id(source_conn, door_id)
            if door_data:
                existing_door = get_door_from_destination(dest_conn, door_data)
                if existing_door:
                    new_door_id = existing_door['id']
                    logger.info(f"✅ Door already exists in destination with ID: {new_door_id}")
                else:
                    new_door_id = str(uuid.uuid4())
                    insert_door_data_to_destination(dest_conn, door_data, new_door_id)
                    logger.info(f"✅ Inserted new door with ID: {new_door_id}")



        # 3️⃣ Handle Employee
        new_employee_id = None
        # employee_id = original_alarm.get('employee_id')
        # print("Employee ID:", employee_id)
        # if employee_id:
        #     employee_data = fetch_employee_data_by_id(source_conn, employee_id)
        #     print("Employee Data:", employee_data)
        #     if employee_data:
        #         existing_employee = get_employee_from_destination(dest_conn, employee_data)
        #         print("Existing Employee:", existing_employee)
        #         if existing_employee:
        #             new_employee_id = existing_employee['id']
        #             logger.info(f"✅ Employee already exists in destination with ID: {new_employee_id}")
        #         else:
        #             new_employee_id = str(uuid.uuid4())
        #             insert_employee_data_to_destination(dest_conn, employee_data, new_employee_id)
        #             logger.info(f"✅ Inserted new employee with ID: {new_employee_id}")
        # else:
        #     new_employee_id = None

        # 4️⃣ Resolve Alarm Type
        source_alarm_type_id = get_alarm_type_id(source_conn, original_raw_alarm_id)
        source_alarm_type = get_alarm_type(source_conn, source_alarm_type_id)
        destination_alarm_type_id = get_respective_alarm_type_id_from_destination(dest_conn, source_alarm_type)
        if not destination_alarm_type_id:
            logger.warning(f"⚠️ No mapped alarm type for '{source_alarm_type}'. Using predefined alarm type ID 'acf15f45-1c8f-426a-8b78-5dbbfef95c36'.")
            destination_alarm_type_id = "acf15f45-1c8f-426a-8b78-5dbbfef95c36"

        # 5️⃣ Fetch Media
        media_records = fetch_alarm_media_by_alarm_id(source_conn, original_raw_alarm_id)
        media_id = None
        if not media_records:
            logger.warning("⚠️ No media records found. Continuing migration without media.")
        else:
            logger.info("✅ Media record fetched.")

        tap = original_alarm.get('true_alarm_probability')
        print(f"True Alarm Probability: {tap}")

        # 6️⃣ Prepare raw alarm
        new_alarm_data = prepare_new_raw_alarm_data(
            original_alarm_data=original_alarm,
            new_camera_id=new_camera_id,
            new_tenant_id=new_tenant_id,
            alarm_type_id=destination_alarm_type_id,
            new_door_id=new_door_id,
            new_employee_id=new_employee_id,
            new_latest_media_id=None
        )
        new_alarm_id = new_alarm_data['id']
        state.add_alarm(new_alarm_id)

        # 7️⃣ Insert media (only if media records exist)
        if media_records:
            media_id = insert_alarm_media_and_get_id(dest_conn, media_records[0], new_alarm_id)
            if media_id:
                logger.info(f"✅ Media inserted with ID: {media_id}")
                new_alarm_data['latest_alarm_media_id'] = media_id
            else:
                logger.warning("⚠️ Media insert failed, but continuing migration.")

        # 8️⃣ Handle ML Output and Video Tags
        # original_ml_output_id = original_alarm.get('ml_output_id')
        # if original_ml_output_id:
        #     ml_output_data = fetch_ml_output_by_id(source_conn, original_ml_output_id)
        #     if ml_output_data:
        #         video_tags = fetch_video_tags_by_ml_output_id(source_conn, original_ml_output_id)
        #         new_ml_output_id = insert_ml_output_to_destination(dest_conn, ml_output_data, new_alarm_id, new_tenant_id)
        #         if new_ml_output_id:
        #             insert_video_tags_to_destination(dest_conn, video_tags, new_ml_output_id, new_tenant_id)
        #             new_alarm_data['ml_output_id'] = new_ml_output_id
        #             logger.info(f"✅ ML Output and {len(video_tags)} video tags migrated.")

        # 9️⃣ Handle Alarm Updates + Users
        alarm_updates = fetch_alarm_updates(source_conn, original_raw_alarm_id)
        last_update_id = None
        if alarm_updates:
            last_update_id = insert_alarm_updates(dest_conn, source_conn, alarm_updates, new_alarm_id, new_tenant_id)
            if last_update_id:
                new_alarm_data['alarm_update_id'] = last_update_id
                logger.info(f"✅ Alarm updates migrated. Last update ID: {last_update_id}")

        
        if original_alarm.get('source_entity_type') == 'DOOR':
            new_alarm_data['source_entity_id'] = new_door_id


        # 🔟 Add migrated IDs to raw alarm
        new_alarm_data['source_id'] = original_alarm.get('source_id')
        new_alarm_data['door_id'] = new_door_id
        new_alarm_data['employee_id'] = new_employee_id
        new_alarm_data['latest_alarm_media_id'] = media_id  # This will be None if no media
        new_alarm_data['alarm_update_id'] = last_update_id
        new_alarm_data['ml_output_id'] = None
        new_alarm_data['created_at_utc'] = datetime.now(timezone.utc)
        new_alarm_data['updated_at_utc'] = datetime.now(timezone.utc)
        new_alarm_data['alarm_timestamp_utc'] = datetime.now(timezone.utc)
        new_alarm_data['alarm_state'] = 'UNPROCESSED'
        new_alarm_data['current_status'] = "Analyzing"
        new_alarm_data['true_alarm_probability'] = None
        new_alarm_data['partition_key'] = 202509


        # 1️⃣1️⃣ Insert Raw Alarm
        logger.info(f"📥 Inserting raw alarm with ID: {new_alarm_id}")
        success = insert_raw_alarm(dest_conn, new_alarm_data)
        if success:
            logger.info(f"✅ Raw alarm inserted successfully with ID: {new_alarm_id}")
        else:
            logger.error("❌ Failed to insert raw alarm.")

        logger.info("✅ Migration complete. Connections closed.")
        return "Success"
    except Exception as e:
        logger.error(f"❌ Exception: {str(e)}")
        return f"Error: {str(e)}"
    finally:
        try:
            source_conn.close()
            dest_conn.close()
        except:
            pass

import csv
import os
def batch_process_alarms(csv_path):
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return
    
    rows = []
    total_rows = 0
    successful_rows = 0
    
    print(f"📁 Processing alarms from: {csv_path}")
    
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, 1):
            raw_alarm_id = row['raw_alarm_id']
            print(f"Processing {i}: {raw_alarm_id}")
            
            try:
                result = main(raw_alarm_id)
                row['migration_status'] = result or "Success"
                row['processed_at'] = datetime.now().isoformat()
                if result == "Success":
                    successful_rows += 1
            except Exception as e:
                row['migration_status'] = f"Error: {str(e)}"
                row['processed_at'] = datetime.now().isoformat()
                print(f"❌ Error processing {raw_alarm_id}: {str(e)}")
            
            rows.append(row)
            total_rows += 1
    
    # Write results to a new CSV file
    output_file = csv_path.replace('.csv', '_results.csv')
    with open(output_file, 'w', newline='') as csvfile:
        if rows:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    
    print(f"✅ Batch processing complete!")
    print(f"📊 Total: {total_rows}, Successful: {successful_rows}, Failed: {total_rows - successful_rows}")
    print(f"📄 Results saved to: {output_file}")

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Migrate a raw alarm by ID or batch process from CSV')
#     parser.add_argument('--batch', action='store_true', help='Process alarms in batch from alarms.csv')
#     parser.add_argument('--csv', type=str, default='alarms.csv', help='Path to CSV file (default: alarms.csv)')
#     parser.add_argument('original_raw_alarm_id', type=str, nargs='?', help='The ID of the raw alarm to migrate')
    
#     args = parser.parse_args()
    
#     if args.batch:
#         batch_process_alarms(args.csv)
#     elif args.original_raw_alarm_id:
#         main(args.original_raw_alarm_id)
#     else:
#         print("Please provide either --batch flag or an alarm ID")

if __name__ == '__main__':
    main("8859d3ec-0bb3-4a7b-b878-3f355124b35c")

#created_at_utc
#updated_at_utc
#alarm_timestamp_utc
#alarm_state : UNPROCESSED
#current_status : NULL
#true_alarm_probability : NULL
#partition_key : 202509